import sys
import logging
import argparse
import typing
import html
from decimal import Decimal
from ZODB.Connection import Connection
from datetime import datetime, timezone, timedelta
from pyramid.paster import bootstrap, setup_logging
from pyramid.request import Request
from pyramid_zodbconn import get_connection

# noinspection PyUnresolvedReferences
from BTrees.OOBTree import OOBTree
# noinspection PyUnresolvedReferences
from BTrees.IOBTree import IOBTree

# local imports
from .params import Params
from .settings import settings
from .idx_utils import idx_update
from .failure import Failure
from .telegram import send_to_telegram_multiple
from .wildberries import fetch_product_details, fetch_categories, fetch_subcategories, fetch_products
from .wildberries import UnexpectedResponse
from .models import AppRoot, get_app_root
from .models.tcm import in_transaction
from .models.wb import LastUpdateResult, Product, Category, Subcategory, PriceSlot

log = logging.getLogger(__name__)


def dt_fmt(dt: datetime) -> str:
    """Format date/time with minutes precision in local timezone"""
    return f'{dt.astimezone():%Y-%m-%d %H:%M}'


def fetch_product_updates(app_root: AppRoot, articles: list[str], failures: list[Failure]) -> list[Product]:
    """
    Try to fetch product details for given product articles.
    New products can be distinguished by the old_values == None.
    Updated Product entities have old_values volatile property with fields with previous values if any.
    @param app_root: App Root persistent object
    @param articles: list of articles
    @param failures: output param, filled with failed fetches if any
    @return: a list of all new and updated Product entities
    """
    products: list[Product] = []
    for article in articles:
        try:
            fetch_started_at, product_details = fetch_product_details(article)

            if article in app_root.article_to_product:
                # get entity from database
                product = app_root.article_to_product[article]
                product.update(fetch_started_at, **product_details)
            else:
                # create new entity
                product = Product(article=article, **product_details, fetched_at=fetch_started_at)
                app_root.article_to_product[article] = product

            products.append(product)

        except Exception as e:
            failures.append(Failure(
                Product.fmt_article_descriptor(article), e
            ))

    return products


def update_product_categories(app_root: AppRoot) -> None:
    """
    Fetch all product categories from the Wildberries website.
    Updates database: creates new categories, updates existing, does not delete disappearing ones.
    Updates lw_name_to_category and lw_seo_to_category mappings.
    Raises an exception if there is a fetch or parse error.
    @param app_root: App Root persistent object
    """
    fetch_started_at, product_categories_list = fetch_categories()

    new_cats_num, updated_cats_num, unchanged_cats_num, lw_name_to_cat, lw_seo_to_cat = 0, 0, 0, {}, {}
    for cat_props in product_categories_list:
        # rename fields to conform persistent entity
        cat_id = cat_props['id']; del cat_props['id']
        cat_props['parent_id'] = cat_props['parent']; del cat_props['parent']

        if cat_id in app_root.id_to_category:
            # get entity from database
            category = app_root.id_to_category[cat_id]
            if category.fetched_at == fetch_started_at:
                # we have already seen this ID in the response
                raise UnexpectedResponse(f'several categories with the same ID: {category.name}, {cat_props["name"]}')

            if category.update(fetch_started_at, **cat_props):
                updated_cats_num += 1
            else:
                unchanged_cats_num += 1

        else:
            # create new entity
            category = Category(id_=cat_id, **cat_props, fetched_at=fetch_started_at)
            app_root.id_to_category[cat_id] = category
            new_cats_num += 1

        # update indexes
        idx_update(lw_name_to_cat, key=category.name.lower(), element=category)
        if category.seo:
            idx_update(lw_seo_to_cat, key=category.seo.lower(), element=category)

    # update app_root.lw_name_to_category persistent mapping if required, do not delete old names
    for lw_name, cat in lw_name_to_cat.items():
        if lw_name not in app_root.lw_name_to_category or app_root.lw_name_to_category[lw_name] != cat:
            app_root.lw_name_to_category[lw_name] = cat

    # update app_root.lw_seo_to_category persistent mapping if required, do not delete old seos
    for lw_seo, cat in lw_seo_to_cat.items():
        if lw_seo not in app_root.lw_seo_to_category or app_root.lw_seo_to_category[lw_seo] != cat:
            app_root.lw_seo_to_category[lw_seo] = cat

    app_root.categories_last_update = LastUpdateResult(
        fetched_at=fetch_started_at,
        num_new=new_cats_num,
        num_updated=updated_cats_num,
        num_gone=len(app_root.id_to_category) - new_cats_num - updated_cats_num - unchanged_cats_num
    )


def update_subcategories(app_root: AppRoot, category: Category) -> None:
    """
    Fetch all subcategories for the given product category.
    Updates category entity: creates new subcategories, updates existing, but does not delete disappearing ones.
    Updates category.lw_name_to_subcategory mapping.
    Updates app_root.lw_name_to_subcategory and app_root.id_to_subcategory mappings.
    Raises an exception if there is a fetch or parse error.
    @param app_root: App Root persistent object
    @param category: Product category entity to update its subcategories
    """
    if not category.shard or not category.query:
        raise ValueError(f'category {category} must have "shard" and "query" properties to fetch subcategories')

    fetch_started_at, subcategories_list = fetch_subcategories(category.shard, cat_filter=category.query)

    new_scats_num, updated_scats_num, unchanged_scats_num, lw_name_to_scat = 0, 0, 0, {}
    for scat_props in subcategories_list:
        scat_id = scat_props['id']; del scat_props['id']  # delete "id" field to conform persistent entity

        if scat_id in category.id_to_subcategory:
            # get existing entity from database
            scat = category.id_to_subcategory[scat_id]
            if scat.fetched_at == fetch_started_at:
                # we have already seen this ID in the response
                raise UnexpectedResponse(f'several sub-cats with ID {scat_id}: {scat.name}, {scat_props["name"]}')

            if scat.update(fetch_started_at, **scat_props):
                updated_scats_num += 1
            else:
                unchanged_scats_num += 1

        else:
            # create new entity
            scat = Subcategory(id_=scat_id, **scat_props, fetched_at=fetch_started_at, category=category)
            category.id_to_subcategory[scat_id] = scat
            new_scats_num += 1

        # get existing subcategory with this lowered name if any
        lw_name = scat_props['name'].lower()
        ex_scat = category.lw_name_to_subcategory[lw_name] if lw_name in category.lw_name_to_subcategory else None

        # verify we got no duplicates
        if ex_scat and ex_scat.fetched_at == fetch_started_at and ex_scat != scat:
            # we have already seen this name in the response
            raise UnexpectedResponse(f'several sub cats with name {scat.name}: {scat_id}, {ex_scat.id}')

        # if no existing subcategory, or it differs, update lw_name_to_subcategory dict
        if ex_scat != scat:
            category.lw_name_to_subcategory[lw_name] = scat

        # verify consistency
        if scat.category != category:
            raise RuntimeError(f'subcategory.category != category: {scat.category} != {category}')

        # update indexes
        idx_update(app_root.lw_name_to_subcategory, key=lw_name, element=scat)
        idx_update(app_root.id_to_subcategory, key=scat_id, element=scat)

    # of for scat_props in subcategories_list

    # save results of the update
    category.subcategories_last_update = LastUpdateResult(
        fetched_at=fetch_started_at,
        num_new=new_scats_num,
        num_updated=updated_scats_num,
        num_gone=len(category.id_to_subcategory) - new_scats_num - updated_scats_num - unchanged_scats_num
    )


def update_all_categories_and_subcategories(app_root: AppRoot, conn: Connection, failures: list[Failure]) -> None:
    """
    Fetch from Wildberries and update all categories and subcategories.
    @param app_root: App Root persistent object
    @param conn: database connection
    @param failures: output param, filled with failures if any
    """
    log.info('fetch, parse and save to database all product categories')
    with in_transaction(conn):
        try:
            update_product_categories(app_root)
        except Exception as e:
            log.warning(f'failed to update categories: {e}')
            failures.append(Failure('update categories', e))

    lur = app_root.categories_last_update
    log.info(f'=== categories added: {lur.num_new}, updated: {lur.num_updated}, disappeared: {lur.num_gone}')

    log.info('update subcategories for all product categories')
    for cat in app_root.id_to_category.values():
        if not cat.query or not cat.shard:
            continue

        log.info(f'trying to update subcategories for {cat}')
        try:
            with in_transaction(conn):
                update_subcategories(app_root, cat)
            lur = cat.subcategories_last_update
            log.info(f'=== scats added: {lur.num_new}, updated: {lur.num_updated}, disappeared: {lur.num_gone}')

        except Exception as e:
            log.warning(f'failed to update subcategories in {cat.entity_descriptor}: {e}')
            failures.append(Failure(f'update subcategories in {cat.entity_descriptor}', e))


def dump_all_categories_and_subcategories(app_root: AppRoot) -> None:
    log.info('print all categories and subcategories from the database')
    print('Под;Кат;Подкатегория;Категория;Полное название категории;Род;Фильтр;URL;Обновлено')
    for c in app_root.id_to_category.values():
        url = c.url if c.url.startswith('http') else f'https://www.wildberries.ru{c.url}'
        c_fetched_at_str = dt_fmt(c.fetched_at)
        par_str = c.parent_id or ''
        seo_str = c.seo or ''
        qry_str = c.query or ''
        # noinspection PyProtectedMember
        if not c._id_to_subcategory:
            print(f';{c.id};;{c.name};{seo_str};{par_str};{qry_str};{url};{c_fetched_at_str}')
        else:
            for sc in c.id_to_subcategory.values():
                sc_fetched_at_str = dt_fmt(sc.fetched_at)
                print(f'{sc.id};{c.id};{sc.name};{c.name};{seo_str};{par_str};{qry_str};{url};{sc_fetched_at_str}')


def get_matched_items(items: typing.Iterable[tuple[str, object | set]], search: str) -> set:
    """
    Get all items with a key starting with a given string and the remaining suffix within the configured threshold
    @param items: iterable to search in
    @param search: searching string
    @return: set of matching objects, can be empty
    """
    matched = set()
    for key, item in items:
        if key.startswith(search) and len(key) <= len(search) + settings.search_max_suffix:
            if isinstance(item, set):
                matched.update(item)
            else:
                matched.add(item)

    return matched


def find_categories(app_root: AppRoot, search: str | int) -> set[Category]:
    """Find categories by ID, name or seo"""
    id_to_cat = app_root.id_to_category;              id_to_cat: IOBTree
    lw_name_to_cat = app_root.lw_name_to_category;    lw_name_to_cat: OOBTree
    lw_seo_to_cat = app_root.lw_seo_to_category;      lw_seo_to_cat: OOBTree

    if isinstance(search, int) or search.isdigit():
        # search by category ID
        cat_id = int(search)
        return {id_to_cat[cat_id]} if cat_id in id_to_cat else set()

    else:
        # search by category name or seo, case-insensitive
        search = search.lower()
        chars_stripped = 0
        while len(search) >= settings.search_min_chars and chars_stripped <= settings.search_max_suffix:
            key_max = search + chr(sys.maxunicode)
            matched = get_matched_items(lw_name_to_cat.items(min=search, max=key_max), search)
            matched.update(
                get_matched_items(lw_seo_to_cat.items(min=search, max=key_max), search)
            )
            if matched:
                return matched

            search = search[:-1]
            chars_stripped += 1

        return set()


def find_subcategories(app_root: AppRoot, s_cat: str | int, s_scat: str | int) -> set[Subcategory]:
    """
    Find subcategories by category and subcategory names or IDs.
    If non-empty s_cat given, searches for categories first, next — for subcategories in found categories.
    Else, search for subcategories in global app_root.*_to_subcategory indexes.
    @param app_root: App Root persistent object
    @param s_cat: string or ID to search categories
    @param s_scat: string or ID to search subcategories
    @return: set of subcategories, can be empty
    """

    def _find_subcategories_in_container(container: Category | AppRoot, search: str | int) -> set[Subcategory]:
        """
        Find subcategories in given category or app_root by ID or name
        @param container: Category or AppRoot with mappings
        @param search: ID or string to search in name
        @return: set of subcategories, can be empty
        """
        id_to_scat = container.id_to_subcategory;             id_to_scat: IOBTree
        lw_name_to_scat = container.lw_name_to_subcategory;   lw_name_to_scat: OOBTree

        if isinstance(search, int) or search.isdigit():
            # search by subcategory ID
            scat_id = int(search)
            scats_found = id_to_scat[scat_id] if scat_id in id_to_scat else set()
            return scats_found if isinstance(scats_found, set) else {scats_found}

        else:
            # search by sub category name, case-insensitive
            search = search.lower()
            chars_stripped = 0
            while len(search) >= settings.search_min_chars and chars_stripped <= settings.search_max_suffix:
                key_max = search + chr(sys.maxunicode)
                matched = get_matched_items(lw_name_to_scat.items(min=search, max=key_max), search)
                if matched:
                    return matched

                search = search[:-1]
                chars_stripped += 1

            return set()

    if s_cat:
        # if we have concrete categories, search in them
        cats = find_categories(app_root, s_cat)
        scats = set()
        for cat in cats:
            scats.update(_find_subcategories_in_container(cat, s_scat))
        return scats

    # else search by subcategory name or ID in global index
    return _find_subcategories_in_container(app_root, s_scat)


def send_spp_changes(
        app_root: AppRoot, contacts: list[str], entities: list[Product | PriceSlot], failures: list[Failure]) -> bool:
    """
    Send report about SPP changes for all given products or price slots.
    Conforms and updates ``app_root.entity_descr_to_report_sent_at`` index.
    @param app_root: App Root persistent object
    @param entities: list of products or price slots with SPP change
    @param contacts: contacts for sending a report
    @param failures: output param, filled with send failures
    @return: True — report was sent to at least one contact
    """
    # filter out products for whose report was recently sent
    idx = app_root.entity_descr_to_report_sent_at
    dt_past = datetime.now(timezone.utc) - timedelta(minutes=settings.report_changes_delay_interval)
    entities = [x for x in entities if x.entity_descriptor not in idx or idx[x.entity_descriptor] < dt_past]
    if not entities:
        # all SPP changes for given entities has been recently reported, do not send a report
        return False

    report_lines = ['<b>Изменения СПП</b>', '↓']
    for e in entities:
        if isinstance(e, Product):
            report_lines.append(
                f'{dt_fmt(e.fetched_at)}: {html.escape(e.name)}, арт. {html.escape(e.article)}, цена {e.price}, '
                f'со скидкой {e.price_sale}, СПП <b>{e.discount_client}</b>'
            )
            descr_was = f'{dt_fmt(e.old_values["fetched_at"])} было: СПП <b>{e.old_values["discount_client"]}</b>'
            if 'name' in e.old_values:
                descr_was += f', название "{html.escape(e.old_values["name"])}"'
            if 'price' in e.old_values:
                descr_was += f', цена {e.old_values["price"]}'
            if 'price_sale' in e.old_values:
                descr_was += f', цена со скидкой {e.old_values["price_sale"]}'
            report_lines.append(descr_was)
            report_lines.append('')
        else:  # PriceSlot
            report_lines.append(
                f'{dt_fmt(e.fetched_at)}: {html.escape(e.entity_descriptor)}: СПП <b>{e.discount_client}</b>\n'
                f'{dt_fmt(e.old_values["fetched_at"])}: было СПП <b>{e.old_values["discount_client"]}</b>'
            )
            report_lines.append('')

    report_text = '\n'.join(report_lines)
    log.info(f'send changes report to users')
    send_errors = send_to_telegram_multiple(settings.telegram_bot_token, contacts, report_text)

    for contact, exception in send_errors.items():
        failures.append(Failure(str(contact), f'failed to send report: {exception}'))

    if any(x not in send_errors for x in contacts):
        # report was sent to at least one recipient, save the current date/time in the index
        for e in entities:
            idx[e.entity_descriptor] = datetime.now(timezone.utc)
        return True

    # report was failed to send
    return False


def monitor_articles(app_root: AppRoot, params: Params, conn: Connection, failures: list[Failure]) -> None:
    """
    Fetch product updates and send report to all users in case of SPP change. Does all in a new transaction.
    If any SPP changed, and it was not possible to send a report to at least one contact, rollbacks transaction.
    @param app_root: App Root persistent object
    @param params: input params object
    @param conn: database connection
    @param failures: output param, filled with failures if any
    """
    class SilentlyRollbackTransaction(Exception):
        pass

    try:
        with in_transaction(conn):
            products = fetch_product_updates(app_root, params.monitor_articles, failures)
            new_products_num = len([x for x in products if x.old_values is None])
            log.info(f'products fetched new: {new_products_num}, updated: {len(products) - new_products_num}')

            # filter products with changed SPP
            products = [x for x in products if x.old_values is not None and 'discount_client' in x.old_values]
            if products:
                # there are products with changed SPP, send report to users
                if not send_spp_changes(app_root, params.contacts_users, products, failures):
                    # failed to send a report to at least one user, raise an exception to rollback database changes
                    raise SilentlyRollbackTransaction()

    except SilentlyRollbackTransaction:
        pass


def send_admin_report(app_root: AppRoot, params: Params, failures: list[Failure]) -> None:
    """
    Send error report to all configured admin contacts.
    Conforms and updates ``app_root.entity_descr_to_report_sent_at`` index.
    If it was not possible to send a report to at least one admin contact, raises an exception.
    In case of some admin contacts failed, sends additional error report about that to other admin contacts.
    @param app_root: App Root persistent object
    @param params: input params object
    @param failures: failures to report about
    """
    def _send_failures_to_admins(header: str, failures_: list[Failure]) -> dict[str, Exception]:
        # filter out failures for whose report was recently sent
        idx = app_root.entity_descr_to_report_sent_at
        dt_past = datetime.now(timezone.utc) - timedelta(minutes=settings.report_errors_delay_interval)
        failures_ = [x for x in failures_ if x.entity_descr not in idx or idx[x.entity_descr] < dt_past]
        if not failures_:
            return {}

        def _trunc(text: str) -> str:
            return text if len(text) < 256 else f'{text[:250]} ...'

        report_text = '\n'.join(
            [f'<b>{header}</b>', '↓'] +
            [f'• {html.escape(x.entity_descr)}: {html.escape(_trunc(x.message))}' for x in failures_] +
            ['']
        )

        log.info(f'send errors report to admins')
        send_failures_ = send_to_telegram_multiple(settings.telegram_bot_token, params.contacts_admins, report_text)
        if all(x in send_failures_ for x in params.contacts_admins):
            raise RuntimeError(f'failed to send error report to any of the configured admin contacts')

        # report was sent to at least one recipient, save the current date/time in the index
        for x in failures_:
            idx[x.entity_descr] = datetime.now(timezone.utc)

        return send_failures_

    send_failures = _send_failures_to_admins('Failures', failures)
    if send_failures:
        _send_failures_to_admins(
            'Failed to send error report to the following contacts',
            [Failure(c, e) for c, e in send_failures.items()]
        )


def get_or_create_all_slots(app_root: AppRoot, params: Params, failures: list[Failure]) -> list[list[PriceSlot]]:
    """
    Search for all subcategories given in input parameters.
    If any of searches gives empty result, tries to update categories
    and relevant subcategories from Wildberries and retries search.
    @param app_root: App Root persistent object
    @param params: input parameters
    @param failures: output param, filled with failures if any
    @return: list of persistent price slots grouped by matched subcategories and ordered by price_from
    """

    # first, determine if there are unknown categories or subcategories required to fetch from Wildberries
    cat_names_or_ids_unknown = set()
    for scats_params in params.monitor_subcategories:
        if scats_params.category_search:
            if not find_subcategories(app_root, scats_params.category_search, scats_params.subcategory_search):
                cat_names_or_ids_unknown.add(scats_params.category_search)

    if cat_names_or_ids_unknown:
        # there are unknown names or ids: try to fetch all categories and relevant subcategories from Wildberries
        log.info('try to fetch product categories from Wildberries')
        try:
            update_product_categories(app_root)
        except Exception as e:
            log.warning(f'failed to update categories: {e}')
            failures.append(Failure('update categories', e))

        # find all categories matched to any name or id in the list of unknown ones
        cats_relevant = set()
        for cat_name_or_id in cat_names_or_ids_unknown:
            cats = find_categories(app_root, cat_name_or_id)
            cats_relevant.update(cats)

        for cat in cats_relevant:
            log.info(f'fetching subcategories in category "{cat.entity_descriptor}" from Wildberries')
            try:
                update_subcategories(app_root, cat)
            except Exception as e:
                log.warning(f'failed to update subcategories in {cat.entity_descriptor}: {e}')
                failures.append(Failure(f'update subcategories in {cat.entity_descriptor}', e))

    # search subcategories and get/create price slots
    slots: list[list[PriceSlot]] = []
    for scats_params in params.monitor_subcategories:
        scats_matched = find_subcategories(app_root, scats_params.category_search, scats_params.subcategory_search)
        if not scats_matched:
            failures.append(Failure(scats_params.scat_search_descriptor, 'no subcategories found'))
        elif len(scats_matched) > settings.max_matched_subcategories:
            failures.append(Failure(
                scats_params.scat_search_descriptor,
                f'found {len(scats_matched)} subcategories, '
                f'it is more then configured maximum of {settings.max_matched_subcategories}'
            ))
        else:
            slots_in_scat: list[PriceSlot] = []
            for scat in scats_matched:
                # get/create price slots for each subcategory found
                slots_in_scat += scat.get_or_create_slots(
                    scats_params.price_min, scats_params.price_max, scats_params.price_step
                )
            slots.append(slots_in_scat)

    return slots


def add_article_to_slot(slots: list[PriceSlot], article: str, price: Decimal) -> PriceSlot | None:
    """
    Adds an article with a given price to the corresponding slot.
    @param slots: list of price slots ordered by price_from
    @param article: product article
    @param price: product price
    @return: PriceSlot article is added to, None if suitable slot is not found or article is already in the slot
    """
    for slot in slots:
        if slot.price_from <= price < slot.price_to:
            # ↑ suitable slot is found
            if article not in slot.articles:
                slot.articles.add(article)
                # article is added to the suitable slot
                return slot
            else:
                # article is already in the slot
                return None

    # suitable slot is not found
    return None


def fill_slots_with_articles(slots: list[list[PriceSlot]]) -> None:
    """
    Fetches product articles for all given slots
    @param slots: price slots grouped by subcategory and ordered by price_from
    """
    for slots_in_scat in slots:
        scat = slots_in_scat[0].subcategory
        shard, cat_id, xsubject = scat.category.shard, scat.category.id, scat.id  # filters for the given subcategory
        num_pages = settings.products_num_pages_to_fetch

        # minimal price suitable for filtering
        price_min = slots_in_scat[0].price_from * (1 - settings.maximum_client_discount_base / 100)

        price_to = slots_in_scat[-1].price_to  # starting from maximal price suitable for filtering
        ratio = 1 - (slots_in_scat[0].price_to - slots_in_scat[0].price_from) / price_to  # decreasing ratio
        while price_to > price_min:
            price_from = price_to * ratio
            # try to fetch products in this price range
            try:
                _, products_props = fetch_products(
                    shard=shard, cat_id=cat_id, xsubject=xsubject,
                    num_pages=num_pages, price_from=price_from, price_to=price_to
                )
                log.info(f'fetched {len(products_props)} in the price range {price_from:.2f} .. {price_to:.2f}')
                articles_added = 0
                for props in products_props:
                    if add_article_to_slot(slots_in_scat, props['id'], props['price_sale']):
                        articles_added += 1
                log.info(f'added to slots: {articles_added}')
            except Exception as e:
                log.warning(f'fetch failure: {e}')

            price_to = price_from


def determine_spp_in_slots(slots: list[list[PriceSlot]], failures: list[Failure]) -> list[PriceSlot]:
    """
    Fetches product details for required number of articles and saves SPP
    @param slots: price slots grouped by subcategory filled with articles
    @param failures: output param, filled with failures if any
    @return: list of price slots with SPP changed
    """
    slots_with_changed_spp: list[PriceSlot] = []

    def validate_enough_products(num: int) -> bool:
        if num >= settings.products_num_to_determine_spp:
            return True
        failures.append(Failure(slot.entity_descriptor, f'not enough products: {num}'))
        return False

    def calc_and_save_spp(slot_: PriceSlot, spp_to_num: dict) -> bool:
        max_num = max(x for x in spp_to_num.values())
        percent = Decimal(max_num) / settings.products_num_to_determine_spp * 100
        if percent < settings.products_num_percent_min_determine_spp:
            failures.append(Failure(slot.entity_descriptor, f'not large enough percent: {percent:.2f}'))
            return False
        else:
            spp_ = [x for x, v in spp_to_num.items() if v == max_num][0]
            return slot_.update(datetime.now(tz=timezone.utc), discount_client=spp_)

    for slots_in_scat in slots:
        for slot in slots_in_scat:
            if not validate_enough_products(len(slot.articles)):
                continue
            log.info(f'fetching product details for articles in slot {slot.entity_descriptor}')
            spp_to_num_products = {}
            total_spp_in_slot = 0
            for article in slot.articles:
                try:
                    _, product_props = fetch_product_details(article)
                    spp = product_props['discount_client']
                    total_spp_in_slot += 1
                    if spp in spp_to_num_products:
                        spp_to_num_products[spp] += 1
                    else:
                        spp_to_num_products[spp] = 1
                    if total_spp_in_slot >= settings.products_num_to_determine_spp:
                        break
                except Exception as e:
                    log.warning(f'fetch error: {e}')
            if not validate_enough_products(total_spp_in_slot):
                continue
            if calc_and_save_spp(slot, spp_to_num_products) and slot.old_values['discount_client'] is not None:
                slots_with_changed_spp.append(slot)

    return slots_with_changed_spp


def monitor_slots(app_root: AppRoot, params: Params, conn: Connection, failures: list[Failure]) -> None:
    """
    Determine SPP updates and send report to all users in case of SPP change. Does all in a new transaction.
    If any SPP changed, and it was not possible to send a report to at least one contact, rollbacks transaction.
    @param app_root: App Root persistent object
    @param params: input params object
    @param conn: database connection
    @param failures: output param, filled with failures if any
    """
    class SilentlyRollbackTransaction(Exception):
        pass

    try:
        with in_transaction(conn):
            log.info('try to find all subcategories matched to configured')
            slots = get_or_create_all_slots(app_root, params, failures)
            log.info(f'found {len(slots)} subcategories')

            log.info('fetch articles and fill price slots')
            fill_slots_with_articles(slots)

            log.info('determine SPP in all prepared price slots')
            slots_with_changed_spp = determine_spp_in_slots(slots, failures)

            if slots_with_changed_spp:
                # there are slots with changed SPP, send report to users
                if not send_spp_changes(app_root, params.contacts_users, slots_with_changed_spp, failures):
                    # failed to send a report to at least one user, raise an exception to rollback database changes
                    raise SilentlyRollbackTransaction()

    except SilentlyRollbackTransaction:
        pass


def main():
    parser = argparse.ArgumentParser(description='Wildberries SPP Monitor.')
    parser.add_argument('config_uri', help='The URI to the main configuration file.')
    args = parser.parse_args()

    # setup logging from config file settings
    setup_logging(args.config_uri)

    # bootstrap Pyramid environment to get configuration
    with bootstrap(args.config_uri) as env:
        # registry: pyramid.registry.Registry = env['registry']
        request: Request = env['request']

        log.info('load and validate input params')
        params = Params()

        log.info('get database connection and App Root object')
        conn: Connection = get_connection(request)
        app_root = get_app_root(conn)

        # the cumulative list of all failures to report to admins
        failures: list[Failure] = []

        log.info('try to fetch product updates for all configured articles')
        monitor_articles(app_root, params, conn, failures)

        if len(app_root.id_to_category) == 0:
            # database is empty, fetch all categories and subcategories
            update_all_categories_and_subcategories(app_root, conn, failures)
            if failures:
                log.warning("=== ↓ === a list of all failures in updating all categories === ↓ ===")
                for failure in failures:
                    log.warning(f'{failure.entity_descr} → {failure.message}')
                log.warning("=== ↑ === a list of all failures in updating all categories === ↑ ===")
                failures.clear()

        log.info('try to determine SPPs for all configured subcategories and price ranges')
        monitor_slots(app_root, params, conn, failures)

        if failures:
            with in_transaction(conn):
                send_admin_report(app_root, params, failures)


if __name__ == '__main__':
    main()
