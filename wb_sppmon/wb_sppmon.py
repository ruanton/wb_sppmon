import sys
import logging
import argparse
import typing
import html
import ZODB.Connection
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
from .telegram import send_to_telegram_multiple
from .wildberries import UnexpectedResponse, fetch_product_details, fetch_categories, fetch_subcategories
from .models import AppRoot, get_app_root
from .models.tcm import in_transaction
from .models.wb import LastUpdateResult, Product, Category, Subcategory

log = logging.getLogger(__name__)


def dt_fmt(dt: datetime) -> str:
    """Format date/time in local timezone, minutes precision"""
    return f'{dt.astimezone():%Y-%m-%d %H:%M}'


def fetch_product_updates(app_root: AppRoot, articles: list[str]) -> tuple[list[Product], int, dict[str, Exception]]:
    """
    Try to fetch product details for given product articles. Returns updated products only.
    Every returned Product entity have old_values volatile property with previous fields.
    @param app_root: App Root persistent object
    @param articles: list of articles
    @return: (
      • list of updated Product entities,
      • number of new products,
      • mapping: article => exception, for all articles failed to fetch
    )
    """
    updated_products: list[Product] = []
    article_to_exception: dict[str, Exception] = {}

    new_products_num = 0
    for article in articles:
        try:
            fetch_started_at, product_details = fetch_product_details(article)

            if article in app_root.article_to_product:
                # get entity from database
                product = app_root.article_to_product[article]
                if product.update(fetch_started_at, **product_details):
                    updated_products.append(product)
            else:
                # create new entity
                product = Product(article=article, **product_details, fetched_at=fetch_started_at)
                app_root.article_to_product[article] = product
                new_products_num += 1

        except Exception as e:
            article_to_exception[article] = e

    return updated_products, new_products_num, article_to_exception


def update_product_categories(app_root: AppRoot) -> None:
    """
    Fetch all product categories from the Wildberries website.
    Updates database: creates new categories, updates existing, does not delete disappearing ones.
    Updates lw_name_to_category and lw_seo_to_category mappings.
    Raises exception on fetch or parse error.
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


def update_subcategories(app_root: AppRoot, category: Category):
    """
    Fetch all subcategories for the given product category.
    Updates category entity: creates new subcategories, updates existing, but does not delete disappearing ones.
    Updates category.lw_name_to_subcategory mapping.
    Updates app_root.lw_name_to_subcategory and app_root.id_to_subcategory mappings.
    Raises exception on fetch or parse error.
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


def update_all_categories_and_subcategories(app_root: AppRoot, conn: ZODB.Connection.Connection):
    log.info('fetch, parse and save to database all product categories')
    with in_transaction(conn):
        update_product_categories(app_root)

    lur = app_root.categories_last_update
    log.info(f'=== categories added: {lur.num_new}, updated: {lur.num_updated}, disappeared: {lur.num_gone}')

    log.info('update subcategories for all product categories')
    for c in app_root.id_to_category.values():
        if not c.query or not c.shard:
            continue
        # if cat.subcategories_last_update:
        #     continue
        log.info(f'trying to update subcategories for {c}')
        try:
            with in_transaction(conn):
                update_subcategories(app_root, c)
            lur = c.subcategories_last_update
            log.info(f'=== scats added: {lur.num_new}, updated: {lur.num_updated}, disappeared: {lur.num_gone}')
        except Exception as e:
            log.warning(f'error: {e}')


def dump_all_categories_and_subcategories(app_root: AppRoot):
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
            matched.update(get_matched_items(lw_seo_to_cat.items(min=search, max=key_max), search))
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
        cats = find_categories(app_root, s_cat)
        scats = set()
        for cat in cats:
            scats.update(_find_subcategories_in_container(cat, s_scat))
        return scats

    return _find_subcategories_in_container(app_root, s_scat)


def send_spp_changes_report(app_root: AppRoot, contacts: list[str], products: list[Product]) -> dict[str, Exception]:
    """
    Send report about SPP changes for all given products.
    Conforms and updates ``app_root.entity_descr_to_report_sent_at`` index.
    @return: contact => Exception, for all send failures.
    """
    # filter out products for whose report was recently sent
    idx = app_root.entity_descr_to_report_sent_at
    dt_past = datetime.now(timezone.utc) - timedelta(minutes=settings.report_changes_delay_interval)
    products = [x for x in products if f'article:{x.article}' not in idx or idx[f'article:{x.article}'] < dt_past]
    if not products:
        return {}

    report_lines = ['<b>Изменения СПП</b>', '↓']
    for p in products:
        report_lines.append(
            f'{dt_fmt(p.fetched_at)}: "{html.escape(p.name)}", арт. {html.escape(p.article)}, цена {p.price}, '
            f'со скидкой {p.price_sale}, СПП <b>{p.discount_client}</b>'
        )
        descr_was = f'{dt_fmt(p.old_values["fetched_at"])} было: СПП <b>{p.old_values["discount_client"]}</b>'
        if 'name' in p.old_values:
            descr_was += f', название "{html.escape(p.old_values["name"])}"'
        if 'price' in p.old_values:
            descr_was += f', цена {p.old_values["price"]}'
        if 'price_sale' in p.old_values:
            descr_was += f', цена со скидкой {p.old_values["price_sale"]}'
        report_lines.append(descr_was)
        report_lines.append('')

    report_text = '\n'.join(report_lines)
    log.info(f'send changes report to users')
    send_errors = send_to_telegram_multiple(settings.telegram_bot_token, contacts, report_text)

    if any(x not in send_errors for x in contacts):
        # report was sent to at least one recipient, save the current date/time in the index
        for p in products:
            idx[f'article:{p.article}'] = datetime.now(timezone.utc)

    return send_errors


def monitor_articles(app_root: AppRoot, params: Params, conn: ZODB.Connection.Connection) -> dict[str, Exception]:
    """
    Fetch product updates and send report to all users in case of SPP change. Does all in a new transaction.
    If any SPP changed, and it was not possible to send a report to at least one contact, rollbacks transaction.
    @return: failed entity descriptor => Exception, for each failure
    """
    class SilentlyRollbackTransaction(Exception):
        pass

    failures = {}
    try:
        with in_transaction(conn):
            products, new_products_num, article_failures = fetch_product_updates(app_root, params.monitor_articles)
            log.info(f'fetched new: {new_products_num}, updated: {len(products)}, failed: {len(article_failures)}')
            failures.update({
                f'article:{k}': v for k, v in article_failures.items()
            })

            # filter products with changed SPP
            products_spp_changed = [x for x in products if 'discount_client' in x.old_values]
            if products_spp_changed:
                # there are products with changed SPP, send report to users
                send_failures = send_spp_changes_report(app_root, params.contacts_users, products_spp_changed)
                failures.update({
                    f'send to {k}': v for k, v in send_failures.items()
                })

                if all(x in send_failures for x in params.contacts_users):
                    # failed to send a report to at least one user, raise an exception to rollback database changes
                    raise SilentlyRollbackTransaction()

    except SilentlyRollbackTransaction:
        pass

    return failures


def send_admin_report(app_root: AppRoot, params: Params, failures: dict[str, Exception]):
    """
    Send error report to all configured admin contacts.
    Conforms and updates ``app_root.entity_descr_to_report_sent_at`` index.
    If it was not possible to send a report to at least one admin contact, raises an exception.
    In case of some admin contacts failed, sends additional error report about that to other admin contacts.
    """
    def _send_failures_to_admins(header: str, failures_: dict[str, Exception]) -> dict[str, Exception]:
        # filter out failures for whose report was recently sent
        idx = app_root.entity_descr_to_report_sent_at
        dt_past = datetime.now(timezone.utc) - timedelta(minutes=settings.report_errors_delay_interval)
        failures_ = {k: v for k, v in failures_.items() if k not in idx or idx[k] < dt_past}
        if not failures_:
            return {}

        report_text = '\n'.join([f'<b>{header}</b>', '↓'] + [
            f'• {html.escape(k)}: {html.escape(str(v))}' for k, v in failures_.items()
        ] + [''])

        log.info(f'send errors report to admins')
        send_failures_ = send_to_telegram_multiple(settings.telegram_bot_token, params.contacts_admins, report_text)
        if all(x in send_failures_ for x in params.contacts_admins):
            raise RuntimeError(f'failed to send error report to any of configured admin contacts')

        # report was sent to at least one recipient, save the current date/time in the index
        for k in failures_:
            idx[k] = datetime.now(timezone.utc)

        return send_failures_

    send_failures = _send_failures_to_admins('Failures', failures)
    if send_failures:
        failures = {f'send to {k}': v for k, v in send_failures.items()}
        _send_failures_to_admins('Cannot send error report to the following contacts', failures)


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
        conn: ZODB.Connection.Connection = get_connection(request)
        app_root = get_app_root(conn)

        log.info('try to fetch product updates for all configured articles')
        failures = monitor_articles(app_root, params, conn)

        if failures:
            with in_transaction(conn):
                send_admin_report(app_root, params, failures)


if __name__ == '__main__':
    main()
