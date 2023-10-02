import sys
import logging
import argparse
import typing
import ZODB.Connection
from pyramid.paster import bootstrap, setup_logging
from pyramid.registry import Registry
from pyramid.request import Request
from pyramid_zodbconn import get_connection

# noinspection PyUnresolvedReferences
from BTrees.OOBTree import OOBTree
# noinspection PyUnresolvedReferences
from BTrees.IOBTree import IOBTree

# local imports
from .params import Params, Settings
from .wildberries import UnexpectedResponse, fetch_product_details, fetch_categories, fetch_subcategories
from .models import AppRoot, get_app_root
from .models.tcm import in_transaction
from .models.wb import LastUpdateResult, Product, Category, Subcategory

log = logging.getLogger(__name__)


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

        # add the category to lw_name_to_category dict
        lw_name = category.name.lower()
        if lw_name in lw_name_to_cat:
            # a category or a set of categories with this name already exist in mapping
            if isinstance(lw_name_to_cat[lw_name], Category):
                lw_name_to_cat[lw_name] = {lw_name_to_cat[lw_name], category}  # convert to a set
            else:
                lw_name_to_cat[lw_name].add(category)  # add to the set
        else:
            lw_name_to_cat[lw_name] = category  # set mapping to single entity

        # add the category to lw_seo_to_category dict
        if category.seo:
            lw_seo = category.seo.lower()
            if lw_seo in lw_seo_to_cat:
                # a category or a set of categories with this seo already exist in mapping
                if isinstance(lw_seo_to_cat[lw_seo], Category):
                    lw_seo_to_cat[lw_seo] = {lw_seo_to_cat[lw_seo], category}  # convert to a set
                else:
                    lw_seo_to_cat[lw_seo].add(category)  # add to the set
            else:
                lw_seo_to_cat[lw_seo] = category  # set mapping to single entity

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
    Updates app_root.lw_name_to_subcategory mapping.
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

        # update app_root.lw_name_to_subcategory mapping
        if lw_name in app_root.lw_name_to_subcategory:
            # a subcategory or a set of subcategories with this name already exist in mapping
            if isinstance(app_root.lw_name_to_subcategory[lw_name], Subcategory):
                # convert to a set
                app_root.lw_name_to_subcategory[lw_name] = {app_root.lw_name_to_subcategory[lw_name], scat}
            else:
                app_root.lw_name_to_subcategory[lw_name].add(scat)  # add to the set
        else:
            app_root.lw_name_to_subcategory[lw_name] = scat  # set mapping to single entity

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
        c_fetched_at_str = f'{c.fetched_at:%Y-%m-%d %H:%M}'
        par_str = c.parent_id or ''
        seo_str = c.seo or ''
        qry_str = c.query or ''
        # noinspection PyProtectedMember
        if not c._id_to_subcategory:
            print(f';{c.id};;{c.name};{seo_str};{par_str};{qry_str};{url};{c_fetched_at_str}')
        else:
            for sc in c.id_to_subcategory.values():
                sc_fetched_at_str = f'{sc.fetched_at:%Y-%m-%d %H:%M}'
                print(f'{sc.id};{c.id};{sc.name};{c.name};{seo_str};{par_str};{qry_str};{url};{sc_fetched_at_str}')


def get_matched_items(settings: Settings, items: typing.Iterable[tuple[str, object | set]], search: str) -> set:
    """
    Get all items with a key starting with a given string and the remaining suffix within the configured threshold
    @param items: iterable to search in
    @param search: searching string
    @param settings: application settings
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


def find_categories(settings: Settings, app_root: AppRoot, search: str | int) -> set[Category]:
    """Find categories by ID, name or seo"""
    id_to_cat = app_root.id_to_category;              id_to_cat: IOBTree
    lw_name_to_cat = app_root.lw_name_to_category;    lw_name_to_cat: OOBTree
    lw_seo_to_cat = app_root.lw_seo_to_category;      lw_seo_to_cat: OOBTree

    if isinstance(search, int) or search.isdigit():
        # search by category ID
        cat_id = int(search)
        return id_to_cat[cat_id] if cat_id in id_to_cat else set()

    else:
        # search by category name or seo, case-insensitive
        search = search.lower()
        chars_stripped = 0
        while len(search) >= settings.search_min_chars and chars_stripped <= settings.search_max_suffix:
            key_max = search + chr(sys.maxunicode)
            matched = get_matched_items(settings, lw_name_to_cat.items(min=search, max=key_max), search)
            matched.update(get_matched_items(settings, lw_seo_to_cat.items(min=search, max=key_max), search))
            if matched:
                return matched

            search = search[:-1]
            chars_stripped += 1

        return set()


def find_subcategories(settings: Settings, category: Category, search: str | int) -> set[Subcategory]:
    """
    Find subcategories in given category by ID or name
    @param category: to find subcategories in
    @param search: ID or string to search in name
    @param settings: application settings
    @return: set of subcategories, can be empty
    """
    id_to_scat = category.id_to_subcategory;             id_to_scat: IOBTree
    lw_name_to_scat = category.lw_name_to_subcategory;   lw_name_to_scat: OOBTree

    if isinstance(search, int) or search.isdigit():
        # search by subcategory ID
        scat_id = int(search)
        return id_to_scat[scat_id] if scat_id in id_to_scat else set()

    else:
        # search by sub category name, case-insensitive
        search = search.lower()
        chars_stripped = 0
        while len(search) >= settings.search_min_chars and chars_stripped <= settings.search_max_suffix:
            key_max = search + chr(sys.maxunicode)
            matched = get_matched_items(settings, lw_name_to_scat.items(min=search, max=key_max), search)
            if matched:
                return matched

            search = search[:-1]
            chars_stripped += 1

        return set()


def main():
    parser = argparse.ArgumentParser(description='Wildberries SPP Monitor.')
    parser.add_argument('config_uri', help='The URI to the main configuration file.')
    args = parser.parse_args()

    # setup logging from config file settings
    setup_logging(args.config_uri)

    # bootstrap Pyramid environment to get configuration
    with bootstrap(args.config_uri) as env:
        registry: Registry = env['registry']
        request: Request = env['request']
        settings = Settings(registry.settings)  # application custom settings

        log.info('load and validate input params')
        params = Params(registry.settings)
        print(f'=== Inputs:\n{params}\n')

        log.info('get database connection and App Root object')
        conn = get_connection(request)
        app_root = get_app_root(conn)

        log.info('try to fetch product updates for all articles from input params')
        with in_transaction(conn):
            products, new_products_num, exceptions = fetch_product_updates(app_root, params.product_articles)

        if new_products_num:
            print(f'=== New products: {new_products_num}')
        if products:
            print(f'=== Updated products:')
            print('\n'.join([f'  {v}, old_values: {v.old_values}' for v in products]))
        if exceptions:
            print(f'\n=== Failed to fetch details for product articles:')
            print('\n'.join([f'  {x}: {v}' for x, v in exceptions.items()]))


if __name__ == '__main__':
    main()
