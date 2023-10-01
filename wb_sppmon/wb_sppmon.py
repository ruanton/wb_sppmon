import logging
import argparse
from pyramid.paster import bootstrap, setup_logging
from pyramid.registry import Registry
from pyramid.request import Request
from pyramid_zodbconn import get_connection

# local imports
from .params import Params
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
    Updates name_to_category mapping.
    Raises exception on fetch or parse error.
    @param app_root: App Root persistent object
    """
    fetch_started_at, product_categories_list = fetch_categories()

    new_cats_num, updated_cats_num, unchanged_cats_num, name_to_cat = 0, 0, 0, {}
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

        # add the category to name_to_category dict
        if category.name in name_to_cat:
            # a category or a set of categories with this name already exist in mapping
            if isinstance(name_to_cat[category.name], Category):
                name_to_cat[category.name] = {name_to_cat[category.name], category}  # convert to a set
            else:
                name_to_cat[category.name].add(category)  # add to the set
        else:
            name_to_cat[category.name] = category  # set mapping to single entity

    # update persistent mapping if required, do not delete old names
    for name, cat in name_to_cat.items():
        if name not in app_root.name_to_category or app_root.name_to_category[name] != cat:
            app_root.name_to_category[name] = cat

    app_root.categories_last_update = LastUpdateResult(
        fetched_at=fetch_started_at,
        num_new=new_cats_num,
        num_updated=updated_cats_num,
        num_gone=len(app_root.id_to_category) - new_cats_num - updated_cats_num - unchanged_cats_num
    )


def update_subcategories(category: Category):
    """
    Fetch all subcategories for the given product category.
    Updates category entity: creates new subcategories, updates existing, but does not delete disappearing ones.
    Updates name_to_subcategory mapping.
    Raises exception on fetch or parse error.
    @param category: Product category entity to update its subcategories
    """
    if not category.shard or not category.query:
        raise ValueError(f'category {category} must have "shard" and "query" properties to fetch subcategories')

    fetch_started_at, subcategories_list = fetch_subcategories(category.shard, cat_filter=category.query)

    new_scats_num, updated_scats_num, unchanged_scats_num, name_to_scat = 0, 0, 0, {}
    for scat_props in subcategories_list:
        scat_id = scat_props['id']; del scat_props['id']  # delete "id" field to conform persistent entity
        scat_name = scat_props['name']

        if scat_id in category.id_to_subcategory:
            # get existing entity from database
            subcategory = category.id_to_subcategory[scat_id]
            if subcategory.fetched_at == fetch_started_at:
                # we have already seen this ID in the response
                raise UnexpectedResponse(f'several subcategories with ID {scat_id}: {subcategory.name}, {scat_name}')

            if subcategory.update(fetch_started_at, **scat_props):
                updated_scats_num += 1
            else:
                unchanged_scats_num += 1

        else:
            # create new entity
            subcategory = Subcategory(id_=scat_id, **scat_props, fetched_at=fetch_started_at, category=category)
            category.id_to_subcategory[scat_id] = subcategory
            new_scats_num += 1

        # get existing subcategory with this name if any
        ex_scat = category.name_to_subcategory[scat_name] if scat_name in category.name_to_subcategory else None

        # verify we got no duplicates
        if ex_scat and ex_scat.fetched_at == fetch_started_at and ex_scat != subcategory:
            # we have already seen this name in the response
            raise UnexpectedResponse(f'several sub cats with name {scat_name}: {scat_id}, {ex_scat.id}')

        # if no existing subcategory, or it differs, update name_to_subcategory dict
        if ex_scat != subcategory:
            category.name_to_subcategory[scat_name] = subcategory

        # verify consistency
        if subcategory.category != category:
            raise RuntimeError(f'subcategory.category != category: {subcategory.category} != {category}')

    # of for scat_props in subcategories_list

    # save results of the update
    category.subcategories_last_update = LastUpdateResult(
        fetched_at=fetch_started_at,
        num_new=new_scats_num,
        num_updated=updated_scats_num,
        num_gone=len(category.id_to_subcategory) - new_scats_num - updated_scats_num - unchanged_scats_num
    )


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

        log.info('fetch, parse and save to database all product categories')
        with in_transaction(conn):
            update_product_categories(app_root)

        lur = app_root.categories_last_update
        print(f'=== Categories added: {lur.num_new}, updated: {lur.num_updated}, disappeared: {lur.num_gone}')

        log.info('update subcategories for all product categories')
        for cat in app_root.id_to_category.values():
            if not cat.query or not cat.shard:
                continue
            if cat.subcategories_last_update:
                continue
            print(f'trying to update subcategories for {cat}')
            try:
                with in_transaction(conn):
                    update_subcategories(cat)
                lur = cat.subcategories_last_update
                print(f'=== Scats added: {lur.num_new}, updated: {lur.num_updated}, disappeared: {lur.num_gone}')
            except Exception as e:
                log.warning(f'error: {e}')


if __name__ == '__main__':
    main()
