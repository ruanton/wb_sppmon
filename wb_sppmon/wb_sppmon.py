import logging
import argparse
from pyramid.paster import bootstrap, setup_logging
from pyramid.registry import Registry
from pyramid.request import Request
from pyramid_zodbconn import get_connection

# local imports
from .params import Params
from .wildberries import UnexpectedResponse, fetch_product_details, fetch_categories
from .models import AppRoot, get_app_root
from .models.tcm import in_transaction
from .models.wb import Product, Category

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


def update_product_categories(app_root: AppRoot) -> tuple[int, int, int]:
    """
    Fetch all product categories from the Wildberries website.
    Update database: creates new categories, updates existing, does not delete disappearing ones.
    Raises exception on fetch or parse error.
    @param app_root: App Root persistent object
    @return: (
      • number of new categories,
      • number of updated categories,
      • number of category ID's disappeared from the Wildberries
    )
    """
    fetch_started_at, product_categories_list = fetch_categories()

    new_cats_num, updated_cats_num, unchanged_cats_num = 0, 0, 0

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

    disappeared_cats_num = len(app_root.id_to_category) - new_cats_num - updated_cats_num - unchanged_cats_num
    return new_cats_num, updated_cats_num, disappeared_cats_num


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
            new_cats_num, updated_cats_num, disappeared_cats_num = update_product_categories(app_root)

        print(f'=== Categories added: {new_cats_num}, updated: {updated_cats_num}, disappeared: {disappeared_cats_num}')


if __name__ == '__main__':
    main()
