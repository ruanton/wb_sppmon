import logging
import argparse
from pyramid.paster import bootstrap, setup_logging
from pyramid.registry import Registry
from pyramid.request import Request
from pyramid_zodbconn import get_connection
from ZODB.Connection import Connection

# local imports
from .params import Params
from .wildberries import fetch_product_details
from .models import in_transaction, ZRK_ARTICLE_TO_PRODUCT
from .models.wb import Product

log = logging.getLogger(__name__)


def fetch_product_updates(conn: Connection, articles: list[str]) -> tuple[list[Product], int, dict[str, Exception]]:
    """
    Try to fetch product details for given product articles. Returns updated products only.
    Every returned Product entity have old_values volatile property with previous fields.
    @param conn: database connection
    @param articles: list of articles
    @return: (
      • list of updated Product entities,
      • number of new products,
      • mapping: article => exception, for all articles failed to fetch
    )
    """
    article_to_product: dict[str, Product] = conn.root()[ZRK_ARTICLE_TO_PRODUCT]  # persistent BTree: article => product
    updated_products: list[Product] = []
    article_to_exception: dict[str, Exception] = {}

    new_products_num = 0
    for article in articles:
        try:
            fetch_started_at, product_details = fetch_product_details(article)

            if article in article_to_product:
                # get entity from database
                product = article_to_product[article]
                if product.update(fetch_started_at, **product_details):
                    updated_products.append(product)
            else:
                # create new entity
                product = Product(article=article, **product_details, fetched_at=fetch_started_at)
                article_to_product[article] = product
                new_products_num += 1

        except Exception as e:
            article_to_exception[article] = e

    return updated_products, new_products_num, article_to_exception


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

        log.info('get database connection')
        conn = get_connection(request)

        log.info('try to fetch product updates for all articles from input params')
        with in_transaction(conn):
            products, new_products_num, exceptions = fetch_product_updates(conn, params.product_articles)

        print(f'=== Inputs:\n{params}\n')
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
