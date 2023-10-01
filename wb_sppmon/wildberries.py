"""
Interface to Wildberries website
"""

import logging
import typing
from decimal import Decimal
from datetime import datetime, timezone

# local imports
from . import helpers

log = logging.getLogger(__name__)

URL_WB_DETAILS = (
    'https://card.wb.ru/cards/detail?'
    'appType=1&spp=32&curr=rub&dest=-1257786&regions=80,38,83,4,64,33,68,70,30,40,86,75,69,1,31,66,110,48,22,71,114'
    '&nm={article}'
)
"""Returns JSON with details about the product with the given article"""


URL_WB_CATEGORIES = 'https://static-basket-01.wb.ru/vol0/data/main-menu-ru-ru-v2.json'
"""Returns JSON with all product categories"""


URL_WB_FILTERS = (
    'https://catalog.wb.ru/catalog/{shard}/v4/filters?'  # category shard as got from Wildberries
    'appType=1&curr=rub&dest=-1257786&regions=80,38,83,4,64,33,68,70,30,40,86,75,69,1,31,66,110,48,22,71,114'
    '&{cat_filter}'  # category subfilter like "cat=8225" as specified in category details
)
"""Returns the available filters including a list of subcategories for a given category subfilter"""

SUBCATEGORY_FILTER_NAME = 'Категория';  """Name of filtering by subcategory"""
SUBCATEGORY_FILTER_KEY = 'xsubject';    """Key for URL query for this filtering"""


class WildberriesWebsiteError(Exception):
    """Error fetching info from Wildberries website"""


class UnexpectedResponse(WildberriesWebsiteError):
    """Unexpected response from Wildberries website"""


class NoProductsFound(WildberriesWebsiteError):
    """No products returned from Wildberries website"""


class SeveralProductsFound(WildberriesWebsiteError):
    """Several products returned when one was expected"""


def fetch_product_details(article: str) -> tuple[datetime, dict[str, typing.Any]]:
    """
    Fetch some product details from the Wildberries website by article.
    @param article: product article
    @return: date/time the fetching started, dictionary of the product properties fetched from Wildberries
    """
    try:
        log.debug(f'fetch product details for article {article} from Wildberries website and parse response')
        fetch_started_at = datetime.now(tz=timezone.utc)
        url = URL_WB_DETAILS.format(article=article)
        resp = helpers.http_get(url)
        if not resp.content:
            raise UnexpectedResponse('no content')
        json_resp = resp.json()
        json_resp_repr = f'json response:\n{helpers.json_dumps(json_resp)}'

        # parse json response
        if 'data' not in json_resp:
            raise UnexpectedResponse(f'no "data" in {json_resp_repr}')
        if 'products' not in json_resp['data']:
            raise UnexpectedResponse(f'no "data->products" in {json_resp_repr}')
        json_products = json_resp['data']['products']
        if not json_products:
            raise NoProductsFound(f'no products found, {json_resp_repr}')
        if len(json_products) > 1:
            raise SeveralProductsFound(f'got several products, {json_resp_repr}')
        json_product = json_products[0]
        if 'extended' not in json_product:
            raise UnexpectedResponse(f'no "data->products[0]->extended" in {json_resp_repr}')
        article_from_wb = str(json_product['id'])
        if article_from_wb != article:
            raise UnexpectedResponse(f'got different article: {article_from_wb} != {article}, {json_resp_repr}')

        return (
            fetch_started_at,
            {
                'name': json_product['name'],
                'price': Decimal(str(int(json_product['priceU']) / 100.0)),
                'price_sale': Decimal(str(int(json_product['salePriceU']) / 100.0)),
                'discount_base': Decimal(str(int(json_product['extended']['basicSale']))),
                'discount_client': Decimal(str(int(json_product['extended']['clientSale']))),
            }
        )

    except Exception as e:
        raise WildberriesWebsiteError(f'cannot fetch product details for article {article}: {e}') from e


def fetch_categories() -> tuple[datetime, list[dict[str, int | str | bool]]]:
    """
    Fetch and parse all product categories from the Wildberries website.
    Ignore a tree structure in json returned from Wildberries.
    @return: date/time the fetching started, list of product categories
    """
    try:
        log.debug(f'fetch product categories from the Wildberries website and parse response')
        fetch_started_at = datetime.now(tz=timezone.utc)
        resp = helpers.http_get(URL_WB_CATEGORIES)
        if not resp.content:
            raise UnexpectedResponse('no content')
        json_resp = resp.json()

        # parse json response
        categories: list[dict[str, int | str | bool]] = []

        def parse_categories(json_categories: list):
            for cat in json_categories:
                try:
                    categories.append({
                        'id': cat['id'],
                        'parent': cat['parent'] if 'parent' in cat else None,
                        'name': cat['name'],
                        'seo': cat['seo'] if 'seo' in cat else None,
                        'url': cat['url'],
                        'shard': cat['shard'] if 'shard' in cat else None,
                        'query': cat['query'] if 'query' in cat else None,
                        'landing': cat['landing'] if 'landing' in cat else None,
                        'children_num': len(cat['childs']) if 'childs' in cat else 0,
                    })
                    if 'childs' in cat:
                        parse_categories(cat['childs'])  # recursively parse subcategories

                except Exception as ex:
                    raise UnexpectedResponse(f'cannot parse category, json: {cat}') from ex

        parse_categories(json_resp)

        return fetch_started_at, categories

    except Exception as e:
        raise WildberriesWebsiteError(f'cannot fetch product categories: {e}') from e


def fetch_subcategories(shard: str, cat_filter: str) -> tuple[datetime, list[dict[str, int | str]]]:
    """
    Fetch and parse all product subcategories for a given category subfilter.
    @param shard: part of an HTTP-url
    @param cat_filter: part of an HTTP-query string like 'cat=1234'
    @return: date/time the fetching started, list of product subcategories
    """
    try:
        log.debug(f'fetch subcategories from the Wildberries website for shard: {shard}, subfilter: {cat_filter}')
        fetch_started_at = datetime.now(tz=timezone.utc)
        url = URL_WB_FILTERS.format(shard=shard, cat_filter=cat_filter)
        resp = helpers.http_get(url)
        if not resp.content:
            raise UnexpectedResponse('no content')
        json_resp = resp.json()
        json_resp_repr = f'json response:\n{helpers.json_dumps(json_resp)}'

        # parse json response
        req_name, req_key = SUBCATEGORY_FILTER_NAME, SUBCATEGORY_FILTER_KEY
        if 'data' not in json_resp:
            raise UnexpectedResponse(f'no "data" in {json_resp_repr}')
        if 'filters' not in json_resp['data']:
            raise UnexpectedResponse(f'no "data->filters" in {json_resp_repr}')
        json_filters = json_resp['data']['filters']
        json_filters_selected = [x for x in json_filters if x['name'] == req_name or x['key'] == req_key]
        if not json_filters_selected:
            raise UnexpectedResponse(f'filter with the name "{req_name}" was not found in {json_resp_repr}')
        if not json_filters_selected:
            raise UnexpectedResponse(f'several filters with the name "{req_name}" were found in {json_resp_repr}')
        json_filter = json_filters_selected[0]
        if json_filter['name'] != req_name or json_filter['key'] != req_key:
            raise UnexpectedResponse(f'unexpected filter "{req_name}" returned, {json_resp_repr}')
        if 'items' not in json_filter:
            raise UnexpectedResponse(f'no "items" in "{req_name}" filter, {json_resp_repr}')

        # parse json response
        subcategories: list[dict[str, int | str]] = []
        for scat in json_filter['items']:
            subcategories.append({
                'id': scat['id'],
                'name': scat['name'],
            })

        return fetch_started_at, subcategories

    except Exception as e:
        raise WildberriesWebsiteError(f'cannot fetch product subcategories for cat filter {cat_filter}: {e}') from e
