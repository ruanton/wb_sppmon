"""
Interface to Wildberries website
"""

import logging
import math
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

URL_WB_PRODUCTS = (
    'https://catalog.wb.ru/catalog/{shard}/catalog?'  # category shard as got from Wildberries
    'appType=1&curr=rub&dest=-1257786&regions=80,38,83,4,64,33,68,70,30,40,86,75,69,1,66,110,22,48,31,71,114'
)
"""Returns list of products by given filters, such as: cat=8988, page=20, priceU=60000;90000, sort=popular, etc."""

SUBCATEGORY_FILTER_NAME = 'Категория';  """Name of filtering by subcategory"""
SUBCATEGORY_FILTER_KEY = 'xsubject';    """Key for URL query for filtering by subcategory"""

UNREAL_BIG_PRICE = Decimal('999999999')


class WildberriesWebsiteError(Exception):
    """Error fetching info from Wildberries website"""


class UnexpectedResponse(WildberriesWebsiteError):
    """Unexpected response from Wildberries website"""


class NoProductsFound(WildberriesWebsiteError):
    """No products returned from Wildberries website"""


class SeveralProductsFound(WildberriesWebsiteError):
    """Several products returned when one was expected"""


def parse_json_with_products(json_resp: dict, single_expected=False, article_expected: str = None) -> dict:
    """
    Parse json got from Wildberries.
    @return: json part with product list
    """
    json_resp_str = f'json response:\n{helpers.json_dumps(json_resp)}'
    if 'data' not in json_resp:
        raise UnexpectedResponse(f'no "data" in {json_resp_str}')
    if 'products' not in json_resp['data']:
        raise UnexpectedResponse(f'no "data->products" in {json_resp_str}')
    json_products = json_resp['data']['products']

    if single_expected or article_expected:
        if not json_products:
            raise NoProductsFound(f'no products found, {json_resp_str}')
        if len(json_products) > 1:
            raise SeveralProductsFound(f'got several products, {json_resp_str}')

    if article_expected:
        art_from_wb = str(json_products[0]['id'])
        if art_from_wb != str(article_expected):
            raise UnexpectedResponse(f'got different article: {art_from_wb} != {article_expected}, {json_resp_str}')

    return json_products


def fetch_product_details(article: str) -> tuple[datetime, dict[str, int | str | Decimal]]:
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
        json_products = parse_json_with_products(json_resp, article_expected=article)
        json_product = json_products[0]
        json_product_str = f'json product:\n{helpers.json_dumps(json_product)}'
        if 'extended' not in json_product:
            raise UnexpectedResponse(f'no "extended" in {json_product_str}')

        json_product_extended = json_product['extended']
        if 'clientSale' in json_product_extended:
            discount_client = Decimal(str(int(json_product_extended['clientSale'])))
        else:
            discount_client = Decimal(0)

        if 'basicSale' in json_product_extended:
            discount_base = Decimal(str(int(json_product_extended['basicSale'])))
        else:
            if 'sale' not in json_product:
                raise UnexpectedResponse(f'neither "extended->basicSale" nor "sale" found in {json_product_str}')
            if json_product['sale'] != discount_client:
                raise UnexpectedResponse(f'"sale" != "clientSale" in {json_product_str}')

            # if 'sale' == 'clientDale' => supplier discount is 0
            discount_base = Decimal(0)

        if 'clientSale' not in json_product_extended:
            if 'sale' not in json_product:
                raise UnexpectedResponse(f'neither "extended->clientSale" nor "sale" found in {json_product_str}')
            if json_product['sale'] != discount_base:
                raise UnexpectedResponse(f'"sale" != "basicSale" in {json_product_str}')

        product_properties = {
            'name': json_product['name'],
            'price': Decimal(str(int(json_product['priceU']) / 100.0)),
            'price_sale': Decimal(str(int(json_product['salePriceU']) / 100.0)),
            'discount_base': discount_base,
            'discount_client': discount_client,
        }
        return fetch_started_at, product_properties

    except Exception as e:
        raise WildberriesWebsiteError(f'cannot fetch product details for article {article}: {e}') from e


def fetch_products(
        shard: str, cat_id: int, xsubject: int = None,
        page=1, num_pages: int = None,
        price_from: Decimal = None, price_to: Decimal = None,
        sort: str = 'popular', **filters
) -> tuple[datetime, list[dict[str, int | str | Decimal]]]:
    """
    Fetch products details from the Wildberries website by the given filters.
    @param shard: category shard as got from Wildberries
    @param cat_id: category ID
    @param xsubject: subcategory ID
    @param page: starting page number
    @param num_pages: how many pages to fetch
    @param price_from: filter by minimum price
    @param price_to: filter by maximum price
    @param sort: sort order
    @param filters: optional additional filters
    @return: date/time the fetching started, list of dictionaries of the products properties
    """
    try:
        # convert arguments, except 'page', to filters
        if price_from or price_to:
            price_from_filter = int((price_from or Decimal(0))*100)
            price_to_filter = math.ceil((price_to or UNREAL_BIG_PRICE)*100)
            filters['priceU'] = f'{price_from_filter};{price_to_filter}'
        if sort:
            filters['sort'] = sort
        if xsubject:
            filters['xsubject'] = xsubject

        fetch_started_at = datetime.now(tz=timezone.utc)
        products_details = []
        if num_pages:
            # use recursion to fetch page by page
            if 'num_pages' in filters:
                del filters['num_pages']  # prevent infinite loop can be caused by incorrect call
            for p in range(page, page + num_pages):
                _, products_details_in_page = fetch_products(shard=shard, cat_id=cat_id, page=p, **filters)
                if products_details_in_page:
                    products_details += products_details_in_page
                else:
                    break
            return fetch_started_at, products_details

        # if called without num_pages, continue

        url = URL_WB_PRODUCTS.format(shard=shard)
        log.debug(f'fetch products details from "{url}" + filters, and parse response')
        resp = helpers.http_get(url, params=filters)
        if resp.content and len(parse_json_with_products(resp.json())) == 0:
            # no products, retrying
            resp = helpers.http_get(url, params=filters)

        if not resp.content:
            raise UnexpectedResponse('no content')
        json_resp = resp.json()
        json_products = parse_json_with_products(json_resp)

        products_properties = []
        for json_product in json_products:
            products_properties.append({
                'id': json_product['id'],
                'name': json_product['name'],
                'price': Decimal(str(int(json_product['priceU']) / 100.0)),
                'price_sale': Decimal(str(int(json_product['salePriceU']) / 100.0)),
            })

        return fetch_started_at, products_properties

    except Exception as e:
        raise WildberriesWebsiteError(f'cannot fetch products details: {e}') from e


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
            return fetch_started_at, []  # no subcategories in this category
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
