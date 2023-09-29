"""
Interface to Wildberries website
"""

import logging
from decimal import Decimal
from typing import Any
from datetime import datetime, timezone

# local imports
from .helpers import json_dumps, http_get

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
    'https://catalog.wb.ru/catalog/children_shoes/v4/filters?'
    'appType=1&curr=rub&dest=-1257786&regions=80,38,83,4,64,33,68,70,30,40,86,75,69,1,31,66,110,48,22,71,114'
    '&{cat_filter}'  # category subfilter like "cat=8225" as specified in category details
)
"""Returns the available filters including a list of subcategories for a given category subfilter"""


class WildberriesWebsiteError(Exception):
    """Error fetching info from Wildberries website"""


class UnexpectedResponse(WildberriesWebsiteError):
    """Unexpected response from Wildberries website"""


class NoProductsFound(WildberriesWebsiteError):
    """No products returned from Wildberries website"""


class SeveralProductsFound(WildberriesWebsiteError):
    """Several products returned when one was expected"""


def fetch_product_details(article: str) -> tuple[datetime, dict[str, Any]]:
    """
    Fetch some product details from Wildberries website by article.
    @param article: product article
    @return: date/time the fetching started, dictionary of the product properties fetched from Wildberries
    """
    try:
        log.debug(f'fetch product details for article {article} from Wildberries website and parse response')
        fetch_started_at = datetime.now(tz=timezone.utc)
        url = URL_WB_DETAILS.format(article=article)
        resp = http_get(url)
        json_resp = resp.json()

        # parse json response
        if 'data' not in json_resp:
            raise UnexpectedResponse(f'no "data" in json response: {json_dumps(json_resp)}')
        if 'products' not in json_resp['data']:
            raise UnexpectedResponse(f'no "data->products" in json response: {json_dumps(json_resp)}')
        json_products = json_resp['data']['products']
        if not json_products:
            raise NoProductsFound(f'no products found, json response: {json_dumps(json_resp)}')
        if len(json_products) > 1:
            raise SeveralProductsFound(f'got several products, json response: {json_dumps(json_resp)}')
        json_product = json_products[0]
        if 'extended' not in json_product:
            raise UnexpectedResponse(f'no "data->products[0]->extended" in json response: {json_dumps(json_resp)}')
        article_from_wb = str(json_product['id'])
        if article_from_wb != article:
            raise UnexpectedResponse(f'got different article: {article_from_wb} != {article}')

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
