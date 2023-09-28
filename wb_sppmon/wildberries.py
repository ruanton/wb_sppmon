from decimal import Decimal

# local imports
from helpers import json_dumps, http_get

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
    '&{cat}'  # category subquery like "cat=8225" as specified in category details
)
"""Returns the available filters including a list of subcategories for a given category"""


class WildberriesWebsiteError(Exception):
    """Error fetching info from Wildberries website"""


class UnexpectedResponse(WildberriesWebsiteError):
    """Unexpected response from Wildberries website"""


class NoProductsFound(WildberriesWebsiteError):
    """No products returned from Wildberries website"""


class SeveralProductsFound(WildberriesWebsiteError):
    """Several products returned when one was expected"""


class ProductDetails:
    """Wildberries product details"""
    def __init__(
            self, article: str, name: str, price: Decimal, price_sale: Decimal,
            discount_base: Decimal, discount_client: Decimal
    ):
        """
        Fetch from some details about product by product article.
        @param article: product article
        @param name: product name
        @param price: base product price
        @param price_sale: discounted product price
        @param discount_base: base discount
        @param discount_client: SPP
        """
        self.article = article
        self.name = name
        self.price = price
        self.price_sale = price_sale
        self.discount_base = discount_base
        self.discount_client = discount_client

    def __str__(self):
        return f'{self.article}: {self.name}, {self.price}, {self.price_sale}, spp: {self.discount_client}'


def fetch_product_details(article: str) -> ProductDetails:
    """Fetch some product details from wildberries.ru by article"""
    try:
        url = URL_WB_DETAILS.format(article=article)
        resp = http_get(url)
        details = resp.json()
        if 'data' not in details:
            raise UnexpectedResponse(f'no "data" in json: {json_dumps(details)}')
        if 'products' not in details['data']:
            raise UnexpectedResponse(f'no "data->products" in json: {json_dumps(details)}')
        products = details['data']['products']
        if not products:
            raise NoProductsFound(f'no products found, json: {json_dumps(details)}')
        if len(products) > 1:
            raise SeveralProductsFound(f'got several products: {json_dumps(details)}')
        product = products[0]
        if 'extended' not in product:
            raise UnexpectedResponse(f'no "data->products[0]->extended" in json: {json_dumps(details)}')

        product_details = ProductDetails(
            article=str(product['id']),
            name=product['name'],
            price=Decimal(str(int(product['priceU']) / 100.0)),
            price_sale=Decimal(str(int(product['salePriceU']) / 100.0)),
            discount_base=Decimal(str(int(product['extended']['basicSale']))),
            discount_client=Decimal(str(int(product['extended']['clientSale']))),
        )
        if product_details.article != article:
            raise UnexpectedResponse(f'got different article: {product_details.article} != {article}')

        return product_details

    except Exception as e:
        raise WildberriesWebsiteError(f'cannot fetch product details for article {article}: {e}') from e
