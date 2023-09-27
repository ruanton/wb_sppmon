import argparse
from pyramid.paster import bootstrap, setup_logging

# local imports
from .wildberries import fetch_product_details, ProductDetails


def _read_lines(filename: str) -> list[str]:
    """
    Read all non-empty and no-comment lines from text file.

    @param filename: file name to read
    @return: all meaningful lines, stripped
    """
    with open(filename, encoding='utf-8') as f:
        lines = f.readlines()

    # filter out comments and empty lines
    lines_filtered = [x.strip() for x in lines if x.strip() and not x.strip().startswith('#')]

    return lines_filtered


class ProductCategoryParams:
    """Params for monitoring Wildberries product category"""
    def __init__(self, input_line: str):
        """Parse and validate product category params input line"""
        tokens = [x.strip() for x in input_line.split(',')]
        try:
            self.price_step = int(tokens.pop().strip())
            self.price_max = int(tokens.pop().strip())
            self.price_min = int(tokens.pop().strip())
            self.product = tokens.pop().strip()
            self.category = tokens.pop().strip()
            if tokens:
                raise ValueError('too many columns to unpack')
            if not self.product:
                raise ValueError(f'product name is empty')
            if not self.category:
                raise ValueError(f'category name is empty')
            if not 0 <= self.price_min <= self.price_max:
                raise ValueError(f'not 0 <= {self.price_min} <= {self.price_max}')
            if not 0 <= self.price_step <= self.price_max - self.price_min:
                raise ValueError(f'not 0 <= {self.price_step} <= {self.price_max} - {self.price_min}')

        except Exception as e:
            raise ValueError(f'invalid product category params: {input_line}') from e

    def __str__(self):
        return f'{self.category}, {self.product}, {self.price_min}, {self.price_max}, {self.price_step}'


class Params:
    """Input params"""
    def __init__(self, settings: dict):
        """
        Load and validate input params from settings.
        @param settings: dictionary from config file main section
        """
        self.admin_emails = _read_lines(settings['admin_emails'])
        if any('@' not in x for x in self.admin_emails):
            raise ValueError(f'invalid admin emails')

        self.report_emails = _read_lines(settings['report_emails'])
        if any('@' not in x for x in self.report_emails):
            raise ValueError(f'invalid report emails')

        self.article_numbers = _read_lines(settings['article_numbers'])

        product_categories_lines = _read_lines(settings['product_categories'])
        self.product_categories = [ProductCategoryParams(x) for x in product_categories_lines]

    def __str__(self) -> str:
        lines = [
            f'admin emails: {", ".join(self.admin_emails)}',
            f'report emails: {", ".join(self.report_emails)}',
            f'article numbers: {", ".join(self.article_numbers)}',
            f'product categories:'
        ]
        for cat in self.product_categories:
            lines.append(f'  {cat}')
        return '\n'.join(lines)


def fetch_product_details_for_articles(params: Params) -> dict[str, ProductDetails | Exception]:
    """
    Try to fetch product details for configured articles
    @param params: input parameters with article numbers
    @return: mapping article => ProductDetails or Exception
    """
    article_to_product_details = {}
    for article in params.article_numbers:
        try:
            article_to_product_details[article] = fetch_product_details(article)
        except Exception as ex:
            article_to_product_details[article] = ex
    return article_to_product_details


def main():
    parser = argparse.ArgumentParser(description='Wildberries SPP Monitor.')
    parser.add_argument('config_uri', help='The URI to the main configuration file.')
    args = parser.parse_args()

    # setup logging from config file settings
    setup_logging(args.config_uri)

    with bootstrap(args.config_uri) as env:
        settings = env['registry'].settings

        # load and validate input params
        params = Params(settings)

        # try to fetch product details for all articles from input params
        article_to_product_details = fetch_product_details_for_articles(params)

        print(f'Inputs:\n{params}')
        print(f'Fetched details for products:')
        print('\n'.join([f'  {v}' for v in article_to_product_details.values() if not isinstance(v, Exception)]))
        print(f'Failed to fetch details for products:')
        print('\n'.join([f'  {x}: {v}' for x, v in article_to_product_details.items() if isinstance(v, Exception)]))


if __name__ == '__main__':
    main()
