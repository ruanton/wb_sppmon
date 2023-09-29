"""
Input params
"""


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
            self.product_name = tokens.pop().strip()
            self.category_name = tokens.pop().strip()
            if tokens:
                raise ValueError('too many columns to unpack')
            if not self.product_name:
                raise ValueError(f'product name is empty')
            if not self.category_name:
                raise ValueError(f'category name is empty')
            if not 0 <= self.price_min <= self.price_max:
                raise ValueError(f'not 0 <= {self.price_min} <= {self.price_max}')
            if not 0 <= self.price_step <= self.price_max - self.price_min:
                raise ValueError(f'not 0 <= {self.price_step} <= {self.price_max} - {self.price_min}')

        except Exception as e:
            raise ValueError(f'invalid product category params: {input_line}: {e}') from e

    def __str__(self):
        return f'{self.category_name}, {self.product_name}, {self.price_min}, {self.price_max}, {self.price_step}'


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

        self.product_articles = _read_lines(settings['product_articles'])

        product_categories_lines = _read_lines(settings['product_categories'])
        self.product_categories = [ProductCategoryParams(x) for x in product_categories_lines]

    def __str__(self) -> str:
        lines = [
            f'admin emails: {", ".join(self.admin_emails)}',
            f'report emails: {", ".join(self.report_emails)}',
            f'product articles: {", ".join(self.product_articles)}',
            f'product categories:'
        ] + [f'  {x}' for x in self.product_categories]
        return '\n'.join(lines)
