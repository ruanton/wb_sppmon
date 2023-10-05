"""
Input params
"""

from .settings import settings


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


class ProductSubcategoryParams:
    """Params for monitoring Wildberries product subcategory"""
    def __init__(self, input_line: str):
        """Parse and validate product subcategory params input line"""
        tokens = [x.strip() for x in input_line.split(',')]
        try:
            self.price_step = int(tokens.pop().strip())
            self.price_max = int(tokens.pop().strip())
            self.price_min = int(tokens.pop().strip())
            self.subcategory_search = tokens.pop().strip()
            self.category_search = tokens.pop().strip()
            if tokens:
                raise ValueError('too many columns to unpack')
            if not self.subcategory_search:
                raise ValueError(f'subcategory is empty')
            if not 0 <= self.price_min <= self.price_max:
                raise ValueError(f'not 0 <= {self.price_min} <= {self.price_max}')
            if not 0 <= self.price_step <= self.price_max - self.price_min:
                raise ValueError(f'not 0 <= {self.price_step} <= {self.price_max} - {self.price_min}')

        except Exception as e:
            raise ValueError(f'invalid product subcategory params: {input_line}: {e}') from e

    def __str__(self):
        return (
            f'{self.subcategory_search}, {self.category_search}, '
            f'{self.price_min}, {self.price_max}, {self.price_step}'
        )

    @property
    def scat_search_descriptor(self) -> str:
        """Human-readable subcategory search params descriptor"""
        return f'{self.category_search or "(any)"} → {self.subcategory_search}'


class Params:
    """Input params"""
    def __init__(self):
        """
        Load and validate input params from global settings and auxiliary files.
        """
        self.contacts_admins = _read_lines(settings.contacts_admins_file)
        if any(not x.startswith('telegram:') or not x.split(':')[1].isdigit() for x in self.contacts_admins):
            raise ValueError(f'invalid admins contacts')

        self.contacts_users = _read_lines(settings.contacts_users_file)
        if any(not x.startswith('telegram:') or not x.split(':')[1].isdigit() for x in self.contacts_users):
            raise ValueError(f'invalid users contacts')

        self.monitor_articles = _read_lines(settings.monitor_articles_file)

        monitor_subcategories_lines = _read_lines(settings.monitor_subcategories_file)
        self.monitor_subcategories = [ProductSubcategoryParams(x) for x in monitor_subcategories_lines]

    def __str__(self) -> str:
        lines = [
            f'contacts admins: {", ".join(self.contacts_admins)}',
            f'contacts users: {", ".join(self.contacts_users)}',
            f'monitor articles: {", ".join(self.monitor_articles)}',
            f'monitor subcategories:'
        ] + [f'  {x}' for x in self.monitor_subcategories]
        return '\n'.join(lines)
