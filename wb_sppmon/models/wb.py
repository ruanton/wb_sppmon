"""
Persistent models of Wildberries entities
"""

from decimal import Decimal
from datetime import datetime
from persistent import Persistent

# module imports
from wb_sppmon.helpers import update_object


class Product(Persistent):
    """Wildberries product"""
    def __init__(
            self, article: str, name: str, price: Decimal, price_sale: Decimal,
            discount_base: Decimal, discount_client: Decimal, fetched_at: datetime
    ):
        self.article = article;                  """Product article"""
        self.name = name;                        """Product name"""
        self.price = price;                      """Base product price"""
        self.price_sale = price_sale;            """Discounted product price"""
        self.discount_base = discount_base;      """Base discount"""
        self.discount_client = discount_client;  """Client's discount"""
        self.fetched_at = fetched_at;            """Date/time product details were fetched from Wildberries"""
        self._v_old_values: dict | None = None;  """Previous values of changed fields, not persist"""

    def __str__(self):
        return f'{self.article}: {self.name}, {self.price}, sale: {self.price_sale}, spp: {self.discount_client}'

    @property
    def old_values(self) -> dict | None:
        """Previous values of changed fields; for new entity or entity loaded from database returns None"""
        return self._v_old_values if hasattr(self, '_v_old_values') else None

    def update(self, fetched_at: datetime, **kwargs) -> bool:
        """
        Update product properties to newly fetched values. Saves previous values to _v_old_values.
        Updates fetched_at if any of the given fields have new value.
        If no fields changed, does not update entity and does not change fetched_at,
        but creates empty dictionary _v_old_values.

        @param fetched_at: date/time the values was fetched
        @param kwargs: new field values
        @return: True if entity was updated
        """
        self._v_old_values = update_object(self, **kwargs)
        if self._v_old_values:
            self._v_old_values['fetched_at'] = self.fetched_at
            self.fetched_at = fetched_at

        return bool(self._v_old_values)
