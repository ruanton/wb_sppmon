"""
Persistent models of Wildberries entities
"""

from decimal import Decimal
from datetime import datetime
from persistent import Persistent

# module imports
from wb_sppmon.helpers import update_object


class FetchedEntity(Persistent):
    """Entity fetched from Wildberries"""
    def __init__(self, fetched_at: datetime):
        self.fetched_at = fetched_at;            """Date/time entity properties were fetched from Wildberries"""
        self._v_old_values: dict | None = None;  """Previous values of changed fields, not persist"""

    @property
    def old_values(self) -> dict | None:
        """Previous values of changed fields; for new entity or entity loaded from the database, None is returned"""
        return self._v_old_values if hasattr(self, '_v_old_values') else None

    def update(self, fetched_at: datetime, **kwargs) -> bool:
        """
        Update entity properties to newly fetched values. Saves previous values to _v_old_values.
        If any of the given fields have new value, fetched_at is updated.
        If no fields changed, does not update entity and does not change fetched_at,
        but creates empty dictionary _v_old_values.

        @param fetched_at: date/time the properties were fetched
        @param kwargs: new field values
        @return: True if entity was updated
        """
        self._v_old_values = update_object(self, **kwargs)
        if self._v_old_values:
            self._v_old_values['fetched_at'] = self.fetched_at
            self.fetched_at = fetched_at

        return bool(self._v_old_values)


class Product(FetchedEntity):
    """Wildberries product"""
    def __init__(
            self, article: str, name: str, price: Decimal, price_sale: Decimal, discount_base: Decimal,
            discount_client: Decimal, fetched_at: datetime
    ):
        super().__init__(fetched_at)
        self.article = article;                  """Product article"""
        self.name = name;                        """Product name"""
        self.price = price;                      """Base product price"""
        self.price_sale = price_sale;            """Discounted product price"""
        self.discount_base = discount_base;      """Base discount"""
        self.discount_client = discount_client;  """Client's discount"""

    def __str__(self):
        return f'{self.article}: {self.name}, {self.price}, sale: {self.price_sale}, spp: {self.discount_client}'


class Category(FetchedEntity):
    """Wildberries product category"""
    def __init__(
            self, id_: int, name: str, url: str, children_num: int, fetched_at: datetime,
            parent_id: int = None, shard: str = None, query: str = None, landing: bool = None
    ):
        super().__init__(fetched_at)
        self.id = id_;                     """Category ID"""
        self.name = name;                  """Name of the category"""
        self.url = url;                    """URL of the category"""
        self.children_num = children_num;  """Number of children categories as got in json from Wildberries"""
        self.parent_id = parent_id;        """Parent category ID"""
        self.shard = shard;                """Don't know what's this"""
        self.query = query;                """Query subfilter"""
        self.landing = landing;            """Don't know what's this"""

    def __str__(self):
        return f'{self.id}: {self.name}'
