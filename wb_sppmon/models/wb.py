"""
Persistent models of Wildberries entities
"""

from decimal import Decimal
from datetime import datetime
from persistent import Persistent
# noinspection PyUnresolvedReferences
from BTrees.OOBTree import OOBTree
# noinspection PyUnresolvedReferences
from BTrees.IOBTree import IOBTree

# module imports
from wb_sppmon.helpers import update_object


class LastUpdateResult:
    """Results of the last successful entity set update"""
    def __init__(self, fetched_at: datetime, num_new: int, num_updated: int, num_gone: int):
        self.fetched_at = fetched_at;    """Date/time the fetching started"""
        self.num_new = num_new;          """Number of new entities"""
        self.num_updated = num_updated;  """Number of updated entities"""
        self.num_gone = num_gone;        """Number of disappeared entities"""


class FetchedEntity(Persistent):
    """Entity fetched from Wildberries"""
    def __init__(self, fetched_at: datetime = None):
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

    @staticmethod
    def fmt_article_descriptor(article: str) -> str:
        """Format human-readable article descriptor"""
        return f'article:{article}'

    @property
    def article_descriptor(self) -> str:
        """Human-readable article descriptor"""
        return self.fmt_article_descriptor(self.article)


class Subcategory(FetchedEntity):
    """Wildberries product subcategory"""
    def __init__(self, id_: int, name: str, fetched_at: datetime, category: 'Category'):
        super().__init__(fetched_at)
        self.id = id_;                     """Subcategory ID"""
        self.name = name;                  """Name of the subcategory"""
        self.category = category;          """The category this subcategory belongs to"""
        self._price_range_to_slot = None

    def __str__(self):
        return f'{self.id}: {self.name}'

    @property
    def entity_descriptor(self) -> str:
        """Human-readable subcategory descriptor"""
        return f'{self.category.id}:{self.category.name} → {self.id}:{self.name}'

    @property
    def price_range_to_slot(self) -> dict[tuple[Decimal, Decimal], 'PriceSlot']:
        """OOBTree: price range => subcategory PriceSlot entity"""
        if not hasattr(self, '_price_range_to_slot') or self._price_range_to_slot is None:
            self._price_range_to_slot = OOBTree()
        return self._price_range_to_slot

    def get_or_create_slots(self, price_min: Decimal, price_max: Decimal, step: Decimal) -> list['PriceSlot']:
        """
        Get PriceSlot entities from the database or creates new ones if they don't already exist
        @return: list of Price Slots ordered by price_from
        """
        slots = []
        price_from = price_min

        while price_from < price_max:
            price_to = price_from + step
            if price_to > price_max:
                price_to = price_max

            if (price_from, price_to) in self.price_range_to_slot:
                slot = self.price_range_to_slot[(price_from, price_to)]
            else:
                slot = PriceSlot(self, price_from, price_to)
                self.price_range_to_slot[(price_from, price_to)] = slot

            slots.append(slot)
            price_from = price_to

        return slots


class Category(FetchedEntity):
    """Wildberries product category"""
    def __init__(
            self, id_: int, name: str, url: str, children_num: int, fetched_at: datetime,
            seo: str = None, parent_id: int = None, shard: str = None, query: str = None, landing: bool = None
    ):
        super().__init__(fetched_at)
        self.id = id_;                         """Category ID"""
        self.name = name;                      """Name of the category"""
        self.url = url;                        """URL of the category"""
        self.children_num = children_num;      """Number of children categories as got in json from Wildberries"""
        self.seo = seo;                        """Maybe, full name of the category"""
        self.parent_id = parent_id;            """Parent category ID"""
        self.shard = shard;                    """Part of URL"""
        self.query = query;                    """Query subfilter"""
        self.landing = landing;                """Don't know what's this"""
        self._id_to_subcategory = None
        self._lw_name_to_subcategory = None
        self._subcategories_last_update = None

    def __str__(self):
        return f'{self.id}: {self.name}'

    @property
    def entity_descriptor(self) -> str:
        """Human-readable category descriptor"""
        return f'{self.id}:{self.name}'

    @property
    def id_to_subcategory(self) -> dict[int, Subcategory]:
        """IOBTree: subcategory ID => subcategory entity"""
        if not hasattr(self, '_id_to_subcategory') or self._id_to_subcategory is None:
            self._id_to_subcategory = IOBTree()
        return self._id_to_subcategory

    @property
    def lw_name_to_subcategory(self) -> dict[str, Subcategory]:
        """OOBTree: subcategory lowered name => subcategory entity"""
        if not hasattr(self, '_lw_name_to_subcategory') or self._lw_name_to_subcategory is None:
            self._lw_name_to_subcategory = OOBTree()
        return self._lw_name_to_subcategory

    @property
    def subcategories_last_update(self) -> LastUpdateResult | None:
        """Results of subcategories last update"""
        return self._subcategories_last_update if hasattr(self, '_subcategories_last_update') else None

    @subcategories_last_update.setter
    def subcategories_last_update(self, value: LastUpdateResult):
        self._subcategories_last_update = value


class PriceSlot(FetchedEntity):
    """Subcategory price slot with determined client discount"""
    def __init__(self, subcategory: Subcategory, price_from: Decimal, price_to: Decimal):
        super().__init__()
        self.subcategory = subcategory
        self.price_from = price_from
        self.price_to = price_to
        self.client_discount: Decimal | None = None
        self._v_articles: set[str] = set();  """product articles, not persist"""

    @property
    def articles(self) -> set[str]:
        """A set of product articles, not persist"""
        if not hasattr(self, '_v_articles') or self._v_articles is None:
            self._v_articles: set[str] = set()
        return self._v_articles
