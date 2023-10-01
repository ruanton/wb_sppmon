import datetime
import ZODB.Connection
import persistent
import persistent.mapping
# noinspection PyUnresolvedReferences
from BTrees.OOBTree import OOBTree
# noinspection PyUnresolvedReferences
from BTrees.IOBTree import IOBTree

# local imports
from . import wb
from . import tcm


class CategoriesLastUpdate:
    """Results of product categories last successful update"""
    def __init__(self, fetched_at: datetime.datetime, num_new: int, num_updated: int, num_gone: int):
        self.fetched_at = fetched_at;    """Date/time the fetching started"""
        self.num_new = num_new;          """Number of new categories"""
        self.num_updated = num_updated;  """Number of updated categories"""
        self.num_gone = num_gone;        """Number of disappeared categories"""


class AppRoot(persistent.Persistent):
    """App Root object. Root of all other persistent objects.
    """
    def __init__(self):
        self._article_to_product = None
        self._id_to_category = None
        self._name_to_category = None
        self._categories_last_update = None

    @property
    def article_to_product(self) -> dict[str, wb.Product]:
        """Article to product OOBTree"""
        if not hasattr(self, '_article_to_product') or self._article_to_product is None:
            self._article_to_product = OOBTree()
        return self._article_to_product

    @property
    def id_to_category(self) -> dict[int, wb.Category]:
        """ID to product category IOBTree"""
        if not hasattr(self, '_id_to_category') or self._id_to_category is None:
            self._id_to_category = IOBTree()
        return self._id_to_category

    @property
    def name_to_category(self) -> dict[str, wb.Category | set[wb.Category]]:
        """Category name to object OOBTree"""
        if not hasattr(self, '_name_to_category') or self._name_to_category is None:
            self._name_to_category = OOBTree()
        return self._name_to_category

    @property
    def categories_last_update(self) -> CategoriesLastUpdate | None:
        """Results of categories last update"""
        return self._categories_last_update if hasattr(self, '_categories_last_update') else None

    @categories_last_update.setter
    def categories_last_update(self, value: CategoriesLastUpdate):
        self._categories_last_update = value


def get_app_root(conn: ZODB.Connection.Connection) -> AppRoot:
    """
    Get the AppRoot persistent object. Creates a new one, if it does not already exist.
    Side effect: if the object does not already exist in the database, starts and commits a transaction to create it.
    """
    zodb_root: persistent.mapping.PersistentMapping = conn.root()

    if 'app_root' in zodb_root:
        # get object from database
        app_root: AppRoot = zodb_root['app_root']
    else:
        # create a new object
        with tcm.in_transaction(conn):
            app_root = AppRoot()
            zodb_root['app_root'] = app_root

    return app_root
