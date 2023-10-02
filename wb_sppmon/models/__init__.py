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


class AppRoot(persistent.Persistent):
    """App Root object. Root of all other persistent objects.
    """
    def __init__(self):
        self._article_to_product = None
        self._id_to_category = None
        self._lw_name_to_category = None
        self._lw_seo_to_category = None
        self._categories_last_update = None
        self._lw_name_to_subcategory = None

    @property
    def article_to_product(self) -> dict[str, wb.Product]:
        """OOBTree: product article => product entity"""
        if not hasattr(self, '_article_to_product') or self._article_to_product is None:
            self._article_to_product = OOBTree()
        return self._article_to_product

    @property
    def id_to_category(self) -> dict[int, wb.Category]:
        """IOBTree: product category ID => category entity"""
        if not hasattr(self, '_id_to_category') or self._id_to_category is None:
            self._id_to_category = IOBTree()
        return self._id_to_category

    @property
    def lw_name_to_category(self) -> dict[str, wb.Category | set[wb.Category]]:
        """OOBTree: category lowered name => category or a set of categories"""
        if not hasattr(self, '_lw_name_to_category') or self._lw_name_to_category is None:
            self._lw_name_to_category = OOBTree()
        return self._lw_name_to_category

    @property
    def lw_seo_to_category(self) -> dict[str, wb.Category | set[wb.Category]]:
        """OOBTree: category lowered seo => category or a set of categories"""
        if not hasattr(self, '_lw_seo_to_category') or self._lw_seo_to_category is None:
            self._lw_seo_to_category = OOBTree()
        return self._lw_seo_to_category

    @property
    def categories_last_update(self) -> wb.LastUpdateResult | None:
        """Results of categories last update"""
        return self._categories_last_update if hasattr(self, '_categories_last_update') else None

    @categories_last_update.setter
    def categories_last_update(self, value: wb.LastUpdateResult):
        self._categories_last_update = value

    @property
    def lw_name_to_subcategory(self) -> dict[str, wb.Subcategory | set[wb.Subcategory]]:
        """OOBTree: subcategory lowered name => subcategory or a set of subcategories"""
        if not hasattr(self, '_lw_name_to_subcategory') or self._lw_name_to_subcategory is None:
            self._lw_name_to_subcategory = OOBTree()
        return self._lw_name_to_subcategory


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
