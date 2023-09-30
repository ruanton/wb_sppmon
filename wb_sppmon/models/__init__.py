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
    def __init__(self):
        self._article_to_product = None
        self._id_to_category = None

    @property
    def article_to_product(self) -> dict[str, wb.Product]:
        """Article to product OOBTree"""
        if not hasattr(self, '_article_to_product') or self._article_to_product is None:
            self._article_to_product = OOBTree()
        return self._article_to_product

    @property
    def id_to_category(self) -> dict[int, wb.Category]:
        """ID to product category OOBTree"""
        if not hasattr(self, '_id_to_category') or self._id_to_category is None:
            self._id_to_category = IOBTree()
        return self._id_to_category


def get_app_root(conn: ZODB.Connection.Connection) -> AppRoot:
    """
    Get the AppRoot persistent object. Creates a new one, if it does not already exist.
    When created, starts and commits a transaction.
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
