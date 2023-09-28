import logging
import ZODB.Connection
from persistent.mapping import PersistentMapping
# noinspection PyUnresolvedReferences
from BTrees.OOBTree import OOBTree
# noinspection PyUnresolvedReferences
from BTrees.IOBTree import IOBTree

# local imports
from .tcm import in_transaction

log = logging.getLogger(__name__)


class AppRootModel(PersistentMapping):
    __parent__ = __name__ = None


# ZODB root object keys
ZRK_APP_ROOT = 'app_root'
ZRK_ARTICLE_TO_PRODUCT = 'article_to_product'
ZRK_ID_TO_CATEGORY = 'id_to_category'

# Callables to create new ZODB root objects
ZODB_ROOT_OBJECT_MAKERS = {
    ZRK_APP_ROOT: AppRootModel,
    ZRK_ARTICLE_TO_PRODUCT: OOBTree,
    ZRK_ID_TO_CATEGORY: IOBTree,
}


def zodb_root_maker(conn: ZODB.Connection.Connection) -> PersistentMapping:
    """Get ZODB root. Make new objects if any of the required ones do not exist yet.
    """
    zodb_root: PersistentMapping = conn.root()

    if any(x not in zodb_root for x in ZODB_ROOT_OBJECT_MAKERS):
        log.info('initialize database')
        with in_transaction(conn):
            for key, maker in ZODB_ROOT_OBJECT_MAKERS.items():
                if key not in zodb_root:
                    zodb_root[key] = maker()

    return zodb_root


def app_root_maker(conn: ZODB.Connection.Connection) -> AppRootModel:
    """Get App Root object. Make new, if not exists yet.
    """
    zodb_root = zodb_root_maker(conn)
    app_root: AppRootModel = zodb_root[ZRK_APP_ROOT]
    return app_root
