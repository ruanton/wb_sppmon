import ZODB.Connection
from persistent.mapping import PersistentMapping

# local imports
from .tcm import in_transaction


class AppRootModel(PersistentMapping):
    __parent__ = __name__ = None


def app_root_maker(conn: ZODB.Connection.Connection) -> AppRootModel:
    zodb_root = conn.root()
    if 'app_root' not in zodb_root:
        with in_transaction(conn):
            app_root = AppRootModel()
            zodb_root['app_root'] = app_root

    return zodb_root['app_root']
