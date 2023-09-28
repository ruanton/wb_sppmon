"""
Transaction Context Manager helpers
"""

import ZODB.Connection


class TransactionContextManager(object):
    """PEP 343 context manager"""
    def __init__(self, conn: ZODB.Connection.Connection, note: str = None):
        self.conn = conn
        self.note = note

    def __enter__(self) -> ZODB.Connection.Connection:
        self.tm = tm = self.conn.transaction_manager
        tran = tm.begin()
        if self.note:
            tran.note(self.note)
        return self.conn

    def __exit__(self, typ, val, tb):
        if typ is None:
            self.tm.commit()
        else:
            self.tm.abort()


def in_transaction(conn: ZODB.Connection.Connection, note: str = None) -> TransactionContextManager:
    """
    Execute a block of code as a transaction.
    Starts database transaction. Commits on success __exit__, rollbacks on exception.
    If a note is given, it will be added to the transaction's description.
    The 'in_transaction' returns a context manager that can be used with the ``with`` statement.
    """
    return TransactionContextManager(conn, note)
