"""
Class for storing a description of a failure for reporting to admins
"""

from datetime import datetime, timezone


class Failure:
    def __init__(self, entity_descr: str, message: str | Exception, at: datetime = None):
        """
        Create a failure description object

        @param entity_descr: human-readable descriptor of an entity with a failure
        @param message: description of failure
        @param at: date/time of the failure occurrence
        """
        self.entity_descr = entity_descr
        self.message = str(message)
        self.at = at if at else datetime.now(tz=timezone.utc)
