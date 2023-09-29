"""
Helper functions
"""

import datetime
import time
import random
import requests
import simplejson
from urllib3.exceptions import HTTPError


def _json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f'type {type(obj)} is not serializable')


def json_dumps(obj) -> str:
    return simplejson.dumps(obj, indent=True, ensure_ascii=False, use_decimal=True, default=_json_serial)


def http_get(url: str, retries: int = 5, random_retry_pause: float = 0.5, **kwargs):
    """Perform HTTP-get request, retry several times on network errors"""
    while True:
        try:
            resp = requests.get(url, **kwargs)
            if resp.status_code != 200:
                raise HTTPError(f'status_code={resp.status_code}, reason: {resp.reason}')

            return resp

        except (HTTPError, IOError, TimeoutError, ConnectionResetError):
            # also catches all inherited types, including:
            # - ConnectionError is RequestException is IOError
            # - MaxRetryError is RequestError is PoolError is HTTPError
            # - NewConnectionError is HTTPError
            # - ProtocolError is HTTPError
            if retries <= 0:
                raise

        if random_retry_pause > 0:
            time.sleep(random.uniform(random_retry_pause/2.0, random_retry_pause))

        retries -= 1


def update_object(obj: object, **kwargs) -> dict:
    """
    Updates the object attributes.
    If all new values are equal to existing ones, do not touch the object.
    If any of the given attribute values differ, calls setattr on all
    attributes, even if some of them have the required values.

    @param obj: object
    @param kwargs: new attribute values
    @return: dictionary of changed fields with old values
    """
    unknown_attributes = [x for x in kwargs if not hasattr(obj, x)]
    if unknown_attributes:
        raise ValueError(f'object {obj.__class__} does not have attributes: {", ".join(unknown_attributes)}')

    changed_attributes = [x for x in kwargs if getattr(obj, x) != kwargs[x]]
    if not changed_attributes:
        return {}

    old_fields = {}
    for attr_name, new_value in kwargs.items():
        old_value = getattr(obj, attr_name)
        setattr(obj, attr_name, new_value)
        if old_value != new_value:
            old_fields[attr_name] = old_value

    return old_fields