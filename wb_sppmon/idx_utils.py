"""
BTree index utils
"""

import typing

# noinspection PyUnresolvedReferences
from BTrees.OOBTree import OOBTree
# noinspection PyUnresolvedReferences
from BTrees.IOBTree import IOBTree


def idx_update(idx: OOBTree | IOBTree | dict, key: int | str, element: typing.Any):
    """Add an element to the index if it doesn't already exist"""
    if key in idx:
        # ↑ an element or set of elements with this key already exists in the index
        if isinstance(idx[key], set):
            if element not in idx[key]:
                idx[key].add(element)  # ← add element to set
        else:
            if idx[key] != element:
                idx[key] = {idx[key], element}  # ← convert to set and add new element
    else:
        idx[key] = element  # ← add element to the index
