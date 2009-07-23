# -*- coding: utf-8 -*-
#
# Lazyboy: ColumnFamily
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

import time

from cassandra.ttypes import Column, ColumnParent, BatchMutation

from lazyboy.base import CassandraBase
from lazyboy.exceptions import *

class ColumnFamily(CassandraBase, dict):
    # The template to use for the PK
    _key = {}

    # A tuple of items which must be present for the object to be valid
    _required = ()

    def __init__(self, *args, **kwargs):
        super(ColumnFamily, self).__init__()

        # Initialize
        self._columns, self._original = {}, []
        self._modified, self._deleted = {}, {}

        self.pk = self._gen_pk()
        if args or kwargs:
            self.update(*args, **kwargs)

    def valid(self):
        """Return a boolean indicating whether this is valid or not"""
        return len(self.missing()) == 0

    def missing(self):
        """Return a tuple of required items which are missing"""
        return tuple([f for f in self._required \
                          if f not in self or self[f] == None])

    def _clean(self):
        """Remove every item from the object"""
        map(self.__delitem__, self.keys())
        self._original = []
        self._columns = {}
        self._modified, self._deleted = {}, {}

    def update(self, arg=None, **kwargs):
        """Update the object as with dict.update"""
        if arg:
            if hasattr(arg, 'keys'):
                for k in arg: self[k] = arg[k]
            else:
                for k, v in arg: self[k] = v

        if kwargs:
            for k in kwargs: self[k] = kwargs[k]

    def __setitem__(self, item, value):
        """Set an item, storing it into the _columns backing store."""
        if value.__class__ is unicode:
            value = value.encode('utf-8')
        value = str(value)
        # If this doesn't change anything, don't record it
        if item in self._original and self._original[item].value == value:
            return

        super(ColumnFamily, self).__setitem__(item, value)

        if not item in self._columns:
            self._columns[item] = Column(name=item,
                                                   timestamp=time.time())

        col = self._columns[item]

        if item in self._deleted: del self._deleted[item]

        self._modified[item] = True
        col.value, col.timestamp = value, time.time()

    def __delitem__(self, item):
        super(ColumnFamily, self).__delitem__(item)
        del self._columns[item]
        self._deleted[item] = True
        if item in self._modified: del self._modified[item]

    def load(self, key):
        """Load this ColumnFamily from primary key"""
        self._clean()
        self.pk = self._gen_pk(key)

        self._original = self._get_cas().get_slice(
            self.pk.table, self.pk.key, ColumnParent(self.pk.family),
            '', '', True, 100)
        self.revert()
        return self

    def save(self):
        if not self.valid():
            raise ErrorMissingField("Missing required field(s):",
                                        self.missing())

        client = self._get_cas()
        # Delete items
        deleted = self._deleted.keys()
        [client.remove(self.pk.table, self.pk.key,
                       ColumnPathOrParent(self.pk.family, None, dlt),
                       time.time(), 0) \
             for dlt in deleted if dlt in self._original]

        # Update items
        changed = [self._columns[k] for k in self._modified.keys() \
                       if self._columns.has_key(k) and self._columns[k].value != None]
        if changed:
            client.batch_insert(
                self.pk.table,
                BatchMutation(
                    self.pk.key, {self.pk.family: changed}), 0)
        return self

    def revert(self):
        "Revert changes, restoring to the state we were in when loaded"
        for c in self._original:
            super(ColumnFamily, self).__setitem__(c.name, c.value)
            self._columns[c.name] = c

        self._modified, self._deleted = {}, {}

    def is_modified(self):
        return bool(len(self._modified) + len(self._deleted))


class ImmutableColumnFamily(ColumnFamily):
    """An object representing columns in Cassandra by way of a Python dict."""

    # Tuple of immutable fields and their values
    _immutable = {}

    def __init__(self, *args, **kwargs):
        super(ImmutableColumnFamily, self).__init__(*args, **kwargs)
        for (k, v) in self._immutable.items():
            super(ImmutableColumnFamily, self).__setitem__(k, v)

    def __setitem__(self, attr, value):
        """Set an attribute, unless it is immutable"""
        if attr in self._immutable.keys():
            raise ErrorInvalidField("You may not change the %s field" \
                                            % (attr,))
        super(ImmutableColumnFamily, self).__setitem__(attr, value)

    def __delitem__(self, attr):
        """Delete an attribute, unless it is immutable"""
        if attr in self._immutable.keys():
            raise ErrorInvalidField("You may not change the %s field" \
                                            % (attr,))
        super(ImmutableColumnFamily, self).__delitem__(attr)
