# -*- coding: utf-8 -*-
#
# A new Python file
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

import time
import uuid

from lazyboy.base import CassandraBase
from lazyboy.columnfamily import *

class SuperColumnFamily(ColumnFamily):
    _key = {}

    def _gen_pk(self, key=None, superkey=None):
        key = key or self._gen_uuid()
        superkey = superkey or self._gen_uuid()
        return PrimaryKey(**dict(self._key.items() +
                                 [['key', key], ['superkey', superkey]]))

    def load(self, key, superkey, cols = None):
        """Load this ColumnFamily from primary key"""
        self._clean()
        self.pk = self._gen_pk(key, superkey)
        self._original = cols or []
        self.revert()
        return self

    def _marshal(self):
        """Return columns to be saved."""
        changed = [self._columns[k] for k in self._modified.keys() \
                       if self._columns.has_key(k) \
                   and self._columns[k].value != None]
        return {'deleted': [self._columns[k] for k in self._deleted.keys()],
                'changed': cassandra.SuperColumn(self.pk.superkey,
                                                   changed)}

    def save(self):
        client = self._get_cas()
        changes = self._marshal()

        # Delete items
        [client.remove(self.pk.table, self.pk.key,
                       cassandra.ColumnPathOrParent(self.pk.family, None, c.name),
                       time.time(), 0) for c in changes['deleted']]

        # Update items
        if changes['changed'].columns:
            scol = changes['changed']
            client.batch_insert_superColumn(
                self.pk.table,
                cassandra.BatchMutationSuper(
                    self.pk.key, {self.pk.supercol: [scol]}), 0)
        return self
