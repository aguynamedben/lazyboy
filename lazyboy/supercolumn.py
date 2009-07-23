# -*- coding: utf-8 -*-
#
# A new Python file
#
# © 2009 Digg, Inc All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

import time

from lazyboy.columnfamily import *
from lazyboy.base import CassandraBase

class SuperColumn(CassandraBase, dict):
    name = ""
    family = ColumnFamily
    __cache = {}

    def __init__(self):
        super(SuperColumn, self).__init__()
        self.pk = self._gen_pk()

    def load(self):
        return self

    def load_all(self):
        """Load all SuperColumnFamilies in this SuperColumn"""
        for scol in self._iter_columns('', chunk_size=2**30):
            super(SuperColumn, self).__setitem__(
                scol.name, self._instantiate(scol.name, scol.columns))
        return self

    def __setitem__(self, item, value):
        raise ErrorNotSupported("This operation is unsupported")

    def _load_one(self, superkey):
        """Load and return an instance of the SCF with key superkey."""
        client = self._get_cas()
        if not (self.pk.colspec() + ':' + superkey) in self.__class__.__cache:
            scol = client.get_superColumn(
                self.pk.table, self.pk.key,
                self.name + ':' + superkey)
            self.__class__.__cache[self.pk.colspec() + ':' + superkey] = scol
        else:
            scol = self.__class__.__cache[self.pk.colspec() + ':' + superkey]

        return self._instantiate(superkey, scol.columns)

    def _instantiate(self, superkey, columns):
        """Return an instance of a SuperColumnFamily for this SuperColumn."""
        scf = self.family().load(self.pk.key, superkey, columns)
        scf.pk = self.pk.clone(supercol=self.name, superkey=superkey)
        return scf

    def __getitem__(self, superkey):
        """Return a lazy-loaded SuperColumnFamily."""
        if not superkey in self:
            scf = self._load_one(superkey)
            super(SuperColumn, self).__setitem__(superkey, scf)

        return super(SuperColumn, self).__getitem__(superkey)

    def __len_db__(self):
        """Return the number of SuperColumnFamilies in Cassandra for this SC."""
        return self._get_cas().get_column_count(
            self.pk.table, self.pk.key, cassandra.ColumnParent(self.name))

    def __len_loaded__(self):
        """Return the number of items in this instance.

        This may be less than the number of SuperColumnFamilies
        available to load."""
        return super(SuperColumn, self).__len__()

    def _iter_columns(self, start="", limit=None, chunk_size=10):
        client = self._get_cas()
        returned = 0
        while True:
            fudge = int(bool(start))
            scols = client.get_slice_super(self.pk.table, self.pk.key,
                                           self.name, start, '',
                                           True, 0, chunk_size + fudge)
            if not scols: raise StopIteration()

            for scol in scols[fudge:]:
                returned += 1
                self.__class__.__cache[self.pk.colspec() + ':' + scol.name] = scol
                yield scol
                if returned >= limit:
                   raise StopIteration()
            start = scol.name

            if len(scols) < chunk_size: raise StopIteration()

    def iterkeys(self, *args, **kwargs):
        return (scol.name for scol in self._iter_columns(*args, **kwargs))

    def itervalues(self, *args, **kwargs):
        return (self[scol.name] for scol in self._iter_columns(*args, **kwargs))

    def iteritems(self, *args, **kwargs):
        return ((scol.name, self[scol.name]) \
                    for scol in self._iter_columns(*args, **kwargs))


    def __iter__(self):
        for scol in self._iter_columns():
            if scol.name in self:
                scf = self[scol.name]
            else:
                scf = self._instantiate(scol.name, scol.columns)
                super(SuperColumn, self).__setitem__(scol.name, scf)
            yield scf

    def append(self, column_family):
        assert hasattr(self, 'pk')
        assert hasattr(column_family, 'pk')
        assert hasattr(column_family.pk, 'supercol')
        assert hasattr(column_family.pk, 'superkey')

        column_family.pk = self.pk.clone(
            supercol=self.name, superkey=column_family.pk.superkey)

        res = super(SuperColumn, self).__setitem__(column_family.pk.superkey,
                                                   column_family)
        assert column_family.pk.superkey in self
        return res

    def valid(self):
        return all([cf.valid() for cf in self.values()])

    def missing(self):
        return dict([[k, v.missing()] \
                         for k,v in self.items() if not v.valid()])

    def save(self):
        client = self._get_cas()
        mutation = cassandra.BatchMutationSuper(
            self.pk.key, dict(((k.pk.supercol, [])) for k in self.values()))

        for col in self.values():
            if col.is_modified():
                changes = col._marshal()
                if changes['changed']:
                    mutation.cfmap[col.pk.supercol].append(changes['changed'])

                if changes['deleted']:
                    [client.remove(
                            col.pk.table, col.pk.key,
                            cassandra.ColumnPathOrParent(col.pk.family, self.name,
                                                         c.name),
                            time.time(), 0) for c in changes['deleted']]

        if mutation.cfmap:
            client.batch_insert_super_Column(self.pk.table, mutation, 0)
        return self
