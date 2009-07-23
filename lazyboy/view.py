# -*- coding: utf-8 -*-
#
# A new Python file
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

import time
import datetime
from md5 import md5

from lazyboy.columnfamily import *

class View(CassandraBase):
    """A view"""
    family = ColumnFamily

    def __init__(self, start='', stop='', offset=0, limit=100):
        super(View, self).__init__()
        self.pk = self._gen_pk()
        self.start, self.stop = start, stop
        self.offset, self.limit = offset, limit

    def view_keys(self, start=''):
        "Return a sequence of keys representing the partitions of this view."
        return ()

    def current_key(self):
        """Return the current key to append objects to."""
        k = self.view_keys()
        try:
            return k[0]
        except TypeError:
            key = k.next()
            k.close()
            return key
        raise Exception("I don't know how to cope with these keys.")

    def _iter_partition_keys(self, partition_key):
        """Return keys in one partition of the view."""
        client = self._get_cas()
        last_col, offset, limit = '', 0, 100
        while True:
            cols = client.get_slice(self.pk.table, partition_key,
                                    cassandra.ColumnParent(self.pk.family) ,
                                    last_col, '', True, limit)
            if len(cols) == 0: raise StopIteration()
            for col in cols: yield col.value
            offset, last_col = 1, col.name

            if len(cols) < limit: raise StopIteration()

    def _iter_keys(self):
        """Iterate over object keys for a given view key"""
        for partk in self.view_keys():
            for key in self._iter_partition_keys(partk):
                yield key

        raise StopIteration()

    def __iter__(self):
        """Iterate over all objects in this view."""
        return (self.family().load(key) for key in self._iter_keys())

    def _iter_time(self, start=None, **kwargs):
        day = start or datetime.datetime.today()
        intv = datetime.timedelta(**kwargs)
        while day.year >= 1900:
            yield day.strftime('%Y%m%d')
            day = day - intv

    def _iter_days(self, start = None):
        return self._iter_time(start, days=1)

    def __getitem__(self, item):
        # Fast-forward
        i = 0
        while i < item:
            self.__iter__().next()
            i += 1
        return self.__iter__().next()

    def append(self, column):
        ts = time.time()
        # This is a workaround, since we can't use `:' in column names yet.
        colname = md5(column.pk.key).hexdigest() + '.' + str(ts)
        path = cassandra.ColumnPath(self.pk.family, None, colname)
        self._get_cas().insert(self.pk.table, self.current_key(),
                               path, column.pk.key, ts, 0)
