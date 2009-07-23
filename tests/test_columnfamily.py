# -*- coding: utf-8 -*-
#
# ColumnFamily unit tests
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

import time
import uuid
import random
import unittest

from cassandra.ttypes import Column, ColumnParent, BatchMutation

from lazyboy.connection import Client
from lazyboy.columnfamily import ColumnFamily
from lazyboy.exceptions import ErrorMissingField

from test_base import CassandraBaseTest


_last_cols = []
_mutations = []
class MockClient(Client):
    """A mock Cassandra client which returns canned responses"""
    def __init__(self):
        pass

    def get_slice(self, *args, **kwargs):
        [_last_cols.pop() for i in range(len(_last_cols))]
        cols = []
        for i in range(random.randrange(1, 15)):
            cols.append(Column(name=uuid.uuid4(),
                               value=uuid.uuid4(),
                               timestamp=time.time()))
        _last_cols.extend(cols)
        return cols

    def batch_insert(self, table, batch_mutation, block_for):
        _mutations.append(batch_mutation)
        return True


class ColumnFamilyTest(CassandraBaseTest):
    class ColumnFamily(ColumnFamily):
        _key = {'table': 'eggs',
                'family': 'bacon'}
        _required = ('eggs',)

    def __init__(self, *args, **kwargs):
        super(ColumnFamilyTest, self).__init__(*args, **kwargs)
        self.class_ = self.ColumnFamily


    def test_init(self):
        self.object = self._get_object({'id': 'eggs', 'title': 'bacon'})
        self.assert_(self.object['id'] == 'eggs')
        self.assert_(self.object['title'] == 'bacon')

    def test_valid(self):
        self.assert_(not self.object.valid())
        self.object['eggs'] = 'sausage'
        self.assert_(self.object.valid())

    def test_missing(self):
        self.object._clean()
        self.assert_(self.object.missing() == self.object._required)
        self.object['eggs'] = 'sausage'
        self.assert_(self.object.missing() == ())

    def test_clean(self):
        data = {'id': 'eggs', 'title': 'bacon'}
        self.object = self._get_object(data)
        for k in data:
            self.assert_(k in self.object, "Key %s was not set?!" % (k,))
            self.assert_(self.object[k] == data[k])

        self.object._clean()
        for k in data:
            self.assert_(not k in self.object)

    def test_update(self):
        data = {'id': 'eggs', 'title': 'bacon'}
        self.object.update(data)
        for k in data:
            self.assert_(self.object[k] == data[k])

        self.object._clean()
        self.object.update(data.items())
        for k in data:
            self.assert_(self.object[k] == data[k])

        self.object._clean()
        self.object.update(**data)
        for k in data:
            self.assert_(self.object[k] == data[k])

    def test_setitem_getitem(self):
        data = {'id': 'eggs', 'title': 'bacon'}
        for k in data:
            self.object[k] = data[k]
            self.assert_(self.object[k] == data[k],
                         "Data not set in ColumnFamily")
            self.assert_(k in self.object._columns,
                         "Data not set in ColumnFamily.columns")
            self.assert_(self.object._columns[k].__class__ is Column,
                         "ColumnFamily._columns[%s] is %s, not Column" % \
                             (type(self.object._columns[k]), k))
            self.assert_(self.object._columns[k].value == data[k],
                         "Value mismatch in Column, got `%s', expected `%s'" \
                             % (self.object._columns[k].value, data[k]))
            now = time.time()
            self.assert_(self.object._columns[k].timestamp <= now,
                         "Expected timestamp <= %s, got %s" \
                             % (now, self.object._columns[k].timestamp))

            self.assert_(k not in self.object._deleted,
                         "Key was marked as deleted.")
            self.assert_(k in self.object._modified, "Key not in modified list")

        self.object._original = self.object._columns.values()
        col = self.object._original[0]
        self.object[col.name] = col.value

    def test_delitem(self):
        data = {'id': 'eggs', 'title': 'bacon'}
        for k in data:
            self.object[k] = data[k]
            self.assert_(self.object[k] == data[k],
                         "Data not set in ColumnFamily")
            self.assert_(k not in self.object._deleted,
                         "Key was marked as deleted.")
            self.assert_(k in self.object._modified,
                         "Key not in modified list")
            del self.object[k]
            self.assert_(k not in self.object, "Key was not deleted.")
            self.assert_(k in self.object._deleted,
                         "Key was not marked as deleted.")
            self.assert_(k not in self.object._modified,
                         "Deleted key in modified list")
            self.assert_(k not in self.object._columns,
                         "Column was not deleted.")

    def get_mock_cassandra(self):
        """Return a mock cassandra instance"""
        mock = None
        if not mock: mock = MockClient()
        return mock

    def test_load(self):
        self.object._get_cas = self.get_mock_cassandra
        self.object.load('eggs')
        self.assert_(self.object.pk.key == 'eggs')
        self.assert_(self.object._original == _last_cols)

        for col in _last_cols:
            self.assert_(self.object[col.name] == col.value)
            self.assert_(self.object._columns[col.name] == col)

    def test_save(self):
        self.assertRaises(ErrorMissingField, self.object.save)
        data = {'eggs': 1, 'bacon': 2, 'sausage': 3}
        self.object.update(data)
        self.object._get_cas = self.get_mock_cassandra
        del self.object['bacon']
        # FIXME – This doesn't really work, in the sense that
        # self.fail() is never called, but it still triggers an error
        # which makes the test fail, due to incorrect arity in the
        # arguments to the lambda.
        MockClient.remove = lambda self: self.fail("Nothing should get removed.")

        res = self.object.save()
        self.assert_(res == self.object, "Self not returned from ColumnFamily.save")
        mutation = _mutations[len(_mutations) - 1]
        self.assert_(mutation.__class__ == BatchMutation,
                     "Mutation class is %s, not BatchMutation." % \
                         (mutation.__class__,))

        self.assert_(mutation.key == self.object.pk.key,
                     "Mutation key is %s, not PK key %s." \
                         % (mutation.key, self.object.pk.key))
        self.assert_(self.object.pk.family in mutation.cfmap,
                     "PK family %s not in mutation cfmap" % \
                         (self.object.pk.family,))

        for col in mutation.cfmap[self.object.pk.family]:
            self.assert_(col.__class__ == Column,
                         "Column class isn't Column")
            self.assert_(col.name in data,
                         "Column %s wasn't set from update()" % \
                             (col.name))
            self.assert_(data[col.name] == col.value,
                         "Value of column %s is wrong, %s ≠ %s" % \
                             (col.name, data[col.name], col.value))
            self.assert_(col == self.object._columns[col.name],
                         "Column from cf._columns wasn't used in mutation_t")

    def test_revert(self):
        data = {'id': 'eggs', 'title': 'bacon'}
        for k in data:
            self.object._original.append(Column(name=k,
                                                value=data[k]))

        self.object.revert()

        for k in data:
            self.assert_(self.object[k] == data[k])

    def test_is_modified(self):
        data = {'id': 'eggs', 'title': 'bacon'}

        self.assert_(not self.object.is_modified(),
                     "Untouched instance is marked modified.")

        self.object.update(data)
        self.assert_(self.object.is_modified(),
                     "Altered instance is not modified.")


# class ImmutableColumnFamilyTest(ColumnFamilyTest):
#     class ImmutableColumnFamily(ImmutableColumnFamily, ColumnFamilyTest.class_):
#         _immutable = {'foo': 'xyz'}

#     def __init__(self, *args, **kwargs):
#         self.class_ = self.ImmutableColumnFamily
#         super(ColumnFamilyTest, self).__init__(*args, **kwargs)

#     def test_immutability(self):
#         try:
#             self.object['foo'] = 'bar'
#             self.fail("ErrorInvalidField not raised")
#         except ErrorInvalidField:
#             pass

#         try:
#             del self.object['foo']
#             self.fail("ErrorInvalidField not raised")
#         except ErrorInvalidField:
#             pass

#         self.assertRaises(ErrorInvalidField, self.object.update,
#                           {'foo': 'bar'})


if __name__ == '__main__':
    unittest.main()
