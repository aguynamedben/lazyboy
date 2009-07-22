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

import digg.storage.object
import digg.storage.cassandra as cassandra
from digg.storage.object import *
from digg.storage.object import _CassandraBase


_last_cols = []
_mutations = []
class MockClient(cassandra.Client):
    """A mock Cassandra client which returns canned responses"""
    def __init__(self):
        pass

    def get_slice(self, *args, **kwargs):
        [_last_cols.pop() for i in range(len(_last_cols))]
        cols = []
        for i in range(random.randrange(1, 15)):
            cols.append(cassandra.Column(name=uuid.uuid4(),
                                           value=uuid.uuid4(),
                                           timestamp=time.time()))
        _last_cols.extend(cols)
        return cols

    def batch_insert(self, table, batch_mutation, block_for):
        _mutations.append(batch_mutation)
        return True


class CassandraBaseTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(CassandraBaseTest, self).__init__(*args, **kwargs)
        self.class_ =  _CassandraBase

    def _get_object(self, *args, **kwargs):
        return self.class_(*args, **kwargs)

    def setUp(self):
        self.__cassandra = digg.storage.object.cassandra
        digg.storage.object.cassandra.getClient = lambda table: "Test"
        self.object = self._get_object()

    def tearDown(self):
        digg.storage.object.cassandra = self.__cassandra
        del self.object

    def test_init(self):
        self.assert_(self.object._clients == {})

    def test_get_cas(self):
        _pk = None
        if hasattr(self.object, 'pk'):
            _pk = self.object.pk
            del self.object.pk
        self.assertRaises(UnknownTableException, self.object._get_cas)
        if _pk:
            self.object.pk = _pk

        # Get with explicit tale
        self.assert_(self.object._get_cas('foo') == "Test")

        # Implicit based on pk
        self.object.pk = PrimaryKey(table='eggs', key='sausage')
        self.assert_(self.object._get_cas('foo') == "Test")

    def test_gen_pk(self):
        key = {'table': 'eggs', 'family': 'bacon'}
        self.object._key = key
        pk = self.object._gen_pk()
        for k in key:
            self.assert_(hasattr(pk, k))
            self.assert_(getattr(pk, k) == key[k])

        pk = self.object._gen_pk('test123')
        self.assert_(pk.key == 'test123')
        for k in key:
            self.assert_(hasattr(pk, k))
            self.assert_(getattr(pk, k) == key[k])

        self.object._key = {}


    def test_gen_uuid(self):
        self.assert_(type(self.object._gen_uuid()) == str)
        self.assert_(self.object._gen_uuid() != self.object._gen_uuid(),
                     "Unique IDs aren't very unique.")


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
            self.assert_(self.object._columns[k].__class__ is cassandra.Column,
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
        self.assertRaises(MissingFieldException, self.object.save)
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
        self.assert_(mutation.__class__ == cassandra.BatchMutation,
                     "Mutation class is %s, not BatchMutation." % \
                         (mutation.__class__,))

        self.assert_(mutation.key == self.object.pk.key,
                     "Mutation key is %s, not PK key %s." \
                         % (mutation.key, self.object.pk.key))
        self.assert_(self.object.pk.family in mutation.cfmap,
                     "PK family %s not in mutation cfmap" % \
                         (self.object.pk.family,))

        for col in mutation.cfmap[self.object.pk.family]:
            self.assert_(col.__class__ == cassandra.Column,
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
            self.object._original.append(cassandra.Column(name=k,
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


class SuperColumnFamilyTest(ColumnFamilyTest):
    class SuperColumnFamily(SuperColumnFamily):
        _key = {'table': 'eggs',
                'supercol': 'bacon'}

    def __init__(self, *args, **kwargs):
        super(SuperColumnFamilyTest, self).__init__(*args, **kwargs)
        self.SuperColumnFamily._required = ColumnFamilyTest.ColumnFamily._required
        self.class_ = self.SuperColumnFamily

    def test_gen_pk(self):
        key = self.object._key
        pk = self.object._gen_pk()
        self.assert_(hasattr(pk, 'key') and hasattr(pk, 'superkey'))
        for k in key:
            self.assert_(hasattr(pk, k))
            self.assert_(getattr(pk, k) == key[k])

        pk = self.object._gen_pk('eggs123', 'bacon456')
        self.assert_(hasattr(pk, 'key') and hasattr(pk, 'superkey'))
        self.assert_(pk.key == 'eggs123' and pk.superkey == 'bacon456')
        for k in key:
            self.assert_(hasattr(pk, k))
            self.assert_(getattr(pk, k) == key[k])

    def test_load(self):
        scf = self.SuperColumnFamily()
        data = {'eggs': '_eggs',
                'sausage': '_sausage',
                'spam': '_spam'}

        scf.load('eggs', 'bacon', [cassandra.Column(k, data[k]) for k in data])

        for k in data:
            self.assert_(scf[k] == data[k])

    def test_save(self):
        pass


# class ImmutableColumnFamilyTest(ColumnFamilyTest):
#     class ImmutableColumnFamily(ImmutableColumnFamily, ColumnFamilyTest.class_):
#         _immutable = {'foo': 'xyz'}

#     def __init__(self, *args, **kwargs):
#         self.class_ = self.ImmutableColumnFamily
#         super(ColumnFamilyTest, self).__init__(*args, **kwargs)

#     def test_immutability(self):
#         try:
#             self.object['foo'] = 'bar'
#             self.fail("InvalidFieldException not raised")
#         except InvalidFieldException:
#             pass

#         try:
#             del self.object['foo']
#             self.fail("InvalidFieldException not raised")
#         except InvalidFieldException:
#             pass

#         self.assertRaises(InvalidFieldException, self.object.update,
#                           {'foo': 'bar'})


class SuperColumnTest(CassandraBaseTest):
    class SuperColumn(SuperColumn):
        _key = {'table': 'eggs', 'key': 'bacon'}
        name = "spam"
        family = SuperColumnFamilyTest.SuperColumnFamily


    def _get_object(self, *args, **kwargs):
        return super(SuperColumnTest, self)._get_object(*args, **kwargs)

    def __init__(self, *args, **kwargs):
        super(SuperColumnTest, self).__init__(*args, **kwargs)
        self.class_ = self.SuperColumn

    def test_init(self):
        sc = self._get_object()
        self.assert_(hasattr(sc, 'pk'))

    def test_load(self):
        self.assert_(self.object.load() == self.object)

    def test_load_all(self):
        scols = []
        for name in (map(lambda i: 'spam' + str(i), range(6))):
            scols.append(cassandra.SuperColumn(name=name, columns=[]))
        self.object._iter_columns = lambda key, chunk_size: scols
        self.object.load_all()

        for scol in scols:
            self.assert_(scol.name in self.object)
            self.assert_(self.object[scol.name].__class__ == self.object.family)
            self.assert_(self.object[scol.name]._original == scol.columns)

    def test_setitem(self):
        self.assertRaises(NotSupportedException, self.object.__setitem__,
                          'foo', {})

    def test_load_one(self):
        mock = MockClient()
        scol = cassandra.SuperColumn(name='spam', columns=[])
        mock.get_superColumn = lambda table, key, keyspec: scol
        self.object._get_cas = lambda: mock
        scf = self.object._load_one(scol.name)
        self.assert_(scf.pk.superkey == scol.name)
        self.assert_(scf._original == scol.columns)
        self.assert_(scf.__class__ == self.object.family)

    def test_instantiate(self):
        cols = [cassandra.Column(
                name='eggs', value='bacon', timestamp='1234')]
        scf = self.object._instantiate('spamspamspam', cols)
        self.assert_(scf.pk.superkey == 'spamspamspam',
                     "New object got wrong superkey")
        self.assert_(scf.__class__ == self.object.family,
                     "Got instance of wrong class")
        self.assert_(scf.pk.supercol == self.object.name)
        self.assert_(scf.pk.table == self.object.pk.table)
        self.assert_(scf.pk.key == self.object.pk.key)
        self.assert_(scf._original == cols)

    def test_getitem(self):
        scf = self.object.family()
        self.object.append(scf)
        self.assert_(self.object[scf.pk.superkey] == scf)

        scf_id = uuid.uuid4().hex

        self.object._load_one = lambda superkey: scf_id
        self.assert_(self.object['spam'] == scf_id)

    def test_len_db(self):
        rand_len = random.randint(0, 1000)
        class Fake(object):
            def get_column_count(*args, **kwargs):
                return rand_len

        self.object._get_cas = lambda: Fake()
        self.assert_(self.object.__len_db__() == rand_len)

    def test_len_loaded(self):
        self.assert_(self.object.__len_loaded__() == 0)
        for i in range(1, 5):
            self.object.append(self.class_.family())
            self.assert_(self.object.__len_loaded__() == i)

    def test_iter_columns(self):
        mock = MockClient()
        mock.get_slice_super = lambda table, key, colspec, sort, size: False
        self.object._get_cas = lambda: mock

        it = self.object._iter_columns()

    def test_iterkeys_itervalues(self):
        scols = []
        scmap = {}
        for key in map(lambda i: 'spam' + str(i), range(5)):
            sc = cassandra.SuperColumn(name=key, columns=[])
            scols.append(sc)
            scmap[key] = sc

        gen = (scol for scol in scols)
        self.object._iter_columns = lambda: gen
        for key in self.object.iterkeys():
            self.assert_(key in scmap)

        for val in self.object.itervalues():
            self.assert_(val in scols)
            self.assert_(val.pk.superkey in scmap)

    def test_append(self):
        ncols = 5
        cols = []
        for x in range(ncols):
            cols.append(self.class_.family())

        sc = self._get_object()
        for col in cols:
            sc.append(col)
            self.assert_(col.pk.superkey in sc,
                         "SuperColumn doesn't have SCF %s" % (col.pk.superkey,))

        for col in cols:
            self.assert_(col.pk.superkey in sc,
                         "ColumnFamily wasn't added to SuperColumn")
            self.assert_(sc[col.pk.superkey] == col,
                         "Got the wrong ColumnFamily instance.")

    def test_valid_missing(self):
        sc = self._get_object()
        self.assert_(sc.valid())
        cf = self.class_.family()
        sc.append(cf)

        self.assert_(sc.valid() == cf.valid())

        exp = {}
        if not cf.valid():
            exp = {cf.pk.superkey: cf.missing()}
        self.assert_(sc.missing() == exp)

        if not cf.valid():
            for f in cf._required:
                cf[f] = 'spam'

        self.assert_(cf.valid())
        self.assert_(sc.valid())

        _cf = self.class_.family()
        _cf._required = ('eggs', 'bacon')
        sc.append(_cf)
        self.assert_(not sc.valid())
        self.assert_(sc.missing() == {_cf.pk.superkey: _cf._required})

    def test_save(self):
        pass


if __name__ == '__main__':
    unittest.main()
