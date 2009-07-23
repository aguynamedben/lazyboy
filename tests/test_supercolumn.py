# -*- coding: utf-8 -*-
#
# A new Python file
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

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
        self.assertRaises(ErrorNotSupported, self.object.__setitem__,
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
