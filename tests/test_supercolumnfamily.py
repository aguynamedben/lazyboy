# -*- coding: utf-8 -*-
#
# A new Python file
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

import unittest

import test_columnfamily

from cassandra import *

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

        scf.load('eggs', 'bacon', [Column(k, data[k]) for k in data])

        for k in data:
            self.assert_(scf[k] == data[k])

    def test_save(self):
        pass

if __name__ == '__main__':
    unittest.main()
