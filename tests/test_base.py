# -*- coding: utf-8 -*-
#
# A new Python file
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

import unittest
from lazyboy.base import CassandraBase
import lazyboy.connection
from lazyboy.primarykey import *
from lazyboy.exceptions import ErrorUnknownTable

class CassandraBaseTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(CassandraBaseTest, self).__init__(*args, **kwargs)
        self.class_ =  CassandraBase

    def _get_object(self, *args, **kwargs):
        return self.class_(*args, **kwargs)

    def setUp(self):
        self.__get_pool = lazyboy.connection.get_pool
        lazyboy.connection.get_pool = lambda table: "Test"
        self.object = self._get_object()

    def tearDown(self):
        lazyboy.connection.get_pool = self.__get_pool
        del self.object

    def test_init(self):
        self.assert_(self.object._clients == {})

    def test_get_cas(self):
        _pk = None
        if hasattr(self.object, 'pk'):
            _pk = self.object.pk
            del self.object.pk
        self.assertRaises(ErrorUnknownTable, self.object._get_cas)
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

if __name__ == '__main__':
    unittest.main()
