# -*- coding: utf-8 -*-
#
# Lazyboy: PrimaryKey unit tests
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

import unittest
from lazyboy.primarykey import PrimaryKey
from lazyboy.exceptions import ErrorIncompleteKey

class PrimaryKeyTest(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(PrimaryKeyTest, self).__init__(*args, **kwargs)
        self.allowed = ({'table': 'egg', 'key': 'bacon'},
                        {'table': 'egg', 'key': 'bacon', 'family': 'sausage'},
                        {'table': 'egg', 'key': 'bacon', 'family': 'sausage',
                         'supercol': 'spam', 'superkey': 'tomato'})
        self.denied = ({'table': 'egg', 'key': 'bacon', 'supercol': 'sausage'},
                       {'table': 'egg', 'key': 'bacon', 'superkey': 'sausage'})

    def test_init(self):
        for args in self.allowed:
            pk = PrimaryKey(**args)
            for k in args:
                self.assert_(getattr(pk, k) == args[k],
                             "Expected `%s', for key `%s', got `%s'" % \
                                 (args[k], k, getattr(pk, k)))

        for args in self.denied:
            self.assertRaises(ErrorIncompleteKey, PrimaryKey, **args)

    def test_super(self):
        self.assert_(not PrimaryKey(table='eggs', key='sausage').is_super())
        self.assert_(PrimaryKey(table='eggs', key='sausage', supercol='bacon',
                                superkey='spam').is_super())

    def test_colspec(self):
        spec = PrimaryKey(table='eggs', key='spam', family='bacon').colspec()
        self.assert_(spec == 'bacon:')
        spec = PrimaryKey(table='eggs', key='spam', family='bacon',
                          supercol='sausage', superkey='tomato').colspec()
        # FIXME not sure if this is right, it may need a trailing :
        self.assert_(spec == 'bacon:sausage:tomato')


    def test_str(self):
        x = PrimaryKey(table='eggs', key='spam', family='bacon').__str__()
        self.assert_(type(x) is str)

    def test_unicode(self):
        pk = PrimaryKey(table='eggs', key='spam', family='bacon')
        x = pk.__unicode__()
        self.assert_(type(x) is unicode)
        self.assert_(str(x) == str(pk))

    def test_repr(self):
        pk = PrimaryKey(table='eggs',key='spam',family='bacon')
        self.assert_(unicode(pk) == repr(pk))

    def test_clone(self):
        pk = PrimaryKey(table='eggs',key='spam',family='bacon')
        ppkk = pk.clone()
        self.assert_(repr(pk) == repr(ppkk))
        for k in ('table', 'key', 'family'):
            self.assert_(getattr(pk, k) == getattr(ppkk, k))

        # Changes override base keys, but don't change them.
        _pk = pk.clone(key='sausage')
        self.assert_(_pk.key == 'sausage')
        self.assert_(pk.key == 'spam')
        _pk = pk.clone(supercol='sausage', superkey='tomato')
        self.assert_(_pk.supercol == 'sausage' and _pk.superkey == 'tomato')
        self.assertRaises(AttributeError, _pk.__getattr__, 'sopdfj')
        self.assert_(hasattr(pk, 'supercol'))
        self.assert_(hasattr(pk, 'key'))

        # Changes to the base propagate to cloned PKs.
        pk.table = 'beans'
        self.assert_(_pk.table == 'beans')

        __pk = _pk.clone()
        self.assert_(__pk.table == 'beans')
        pk.table = 'tomato'
        self.assert_(__pk.table == 'tomato')



if __name__ == '__main__':
    unittest.main()
