# -*- coding: utf-8 -*-
#
# A new Python file
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

from lazyboy.exceptions import ErrorIncompleteKey

class PrimaryKey(object):
    def __init__(self, table, key, family = None, supercol=None, superkey=None):
        """Construct a PK object from a string representation or keyword args"""
        if (supercol or superkey) and not (supercol and superkey):
            raise ErrorIncompleteKey("You must set both supercol and superkey")
        for (k, v) in vars().items(): setattr(self, k, v)

    def is_super(self):
        """Return bool if this is a PK for CF in a SCF."""
        return (self.supercol or self.superkey) and \
            (self.supercol and self.superkey)

    def colspec(self):
        """Return the column specification needed by Cassandra."""
        out = "%s:" % (self.family,)
        if self.supercol and self.superkey:
            out += "%s:%s" % (self.supercol, self.superkey)
        return out

    def __str__(self):
        """Return the string representation of this PK"""
        keys = ('table', 'family', 'key', 'supercol', 'superkey')
        return str(dict(((key, getattr(self, key)) for key in keys)))

    def __unicode__(self):
        return unicode(str(self))

    def __repr__(self):
        return unicode(self)

    def clone(self, **kwargs):
        """Return a clone of this key with keyword args changed"""
        return DecoratedPrimaryKey(self, **kwargs)


class DecoratedPrimaryKey(PrimaryKey):
    def __init__(self, parent_key, **kwargs):
        self.parent_key = parent_key
        for (k, v) in kwargs.items(): setattr(self, k, v)

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]

        if hasattr(self.parent_key, attr):
            return getattr(self.parent_key, attr)

        raise AttributeError("`%s' object has no attribute `%s'" % \
                                 (self.__class__.__name__, attr))

    def __hasattr__(self, attr):
        return attr in self.__dict__ or hasattr(self.parent_key, attr)
