# -*- coding: utf-8 -*-
#
# API for interfacing with Cassandra
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

import datetime
from md5 import md5
import time
import uuid
import copy

import digg.storage.cassandra as cassandra


class NotSupportedException(Exception):
    pass


class MissingFieldException(Exception):
    pass


class InvalidFieldException(Exception):
    pass


class IncompleteKeyException(Exception):
    pass


class UnknownTableException(Exception):
    pass


class _CassandraBase(object):
    """The base class for all Cassandra-accessing objects"""

    def __init__(self):
        self._clients = {}

    def _get_cas(self, table=None):
        """Return the cassandra client."""
        if not table and (not hasattr(self, 'pk') or \
                              not hasattr(self.pk, 'table')):
            raise UnknownTableException()

        table = table or self.pk.table
        if table not in self._clients:
            self._clients[table] = cassandra.getClient(table)

        return self._clients[table]

    def _gen_pk(self, key=None):
        """Generate and return a PrimaryKey with a new UUID."""
        key = key or self._gen_uuid()
        return PrimaryKey(**dict(self._key.items() + [['key', key]]))

    def _gen_uuid(self):
        """Generate a UUID for this object"""
        return uuid.uuid4().hex


class PrimaryKey(object):
    def __init__(self, table, key, family = None, supercol=None, superkey=None):
        """Construct a PK object from a string representation or keyword args"""
        if (supercol or superkey) and not (supercol and superkey):
            raise IncompleteKeyException("You must set both supercol and superkey")
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


class ColumnFamily(_CassandraBase, dict):
    # The template to use for the PK
    _key = {}

    # A tuple of items which must be present for the object to be valid
    _required = ()

    def __init__(self, *args, **kwargs):
        super(ColumnFamily, self).__init__()

        # Initialize
        self._columns, self._original = {}, []
        self._modified, self._deleted = {}, {}

        self.pk = self._gen_pk()
        if args or kwargs:
            self.update(*args, **kwargs)

    def valid(self):
        """Return a boolean indicating whether this is valid or not"""
        return len(self.missing()) == 0

    def missing(self):
        """Return a tuple of required items which are missing"""
        return tuple([f for f in self._required \
                          if f not in self or self[f] == None])

    def _clean(self):
        """Remove every item from the object"""
        map(self.__delitem__, self.keys())
        self._original = []
        self._columns = {}
        self._modified, self._deleted = {}, {}

    def update(self, arg=None, **kwargs):
        """Update the object as with dict.update"""
        if arg:
            if hasattr(arg, 'keys'):
                for k in arg: self[k] = arg[k]
            else:
                for k, v in arg: self[k] = v

        if kwargs:
            for k in kwargs: self[k] = kwargs[k]

    def __setitem__(self, item, value):
        """Set an item, storing it into the _columns backing store."""
        if value.__class__ is unicode:
            value = value.encode('utf-8')
        value = str(value)
        # If this doesn't change anything, don't record it
        if item in self._original and self._original[item].value == value:
            return

        super(ColumnFamily, self).__setitem__(item, value)

        if not item in self._columns:
            self._columns[item] = cassandra.Column(name=item,
                                                   timestamp=time.time())

        col = self._columns[item]

        if item in self._deleted: del self._deleted[item]

        self._modified[item] = True
        col.value, col.timestamp = value, time.time()

    def __delitem__(self, item):
        super(ColumnFamily, self).__delitem__(item)
        del self._columns[item]
        self._deleted[item] = True
        if item in self._modified: del self._modified[item]

    def load(self, key):
        """Load this ColumnFamily from primary key"""
        self._clean()
        self.pk = self._gen_pk(key)

        self._original = self._get_cas().get_slice(
            self.pk.table, self.pk.key, cassandra.ColumnParent(self.pk.family),
            '', '', True, 100)
        self.revert()
        return self

    def save(self):
        if not self.valid():
            raise MissingFieldException("Missing required field(s):",
                                        self.missing())

        client = self._get_cas()
        # Delete items
        deleted = self._deleted.keys()
        [client.remove(self.pk.table, self.pk.key,
                       cassandra.ColumnPathOrParent(self.pk.family, None, dlt),
                       time.time(), 0) \
             for dlt in deleted if dlt in self._original]

        # Update items
        changed = [self._columns[k] for k in self._modified.keys() \
                       if self._columns.has_key(k) and self._columns[k].value != None]
        if changed:
            client.batch_insert(
                self.pk.table,
                cassandra.BatchMutation(
                    self.pk.key, {self.pk.family: changed}), 0)
        return self

    def revert(self):
        "Revert changes, restoring to the state we were in when loaded"
        for c in self._original:
            super(ColumnFamily, self).__setitem__(c.name, c.value)
            self._columns[c.name] = c

        self._modified, self._deleted = {}, {}

    def is_modified(self):
        return bool(len(self._modified) + len(self._deleted))


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


class ImmutableColumnFamily(ColumnFamily):
    """An object representing columns in Cassandra by way of a Python dict."""

    # Tuple of immutable fields and their values
    _immutable = {}

    def __init__(self, *args, **kwargs):
        super(ImmutableColumnFamily, self).__init__(*args, **kwargs)
        for (k, v) in self._immutable.items():
            super(ImmutableColumnFamily, self).__setitem__(k, v)

    def __setitem__(self, attr, value):
        """Set an attribute, unless it is immutable"""
        if attr in self._immutable.keys():
            raise InvalidFieldException("You may not change the %s field" \
                                            % (attr,))
        super(ImmutableColumnFamily, self).__setitem__(attr, value)

    def __delitem__(self, attr):
        """Delete an attribute, unless it is immutable"""
        if attr in self._immutable.keys():
            raise InvalidFieldException("You may not change the %s field" \
                                            % (attr,))
        super(ImmutableColumnFamily, self).__delitem__(attr)


class SuperColumn(_CassandraBase, dict):
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
        raise NotSupportedException("This operation is unsupported")

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


class View(_CassandraBase):
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
