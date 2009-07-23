# -*- coding: utf-8 -*-
#
# A new Python file
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

import uuid

from lazyboy.exceptions import ErrorUnknownTable
import lazyboy.connection as connection
from lazyboy.primarykey import PrimaryKey

class CassandraBase(object):
    """The base class for all Cassandra-accessing objects."""

    def __init__(self):
        self._clients = {}

    def _get_cas(self, table=None):
        """Return the cassandra client."""
        if not table and (not hasattr(self, 'pk') or \
                              not hasattr(self.pk, 'table')):
            raise ErrorUnknownTable()

        table = table or self.pk.table
        if table not in self._clients:
            self._clients[table] = connection.get_pool(table)

        return self._clients[table]

    def _gen_pk(self, key=None):
        """Generate and return a PrimaryKey with a new UUID."""
        key = key or self._gen_uuid()
        return PrimaryKey(**dict(self._key.items() + [['key', key]]))

    def _gen_uuid(self):
        """Generate a UUID for this object"""
        return uuid.uuid4().hex
