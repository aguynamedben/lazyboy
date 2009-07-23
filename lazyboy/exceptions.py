# -*- coding: utf-8 -*-
#
# Lazyboy: Exceptions
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

class ErrorNotSupported(Exception):
    pass


class ErrorMissingField(Exception):
    pass


class ErrorInvalidField(Exception):
    pass


class ErrorIncompleteKey(Exception):
    pass


class ErrorUnknownTable(Exception):
    pass

class ErrorCassandraClientNotFound(Exception):
    pass
