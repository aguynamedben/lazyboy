# -*- coding: utf-8 -*-
#
# Prophecy: Exceptions
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

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
