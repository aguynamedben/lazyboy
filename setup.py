# -*- coding: utf-8 -*-
#
# Lazyboy: Setup
#
# © 2009 Digg, Inc. All rights reserved.
# Author: Ian Eure <ian@digg.com>
#

from setuptools import setup, find_packages

setup(name="Lazyboy",
      version=0.5,
      description="Object non-relational manager for Cassandra",
      url="http://github.com/digg/lazyboy/tree/master",
      packages=find_packages(),
      include_package_data=True,
      author="Ian Eure",
      author_email="ian@digg.com",
      license="BSD",
      keywords="database cassandra")
