#!/usr/bin/env python
#from distutils.core import setup
from setuptools import setup, find_packages

setup(
    name="mongate",
    version="2.0.1",
    description="A client library for Sleepy Mongoose that provides the same interface as Pymongo. With support for batch operations.",
    author="Benjamin Coe",
    author_email="bencoe@gmail.com",
    url="https://github.com/bcoe/mongate",
    packages = find_packages(),
    install_requires = ['pymongo', 'httplib2', 'simplejson'],
    tests_require=['nose', 'coverage']
)
