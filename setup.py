import os
import sys

from setuptools import find_packages, setup

# Extract version information from Omniduct _version.py
version_info = {}
with open('omniduct/_version.py') as version_file:
    exec(version_file.read(), version_info)

setup(
    # Application name:
    name="omniduct",
    url="https://github.com/airbnb/omniduct",
    description="A toolkit providing a uniform interface for connecting to and extracting data from a wide variety of (potentially remote) data stores (including HDFS, Hive, Presto, MySQL, etc).",

    # Version number:
    version=version_info['__version__'],

    # Application author details:
    author=version_info['__author__'],
    author_email=version_info['__author_email__'],

    # Package details
    packages=find_packages(),

    # Dependencies
    install_requires=version_info['__dependencies__'],
    extras_require=version_info['__optional_dependencies__']
)
