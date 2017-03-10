import os
import sys

from setuptools import find_packages, setup

# Extract version information from Omniduct _version.py
version_info = {}
with open('omniduct/_version.py') as version_file:
    exec(version_file.read(), version_info)

setup(
    # Package metadata
    name="omniduct",
    version=version_info['__version__'],
    author=version_info['__author__'],
    author_email=version_info['__author_email__'],
    url="https://github.com/airbnb/omniduct",
    description="A toolkit providing a uniform interface for connecting to and extracting data from a wide variety of (potentially remote) data stores (including HDFS, Hive, Presto, MySQL, etc).",
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5'
    ],

    # Package details
    packages=find_packages(),

    # Dependencies
    install_requires=version_info['__dependencies__'],
    extras_require=version_info['__optional_dependencies__']
)
