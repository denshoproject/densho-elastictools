#!/usr/bin/env python

import codecs
import os
import re
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    # intentionally *not* adding an encoding option to open
    return codecs.open(os.path.join(here, *parts), 'r').read()

def find_version(*file_paths):
    #version_file = read(*file_paths)
    #version_match = re.search(r"^VERSION = ['\"]([^'\"]*)['\"]",
    #                          version_file, re.M)
    #if version_match:
    #    return version_match.group(1)
    #raise RuntimeError("Unable to find version string.")
    return read(*file_paths)

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'elasticsearch>=7.0.0,<8.0.0',
    'elasticsearch-dsl>=7.0.0,<8.0.0',
]

test_requirements = ['pytest>=3', ]

setup(
    author="Geoffrey Jost",
    author_email='geoffrey.jost@densho.org',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Tools for using Elasticsearch as a document store and search engine",
    install_requires=requirements,
    license="TBD",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='elastictools',
    name='elastictools',
    packages=find_packages(include=['elastictools', 'elastictools.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/denshoproject/densho-elastictools',
    version = find_version('VERSION'),
    zip_safe=False,
)
