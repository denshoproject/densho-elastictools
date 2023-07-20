#!/usr/bin/env python

import codecs
import os.path
from setuptools import setup, find_packages

def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()

def get_version(rel_path):
    # https://packaging.python.org/en/latest/guides/single-sourcing-package-version/
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")

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
    version=get_version('elastictools/__init__.py'),
    zip_safe=False,
)
