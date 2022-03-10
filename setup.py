#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [ ]

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
    version='0.1.0',
    zip_safe=False,
)
