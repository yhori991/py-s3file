#!/usr/bin/env python
from distutils.core import setup
import os

here = os.path.abspath(os.path.dirname(__file__))

# Read the package info from __version__.py
about = {}
with open(os.path.join(here, 's3file', '__version__.py'), 'r', encoding='utf-8') as f:
    exec(f.read(), about)


# Read the long_description from README.md
def make_long_description_from_markdown():
    with open('README.md', encoding='utf-8') as f:
        readme = f.read()
    return readme


def make_requires(path):
    names = [l.rstrip() for l in open(os.path.join(here, path)).readlines() if l[0] != "#"]
    names = [l for l in names if l != '']
    return names


setup(
    name=about['__title__'],
    packages=['s3file'],
    version=about['__version__'],
    install_requires=make_requires('requirements.txt'),
    tests_require=make_requires('requirements-dev.txt'),
    author=about['__author__'],
    author_email=about['__author_email__'],
    url=about['__url__'],
    description=about['__description__'],
    long_description='',
    long_description_content_type='text/markdown',
    keywords='s3, File IO, File I/O',
    license=about['__license__'],
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
