#!/usr/bin/env python

try:
    from setuptools import setup
    extra = dict(install_requires=[
        'py2neo>=2.0.8',
        'PyYAML>=3.11',
    ],
        include_package_data=True,
    )
except ImportError:
    from distutils.core import setup
    extra = {}


def readme():
    with open("README.md") as f:
        return f.read()


setup(name="twitter-neo4j",
      version="0.0.1",
      description="Loads twitter JSON into Neo4j",
      long_description=readme(),
      author="Kevin Coakley",
      author_email="kcoakley@sdsc.edu",
      scripts=[
          "bin/twitter-neo4j",
      ],
      url="",
      packages=[
          "twitterneo4j",
          "twitterneo4j/multi",
          "twitterneo4j/neo4j",
          "twitterneo4j/tweets",
      ],
      platforms="Posix; MacOS X",
      classifiers=[
          "Programming Language :: Python :: 2",
          "Programming Language :: Python :: 2.7",
      ],
      **extra
      )
