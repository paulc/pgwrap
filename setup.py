#!/usr/bin/env python

try:
    from setuptools import setup, Command
except ImportError:
    from distutils.core import Command,setup

version = "0.3"
description = 'Simple PostgreSQL database wrapper - provides wrapper over psycopg2 supporting a Python API for common sql functions'
long_description = file("README").read()

class GenerateReadme(Command):
    description = "Generates README file from long_description"
    user_options = []
    def initialize_options(self): pass
    def finalize_options(self): pass
    def run(self):
        import pgwrap
        long_description = pgwrap.description
        open("README","w").write(long_description)

setup(name='pgwrap',
      version = version,
      description = description,
      long_description = long_description,
      author = 'Paul Chakravarti',
      author_email = 'paul.chakravarti@gmail.com',
      url = 'https://github.com/paulchakravarti/pgwrap',
      cmdclass = { 'readme' : GenerateReadme },
      packages = ['pgwrap'],
      install_requires = ['psycopg2'],
      license = 'BSD',
      classifiers = [ "Topic :: Database" ]
     )
