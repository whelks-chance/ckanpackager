from setuptools import setup, find_packages

setup(
  name='ckanpackager',
  version='0.1',
  description='Service to package CKAN data into ZIP files and email the link to the file to users',
  url='http://github.com/NaturalHistoryMuseum/ckanpackager',
  packages=find_packages()
)