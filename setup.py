#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='tap-kafka',
      version='9.1.0',
      description='Singer.io tap for extracting data from a Kafka topic',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='TransferWise',
      maintainer='mrmoses',
      url='https://github.com/mrmoses/tap-kafka',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      install_requires=[
          'singer-python==6.*',
          'orjson==3.11.*',
          'dpath==2.2.*',
          'confluent-kafka[protobuf]==2.13.*',
          'grpcio-tools==1.76.*'
      ],
      extras_require={
          'test': [
              'pytest==9.0.*',
              'pylint==4.0.*',
              'pytest-cov==7.0.*'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-kafka=tap_kafka:main
      ''',
      packages=['tap_kafka', 'tap_kafka.serialization']
)
