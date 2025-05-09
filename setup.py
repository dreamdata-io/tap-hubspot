#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-hubspot",
    version="2.6.4",
    description="Singer.io tap for extracting data from the HubSpot API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_hubspot"],
    install_requires=[
        "singer-python>=5.1.1, <5.9",
        "requests==2.22.0",
        "backoff>=1.3.2, <2",
        "ratelimit==2.2.1",
        "pydantic==1.8.2",
    ],
    entry_points="""
          [console_scripts]
          tap-hubspot=tap_hubspot:main
      """,
    packages=["tap_hubspot"],
    include_package_data=True,
)
