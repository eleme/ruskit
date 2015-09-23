#! /usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name="ruskit",
    version="0.0.1",
    description="Redis cluster administration toolkit",
    packages=find_packages(),
    entry_points={
        "console_scripts": ["ruskit = ruskit.cmds:main"]
    },
    install_requires=[
        "hiredis",
        "redis"
    ],
)
