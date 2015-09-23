#! /usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


setup(
    name="ruskit",
    version="0.0.1",
    description="Redis cluster administration toolkit",
    long_description=open("README.md").read(),
    url="https://github.com/eleme/ruskit",
    author="maralla",
    author_email="imaralla@icloud.com",
    license="MIT",
    keyswords="redis cluster administration",
    packages=find_packages(),
    entry_points={
        "console_scripts": ["ruskit = ruskit.cmds:main"]
    },
    install_requires=[
        "hiredis",
        "redis"
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
    ]
)
