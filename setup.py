#!/usr/bin/env python
"""Setup."""

import glob

from setuptools import setup

setup(
    scripts=glob.glob("bin/*"),
)
