#!/usr/bin/env python
"""Setup."""

import glob

from setuptools import setup  # type: ignore

setup(
    scripts=glob.glob("bin/*"),
)
