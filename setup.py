#!/usr/bin/env python
"""Setup."""

import glob

from setuptools import setup

setup(
    scripts=glob.glob("bin/*"),
    package_data={
        "iceprod.core": ["data/*"],
        "iceprod.server": ["data/etc/*", "data/www/*", "data/www_templates/*"]
    }
)
