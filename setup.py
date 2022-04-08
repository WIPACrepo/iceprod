#!/usr/bin/env python
"""Setup."""


import glob
import os
import subprocess
import sys

from setuptools import setup


setup(
    scripts=glob.glob("bin/*"),
    url="https://github.com/WIPACrepo/iceprod",
    package_data={
        "iceprod.server": ["data/etc/*", "data/www/*", "data/www_templates/*"]
    }
)
