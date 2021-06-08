#!/usr/bin/env python
"""Setup."""


import glob
import os
import subprocess
import sys

from setuptools import setup  # type: ignore[import]

###############################################################################################


# fmt: off
try:
    # make dataclasses.js from dataclasses.py
    import inspect
    import json
    current_path = os.path.abspath(os.path.dirname(__file__))
    sys.path.append(current_path)
    from iceprod.core import dataclasses
    dcs = {}
    names = dataclasses._plurals.copy()
    for name, obj in inspect.getmembers(dataclasses,inspect.isclass):
        if name[0] != '_' and dict in inspect.getmro(obj):
            dcs[name] = obj().output()
            names[name] = obj.plural
    data = {'classes':dcs,'names':names}
    with open(os.path.join(current_path,'iceprod','server','data','www','dataclasses.js'),'w') as f:
        f.write('var dataclasses='+json.dumps(data,separators=(',',':'))+';')
except Exception:
    print('WARN: cannot make dataclasses.js')
# fmt: on


###############################################################################################


subprocess.run(
    "pip install git+https://github.com/WIPACrepo/wipac-dev-tools.git".split(),
    check=True,
)
from wipac_dev_tools import SetupShop  # noqa: E402  # pylint: disable=C0413

shop = SetupShop(
    "iceprod",
    os.path.abspath(os.path.dirname(__file__)),
    ((3, 6), (3, 8)),
    "A set of grid middleware and job tracking tools, developed for the IceCube Collaboration.",
)

setup(
    scripts=glob.glob("bin/*"),
    url="https://github.com/WIPACrepo/iceprod",
    package_data={
        "iceprod.server": ["data/etc/*", "data/www/*", "data/www_templates/*"]
    },
    **shop.get_kwargs(
        subpackages=[
            "core",
            "modules",
            "server",
            "server.rest",
            "server.scheduled_tasks",
            "server.modules",
            "server.plugins",
        ],
        other_classifiers=[
            "Operating System :: POSIX :: Linux",
            "Topic :: System :: Distributed Computing",
            "Programming Language :: Python :: Implementation :: CPython",
        ],
    ),
    zip_safe=False
)
