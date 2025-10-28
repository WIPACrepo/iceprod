#!/usr/bin/env python3

import os
from pathlib import Path
import sys


"""
This is a PreCmd script designed to rewrite input file names.
"""

for arg in sys.argv[1:]:
    try:
        name, newname = arg.split('=',1)
    except Exception:
        print('Bad formatting, cannot parse')
        raise
    try:
        if not os.path.exists(name):
            continue
        if '/' in newname:
            Path(newname).parent.mkdir(parents=True, exist_ok=True)
        os.rename(name, newname)
    except Exception:
        print(f'Cannot move inputfile {name} to {newname}', file=sys.stderr)
        raise
