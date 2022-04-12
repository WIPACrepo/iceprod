#!/bin/sh
unset PYTHONPATH
python3 -m virtualenv -p python3 env
echo "unset PYTHONPATH" >> env/bin/activate
. env/bin/activate
pip install -e .[tests,docs]
