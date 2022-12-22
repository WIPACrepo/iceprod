import importlib
from pprint import pprint
import os
import pkgutil
import pathlib

path = str(pathlib.Path(__file__).parent / 'handlers')
for _, name, _ in pkgutil.iter_modules([path]):
    ret = importlib.import_module(f'iceprod.rest.handlers.{name}')
    pprint(dir(ret))
