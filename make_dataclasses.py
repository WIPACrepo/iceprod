# make dataclasses.js from dataclasses.py
import inspect
import json
import os
import sys
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
