"""
Make graphs (lifecycles, ...)
"""
from __future__ import absolute_import, division, print_function

import os
import sys
import json
from collections import OrderedDict

try:
    import pygraphviz as pgv
except ImportError:
    print('(optional) install pygraphviz to generate graphs')
    sys.exit(0)

from iceprod.server import get_pkgdata_filename

def main():
    table_filename = get_pkgdata_filename('iceprod.server','data/etc/db_config.json')
    db_tables = json.load(open(table_filename),object_pairs_hook=OrderedDict)

    for k in db_tables['status_graphs']:
        outfile_name = os.path.join('static','lifecycle_'+k+'.png')
        if os.path.exists(outfile_name) and os.path.getmtime(outfile_name) > os.path.getmtime(table_filename):
            print('graph',outfile_name,'already exists. skipping')
            continue

        G = pgv.AGraph(strict=False,directed=True)
        G.add_nodes_from(db_tables['status_options'][k])
        for row in db_tables['status_graphs'][k]:
            if row[-1] == 'std':
                c = 'cornflowerblue'
            elif row[-1] == 'auto':
                c = 'cyan2'
            elif row[-1] == 'debug':
                c = 'chartreuse2'
            elif row[-1] == 'manual':
                c = 'firebrick2'
            G.add_edge(row[0],row[1],color=c)
        G.draw(outfile_name, prog='dot')

if __name__ == '__main__':
    main()
