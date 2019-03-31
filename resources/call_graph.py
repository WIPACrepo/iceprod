"""
Make a call graph for async functions.

Does not catch regular function calls.
"""
import string
from collections import OrderedDict

# utils
def is_func(line):
    line = line.strip(string.whitespace)
    return line.startswith('async def') or line.startswith('def')
all_whitespace = lambda line: not line.strip(string.whitespace)
def get_indent(line):
    for i,c in enumerate(line):
        if c not in string.whitespace:
            return i
    return -1

def find_func_names(lines):
    """Find all function names"""
    def get_name(line):
        return line.split('def ',1)[-1].split('(',1)[0]
    names = []
    found = False
    indent = -1
    for line in lines:
        line = line.strip('\r\n')
        if found and not all_whitespace(line):
            if get_indent(line) <= indent:
                found = False
        if (not found) and is_func(line):
            found = True
            indent = get_indent(line)
            names.append(get_name(line))
    return names

def find_func(lines, name):
    """Find the lines of a specific function"""
    is_my_func = lambda line: is_func(line) and name in line.split('(')[0]

    found = False
    indent = -1
    last_line = ''
    for line in lines:
        line = line.strip('\r\n')
        if last_line:
            line = last_line+line.strip()
        if line.strip().endswith(','):
            last_line = line
        else:
            last_line = ''
        if (not found) and is_my_func(line):
            found = True
            indent = get_indent(line)
        elif found and not all_whitespace(line):
            if get_indent(line) > indent:
                yield line
            else:
                return # end of function
    if not found:
        raise Exception(f'{name} not found')

def process_func(lines, func_names):
    """Search for function calls"""
    ret = OrderedDict()
    for line in lines:
        #print(f':: {line}')
        for n in func_names:
            if n+'(' in line:
                name = line.split(n+'(')[0].split('(')[-1].split()[-1]+n
                if name in ret:
                    ret[name] += 1
                else:
                    ret[name] = 1
        if 'await' in line:
            line = line.split('await',1)[-1].strip()
            if line.startswith('asyncio.ensure_future'):
                line = line.split('(',1)[-1]
            if 'rest_client.request' in line:
                line = line.split(')',1)[0]+')'
            else:
                line = line.split('(',1)[0]
            if line in ret:
                ret[line] += 1
            else:
                ret[line] = 1
    return ret

def analyze_calls(lines, funcname, indent=0, recurse=True):
    func_names = find_func_names(lines)
    calls = process_func(find_func(lines, funcname), func_names)
    for c in calls:
        if '.' in c and not c.startswith('self'):
            continue
        print(' '*indent+c)
        if recurse:
            if c.startswith('self.'):
                c = c[5:]
            try:
                analyze_calls(lines, c, indent=indent+2, recurse=True)
            except Exception as e:
                #print(' '*indent,e)
                pass

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument('function')
    parser.add_argument('-r','--recurse',default=True)
    args = parser.parse_args()

    print(f'searching for {args.function} in file {args.filename}')
    with open(args.filename) as f:
        lines = f.readlines()

    print('')
    print('Calls: ')
    analyze_calls(lines, args.function, indent=0, recurse=args.recurse)

if __name__ == '__main__':
    main()