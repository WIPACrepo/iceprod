
page_template = '''
    <h1>%s</h1>
    <p class = "desc">
    %s
    </p>
    <h2>Properties</h2>
    <ul>
    %s
    </ul>

    '''


# Converts ##LinkName## in string to html linkds
def add_links(desc):
    d = desc.split('##')
    is_link = False
    desc = ''
    for i in d:
        if is_link:
            desc += "<a href='javascript: show_doc(\"%s\");'>%s</a>" % (i, i)
        else:
            desc += i
        is_link = not is_link
    return desc


# Loads and parses documentation file
def load_doc(filename):
    try:
        f = open(filename.lower()+'.txt', 'rt')
    except IOError:
        return 'Not found'
    lines = []
    for line in f.readlines():
        if len(line.strip()):
            lines.append(line)

    body = ''
    for line in lines[1:]:
        nd = line.split(None, 1)
        name = nd[0]
        desc = nd[1] if len(nd) == 2 else ''
        desc = add_links(desc)
        body += '<li><h3>%s</h3><p>%s</p></li>' % (name, desc)

    nd = lines[0].split(None, 1)
    name = nd[0]
    desc = nd[1] if len(nd) == 2 else ''
    f.close()

    return page_template % (name, desc, body)
