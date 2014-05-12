"""
  A module for overloading xml functions

  copyright  (c) 2012 the icecube collaboration
"""

import StringIO

import logging

from iceprod.core import dataclasses

logger = logging.getLogger('xml')

# import xml library
try:
    from lxml import etree
except ImportError:
    logger.critical("Failed to import lxml.etree")
    raise


class XMLException(Exception):
    """Standard XML exception"""
    pass


DIFPLUS_DOCTYPE = '<!DOCTYPE DIF_Plus PUBLIC "-//W3C//DTD XML 1.0 Strict//EN" "env/share/iceprod/iceprod.v3.dtd">'
CONFIG_DOCTYPE = '<!DOCTYPE configuration PUBLIC "-//W3C//DTD XML 1.0 Strict//EN" "env/share/iceprod/iceprod.v3.dtd">'


def fromString(str):
    parser = etree.XMLParser(strip_cdata=False)
    return etree.fromstring(str,parser)
    
    
    
def getParameter(element):
    """Get a Parameter object from the xml element"""
    p = dataclasses.Parameter()
    if 'type' in element.attrib:
        p.type = element.attrib['type']
    if 'name' in element.attrib:
        p.name = element.attrib['name']
    if 'value' in element.attrib:
        p.value = element.attrib['value']
    elif element.text:
        p.value = element.text
    return p
    
def getResource(element,r=None):
    """Get a Resource object from the xml element"""
    if r is None:
        r = dataclasses.Resource()
    if 'remote' in element.attrib:
        r.remote = element.attrib['remote']
    if 'local' in element.attrib:
        r.local = element.attrib['local']
    if 'compression' in element.attrib:
        r.compression = element.attrib['compression']    
    if r.remote is None and r.local is None:
        raise XMLException('Not a valid xml configuration - resource tag missing remote and local, need at least one')
    return r
    
def getData(element):
    """Get a Data object from the xml element"""
    d = getResource(element,dataclasses.Data())
    if 'type' in element.attrib:
        d.type = element.attrib['type']
    else:
        raise XMLException('Not a valid xml configuration - data tag must have a type attribute')
    if 'movement' in element.attrib:
        d.movement = element.attrib['movement']
    else:
        raise XMLException('Not a valid xml configuration - data tag must have a movement attribute')
    return d
    
def getClass(element):
    """Get a Class object from the xml element"""
    c = dataclasses.Class()
    if 'name' in element.attrib:
        c.name = element.attrib['name']
    else:
        raise XMLException('Not a valid xml configuration - class tag must have a name attribute')
    if 'src' in element.attrib:
        c.src = element.attrib['src']
    if 'resource_name' in element.attrib:
        c.resource_name = element.attrib['resource_name']
    if 'recursive' in element.attrib:
        c.recursive = element.attrib['recursive']
    if 'libs' in element.attrib:
        c.libs = element.attrib['libs']
    if 'env_vars' in element.attrib:
        c.env_vars = element.attrib['env_vars']
    return c

def getProject(element):
    """Get a Project object from the xml element"""
    p = dataclasses.Project()
    if 'name' in element.attrib:
        p.name = element.attrib['name']
    if 'class' in element.attrib:
        p.class_name = element.attrib['class']
    else:
        raise XMLException('Not a valid xml configuration - project tag must have a class attribute')
    return p
    
def getPersonnel(element):
    """Get a Personnel object from the xml element"""
    p = dataclasses.Personnel()
    for x in element:
        if x.tag == 'Role':
            p.role = x.text
        elif x.tag == 'First_Name':
            p.first_name = x.text
        elif x.tag == 'Last_Name':
            p.last_name = x.text
        elif x.tag == 'Email':
            p.email = x.text
        else:
            logger.warning('unknown tag in getPersonnel: %s' % str(x.tag))
    return p

def loadDifPlus(filename,validate=True,config=None):
    """Load an xml DifPlus configuration from file"""
    parser = etree.XMLParser(dtd_validation=validate,strip_cdata=False)
    tree = etree.parse(filename,parser)
    root = tree.getroot()
    if root.tag != 'DIF_Plus':
        raise XMLException('Not a valid xml DifPlus configuration - DifPlus tag is not root')
    
    if not config:
        config = dataclasses.Job()
    config.difplus = dataclasses.DifPlus()
    
    for x in root:
        if x.tag.lower() == 'dif':
            # get Dif
            dif = dataclasses.Dif()
            for y in x:
                if y.tag == 'Entry_ID':
                    dif.entry_id = y.text
                elif y.tag == 'Entry_Title':
                    dif.entry_title = y.text
                elif y.tag == 'Parameters':
                    dif.parameters = y.text
                elif y.tag == 'ISO_Topic_Category':
                    dif.iso_topic_category = y.text
                elif y.tag == 'Data_Center':
                    dc = dataclasses.DataCenter()
                    for z in y:
                        if z.tag == 'Data_Center_Name':
                            dc.name = z.text
                        elif z.tag == 'Personnel':
                            dc.personnel = getPersonnel(z)
                    dif.data_center = dc
                elif y.tag == 'Summary':
                    dif.summary = y.text
                elif y.tag == 'Metadata_Name':
                    dif.metadata_name = y.text
                elif y.tag == 'Metadata_Version':
                    dif.metadata_version = y.text
                elif y.tag == 'Personnel':
                    dif.personnel = getPersonnel(y)
                elif y.tag == 'Sensor_Name':
                    dif.sensor_name = y.text
                elif y.tag == 'Source_Name':
                    dif.source_name = y.text
                elif y.tag == 'DIF_Creation_Date':
                    dif.dif_creation_date = y.text
                else:
                    logger.warning('unknown tag in Dif: %s' % str(y.tag))
            config.difplus.dif = dif
        elif x.tag.lower() == 'plus':
            # get Plus
            plus = dataclasses.Plus()
            for y in x:
                if y.tag == 'Start_DateTime':
                    plus.start = y.text
                elif y.tag == 'End_DateTime':
                    plus.end = y.text
                elif y.tag == 'Category':
                    plus.category = y.text
                elif y.tag == 'Subcategory':
                    plus.subcategory = y.text
                elif y.tag == 'Run_Number':
                    plus.run_number = y.text
                elif y.tag == 'I3Db_Key':
                    plus.i3db_key = y.text
                elif y.tag == 'SimDb_Key':
                    plus.simdb_key = y.text
                elif y.tag == 'Project':
                    # get project
                    name = None
                    version = None
                    for z in y:
                        if z.tag == 'Name':
                            name = z.text
                        elif z.tag == 'Version':
                            version = z.text
                    if name and version:
                        plus.project[name] = version
                elif y.tag == 'Steering_File':
                    plus.steering_file = y.text
                elif y.tag == 'Log_File':
                    plus.log_file = y.text
                elif y.tag == 'Command_Line':
                    plus.command_line = y.text
                else:
                    logger.warning('unknown tag in Plus: %s' % str(y.tag))
            config.difplus.plus = plus
        else:
            logger.warning('unknown tag in DifPlus: %s' % str(x.tag))
    
    # return config
    return config
    
def loadXML(filename,validate=True,config=None):
    """Load an xml configuration from file"""
    parser = etree.XMLParser(dtd_validation=validate,strip_cdata=False)
    tree = etree.parse(filename,parser)
    root = tree.getroot()
    if root.tag != 'configuration':
        raise XMLException('Not a valid xml configuration - configuation tag is not root')
    
    if not config:
        config = dataclasses.Job()
    
    # get configuration
    if 'parentid' in root.attrib:
        config.parent_id = root.attrib['parentid']
        try:
            # try converting to int
            config.parent_id = int(config.parent_id)
        except:
            pass
    if 'version' in root.attrib:
        config.xml_version = root.attrib['version']
        try:
            # try converting to int
            config.xml_version = int(config.xml_version)
        except:
            try:
                # try converting to float
                config.xml_version = float(config.xml_version)
            except:
                pass
    if 'iceprod_version' in root.attrib:
        config.iceprod_version = root.attrib['iceprod_version']
        try:
            # try converting to int
            config.iceprod_version = int(config.iceprod_version)
        except:
            try:
                # try converting to float
                config.iceprod_version = float(config.iceprod_version)
            except:
                pass
    
    # get options
    nodes = [x for x in root.iter('options')]
    if len(nodes):
        options_xml = nodes.pop()
        for x in options_xml:
            if x.tag == 'parameter':
                p = getParameter(x)
                config.options[p.name] = p
            else:
                logger.warning('unknown tag in options: %s' % str(x.tag))
    
    # get steering
    nodes = [x for x in root.iter('steering')]
    if len(nodes):
        steering_xml = nodes.pop()
        config.steering = dataclasses.Steering()
        for x in steering_xml:
            if x.tag == 'parameter':            
                p = getParameter(x)
                config.steering.parameters[p.name] = p
            elif x.tag == 'resource':
                config.steering.resources.append(getResource(x))
            elif x.tag == 'data':
                config.steering.data.append(getData(x))
            elif x.tag == 'system':
                # get system parameters
                for y in x:
                    if y.tag == 'parameter':           
                        p = getParameter(y)
                        config.steering.system[p.name] = p
                    else:
                        logger.warning('unknown tag in steering.system: %s' % str(y.tag))
            elif x.tag == 'batchsys':
                # get batchsys name
                name = 'all'
                if 'name' in x.attrib:
                    name = x.attrib['name']
                if name not in config.steering.batchsys:
                    config.steering.batchsys[name] = {}
                # get batchsys parameters
                for y in x:
                    if y.tag == 'parameter':
                        p = getParameter(y)
                        config.steering.batchsys[name][p.name] = p
                    else:
                        logger.warning('unknown tag in steering.batchsys: %s' % str(y.tag))
            else:
                logger.warning('unknown tag in steering: %s' % str(x.tag))
        
    # get tasks, trays, modules
    for node in root.iter('task'):
        task = dataclasses.Task()
        # get task name
        if 'name' in node.attrib:
            task.name = node.attrib['name']
        else:
            task.name = 'task %d' % (len(config.tasks))
        if task.name.find(',') >= 0:
            logger.warning('the task name \'%s\' contains a comma, which is not valid' % task.name)
            task.name = 'task %d' % (len(config.tasks))
        if task.name in config.tasks:
            logger.warning('the task name \'%s\' is used more than once' % task.name)
            task.name = 'task %d' % (len(config.tasks))
            
        # get depends
        if 'depends' in node.attrib:
            depends = node.attrib['depends']
            if depends and len(depends) > 0:
                task.depends.extend(depends.split(','))
        
        # get parameters, resources, data, classes, projects, trays
        for x in node:
            if x.tag == 'parameter':
                p = getParameter(x)
                task.parameters[p.name] = p
            elif x.tag == 'batchsys':
                # get batchsys name
                name = 'all'
                if 'name' in x.attrib:
                    name = x.attrib['name']
                if name not in task.batchsys:
                    task.batchsys[name] = {}
                # get batchsys parameters
                for y in x:
                    if y.tag == 'parameter':
                        p = getParameter(y)
                        task.batchsys[name][p.name] = p
                    else:
                        logger.warning('unknown tag in steering.batchsys: %s' % str(y.tag))
            elif x.tag == 'resource':
                task.resources.append(getResource(x))
            elif x.tag == 'data':
                task.data.append(getData(x))
            elif x.tag == 'class':
                task.classes.append(getClass(x))
            elif x.tag == 'project':
                task.projects.append(getProject(x))
            elif x.tag == 'tray':
                # get tray
                tray = dataclasses.Tray()
                
                # get tray name
                if 'name' in x.attrib:
                    tray.name = x.attrib['name']
                else:
                    tray.name = 'tray %d' % (len(task.trays))
                if tray.name.find(',') >= 0:
                    logger.warning('the tray name \'%s\' contains a comma, which is not valid' % tray.name)
                    tray.name = 'tray %d' % (len(task.trays))
                if tray.name in task.trays:
                    logger.warning('the tray name \'%s\' is used more than once' % tray.name)
                    tray.name = 'tray %d' % (len(task.trays))
                    
                # get iterations
                if 'iter' in x.attrib:
                    try:
                        tray.iterations = int(x.attrib['iter'])
                    except:
                        logger.warning('bad conversion to int when trying to get tray iterations for tray \'%s\', defaulting to 1' % tray.name)
                
                # get parameters, resources, data, classes, projects, modules
                for y in x:
                    if y.tag == 'parameter':
                        p = getParameter(y)
                        tray.parameters[p.name] = p
                    elif y.tag == 'resource':
                        tray.resources.append(getResource(y))
                    elif y.tag == 'data':
                        tray.data.append(getData(y))
                    elif y.tag == 'class':
                        tray.classes.append(getClass(y))
                    elif y.tag == 'project':
                        tray.projects.append(getProject(y))
                    elif y.tag == 'module':
                        # get module
                        module_good = True
                        module = dataclasses.Module()
                        
                        # get module name
                        if 'name' in y.attrib:
                            module.name = y.attrib['name']
                        else:
                            module.name = 'module %d' % (len(tray.modules))
                        if module.name.find(',') >= 0:
                            logger.warning('the module name \'%s\' contains a comma, which is not valid' % module.name)
                            module.name = 'module %d' % (len(tray.modules))
                        if module.name in tray.modules:
                            logger.warning('the module name \'%s\' is used more than once' % module.name)
                            module.name = 'module %d' % (len(tray.modules))
                        
                        # get running class
                        if 'class' in y.attrib:
                            module.running_class = y.attrib['class']
                        
                        # get source
                        if 'src' in y.attrib:
                            module.src = y.attrib['src']
                        
                        # get args
                        if 'args' in y.attrib:
                            module.args = y.attrib['args']
                        
                        if not module.src and not module.running_class:
                            module_good = False
                            logger.warning('the module name \'%s\' does not define a src or class to run' % module.name)
                        
                        # get parameters, resources, data, classes, projects
                        for z in y:
                            if z.tag == 'parameter':
                                p = getParameter(z)
                                module.parameters[p.name] = p
                            elif z.tag == 'resource':
                                module.resources.append(getResource(z))
                            elif z.tag == 'data':
                                module.data.append(getData(z))
                            elif z.tag == 'class':
                                module.classes.append(getClass(z))
                            elif z.tag == 'project':
                                module.projects.append(getProject(z))
                            else:
                                logger.warning('unknown tag in module %s: %s' % (module.name,str(z.tag)))
                        
                        # add module to tray
                        if module_good:
                            tray.modules[module.name] = module
                        else:
                            logger.error('did not add module %s because of errors in configuration' % module.name)
                    else:
                        logger.warning('unknown tag in tray %s: %s' % (tray.name,str(y.tag)))
                # add tray to task
                task.trays[tray.name] = tray
            else:
                logger.warning('unknown tag in task %s: %s' % (task.name,str(x.tag)))
        # add task to config
        config.tasks[task.name] = task
        
    # return config
    return config
    
def makeParameter(node,param):
    if not param.type:
        param.type = 'string'
    attrib = {'type': str(param.type),
              'name': str(param.name)}
    if param.type == 'pickle':
        n = etree.SubElement(node,'parameter',attrib)
        n.text = etree.CDATA(param.value)
    else:
        try:
            attrib['value'] = unicode(param.value)
        except:
            attrib['value'] = str(param.value)
        etree.SubElement(node,'parameter',attrib)
    
def makeResource(node,resource):
    attrib = {}    
    if resource.remote:
        attrib['remote'] = str(resource.remote)
    if resource.local:
        attrib['local'] = str(resource.local)
    if resource.compression:
        attrib['compression'] = str(resource.compression)
    etree.SubElement(node,'resource',attrib)

def makeData(node,data):
    attrib = {'type':    data.type,
              'movement':data.movement}
    if data.remote:
        attrib['remote'] = str(data.remote)
    if data.local:
        attrib['local'] = str(data.local)
    if data.compression:
        attrib['compression'] = str(data.compression)
    etree.SubElement(node,'data',attrib)
    
def makeClass(node,classes):
    attrib = {'name': str(classes.name),
              'src':  str(classes.src)}
    if classes.recursive:
        attrib['recursive'] = str(classes.recursive)
    if classes.resource_name:
        attrib['resource_name'] = str(classes.resource_name)
    if classes.libs:
        attrib['libs'] = str(classes.libs)
    if classes.env_vars:
        attrib['env_vars'] = str(classes.env_vars)
    etree.SubElement(node,'class',attrib)     
    
def makeProject(node,project):
    attrib = {'class': str(project.class_name)}
    if project.name:
        attrib['name'] = str(project.name)
    etree.SubElement(node,'project',attrib)
    
def makePersonnel(node,person):
    e = etree.SubElement(node,'Personnel')
    if person.role:
        f = etree.SubElement(e,'Role')
        f.text = str(person.role)
    if person.first_name:
        f = etree.SubElement(e,'First_Name')
        f.text = str(person.first_name)
    if person.last_name:
        f = etree.SubElement(e,'Last_Name')
        f.text = str(person.last_name)
    if person.email:
        f = etree.SubElement(e,'Email')
        f.text = str(person.email)
    
def todifplus(config):
    """Convert from python objects to difplus output"""
    tree = etree.parse(StringIO.StringIO('%s<DIF_Plus/>'%DIFPLUS_DOCTYPE))
    xml = tree.getroot()
    xml.attrib['{http://www.w3.org/2001/XMLSchema-instance}noNamespaceSchemaLocation'] = 'IceCubeDIFPlus.xsd'
    
    if not config.difplus:
        raise XMLException('No Dif Plus in this config')
    
    # build dif
    dif = config.difplus.dif
    xml_dif = etree.SubElement(xml,'DIF')
    if dif.entry_id:
        e = etree.SubElement(xml_dif,'Entry_ID')
        e.text = str(dif.entry_id)
    if dif.entry_title:
        e = etree.SubElement(xml_dif,'Entry_Title')
        e.text = str(dif.entry_title)
    e = etree.SubElement(xml_dif,'Parameters')
    e.text = str(dif.parameters)
    e = etree.SubElement(xml_dif,'ISO_Topic_Category')
    e.text = str(dif.iso_topic_category)
    # build data center
    xml_dc = etree.SubElement(xml_dif,'Data_Center')
    if dif.data_center.name:
        e = etree.SubElement(xml_dc,'Data_Center_Name')
        e.text = str(dif.data_center.name)
    if dif.data_center.personnel:
        makePersonnel(xml_dc,dif.data_center.personnel)
    e = etree.SubElement(xml_dif,'Summary')
    e.text = str(dif.summary)
    e = etree.SubElement(xml_dif,'Metadata_Name')
    e.text = str(dif.metadata_name)
    e = etree.SubElement(xml_dif,'Metadata_Version')
    e.text = str(dif.metadata_version)
    if dif.personnel:
        makePersonnel(xml_dif,dif.personnel)
    e = etree.SubElement(xml_dif,'Sensor_Name')
    e.text = str(dif.sensor_name)
    e = etree.SubElement(xml_dif,'Source_Name')
    e.text = str(dif.source_name)
    e = etree.SubElement(xml_dif,'DIF_Creation_Date')
    e.text = str(dif.dif_creation_date)
    
    # build plus
    plus = config.difplus.plus
    xml_plus = etree.SubElement(xml,'Plus')
    if plus.start:
        e = etree.SubElement(xml_plus,'Start_DateTime')
        e.text = str(plus.start)
    if plus.end:
        e = etree.SubElement(xml_plus,'End_DateTime')
        e.text = str(plus.end)
    if plus.category:
        e = etree.SubElement(xml_plus,'Category')
        e.text = str(plus.category)
    if plus.subcategory:
        e = etree.SubElement(xml_plus,'Subcategory')
        e.text = str(plus.subcategory)
    if plus.run_number:
        e = etree.SubElement(xml_plus,'Run_Number')
        e.text = str(plus.run_number)
    if plus.i3db_key:
        e = etree.SubElement(xml_plus,'I3Db_Key')
        e.text = str(plus.i3db_key)
    if plus.simdb_key:
        e = etree.SubElement(xml_plus,'SimDb_Key')
        e.text = str(plus.simdb_key)
    for name in plus.project.keys():
        xml_p = etree.SubElement(xml_plus,'Project')
        e = etree.SubElement(xml_p,'Name')
        e.text = str(name)
        e = etree.SubElement(xml_p,'Version')
        e.text = str(plus.project[name])
    if plus.steering_file:
        e = etree.SubElement(xml_plus,'Steering_File')
        e.text = str(plus.steering_file)
    if plus.log_file:
        e = etree.SubElement(xml_plus,'Log_File')
        e.text = str(plus.log_file)
    if plus.command_line:
        e = etree.SubElement(xml_plus,'Command_Line')
        e.text = str(plus.command_line)
    
    # return difplus
    return tree
    
def toDifPlusstring(config,pretty=False):
    """Return a DIF Plus configuration as an xml string"""
    try:
        xml = todifplus(config)
        return etree.tostring(xml,encoding='UTF-8',xml_declaration=True,pretty_print=pretty)
    except Exception as e:
        logger.error('Failed to write DIF Plus to string')
        raise

def writeDifPlus(filename,config,pretty=False):
    """Write a DIF Plus configuration to file"""
    try:
        xml = todifplus(config)
        xml.write(filename,encoding='UTF-8',xml_declaration=True,pretty_print=pretty)
    except Exception as e:
        logger.error('Failed to write DIF Plus to file \'%s\'' % str(filename))
        raise
    

def toxml(config):
    """Convert from python objects to xml"""
    tree = etree.parse(StringIO.StringIO('%s<configuration/>'%CONFIG_DOCTYPE))
    xml = tree.getroot()
    xml.attrib['{http://www.w3.org/2001/XMLSchema-instance}noNamespaceSchemaLocation'] = 'config.xsd'
    xml.attrib['version'] = str(config.xml_version)
    xml.attrib['iceprod_version'] = str(config.iceprod_version)
    xml.attrib['parentid'] = str(config.parent_id)
    
    if len(config.options) > 0:
        # add options
        xml_options = etree.SubElement(xml,'options')
        for param in config.options.values():
            makeParameter(xml_options,param)
    
    if config.steering:
        # build steering
        steering = etree.SubElement(xml,'steering')
        for param in config.steering.parameters.values():
            makeParameter(steering,param)
        for resource in config.steering.resources:
            makeResource(steering,resource)
        for data in config.steering.data:
            makeData(steering,data)
        if len(config.steering.system) > 0:
            # add system
            xml_system = etree.SubElement(steering,'system')
            for param in config.steering.system.values():
                makeParameter(xml_system,param)
        if len(config.steering.batchsys) > 0:
            # add batchsys
            for name in config.steering.batchsys.keys():
                xml_batchsys = etree.SubElement(steering,'batchsys',{'name':name})
                for param in config.steering.batchsys[name].values():
                    makeParameter(xml_batchsys,param)
    
    # build tasks
    for task in config.tasks.values():
        task_attrib = {}
        if task.name:
            task_attrib['name'] = str(task.name)
        if len(task.depends) > 0:
            task_attrib['depends'] = ','.join(task.depends)
        xml_task = etree.SubElement(xml,'task',task_attrib)
        if len(task.batchsys) > 0:
            # add batchsys
            for name in task.batchsys.keys():
                xml_batchsys = etree.SubElement(xml_task,'batchsys',{'name':name})
                for param in task.batchsys[name].values():
                    makeParameter(xml_batchsys,param)
        for param in task.parameters.values():
            makeParameter(xml_task,param)
        for resource in task.resources:
            makeResource(xml_task,resource)
        for data in task.data:
            makeData(xml_task,data)
        for classes in task.classes:
            makeClass(xml_task,classes)
        for project in task.projects:
            makeProject(xml_task,project)        
        
        # build trays
        for tray in task.trays.values():
            tray_attrib = {'iter': str(tray.iterations)}
            if tray.name:
                tray_attrib['name'] = str(tray.name)
            xml_tray = etree.SubElement(xml_task,'tray',tray_attrib)
            for param in tray.parameters.values():
                makeParameter(xml_tray,param)
            for resource in tray.resources:
                makeResource(xml_tray,resource)
            for data in tray.data:
                makeData(xml_tray,data)
            for classes in tray.classes:
                makeClass(xml_tray,classes)
            for project in tray.projects:
                makeProject(xml_tray,project)  
            
            # build modules
            for module in tray.modules.values():
                module_attrib = {}
                if module.name:
                    module_attrib['name'] = str(module.name)
                if module.running_class:
                    module_attrib['class'] = str(module.running_class)
                if module.src:
                    module_attrib['src'] = str(module.src)
                if module.args:
                    module_attrib['args'] = str(module.args)
                xml_module = etree.SubElement(xml_tray,'module',module_attrib)
                for param in module.parameters.values():
                    makeParameter(xml_module,param)
                for resource in module.resources:
                    makeResource(xml_module,resource)
                for data in module.data:
                    makeData(xml_module,data)
                for classes in module.classes:
                    makeClass(xml_module,classes)
                for project in module.projects:
                    makeProject(xml_module,project)  
    
    # return xml
    return tree

def toXMLstring(config,pretty=False):
    """Print xml configuration to string"""
    try:
        xml = toxml(config)
        return etree.tostring(xml,encoding='UTF-8',xml_declaration=True,pretty_print=pretty)
    except Exception as e:
        logger.error('Failed to write xml tree to string')
        raise

def writeXML(filename,config,pretty=False):
    """Write an xml configuration to file"""
    try:
        xml = toxml(config)
        xml.write(filename,encoding='UTF-8',xml_declaration=True,pretty_print=pretty)
    except Exception as e:
        logger.error('Failed to write configuration to file \'%s\'' % str(filename))
        raise
    
