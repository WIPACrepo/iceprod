"""
Test script for core xml
"""

from __future__ import absolute_import, division, print_function

from tests.util import printer, glob_tests

import logging
logger = logging.getLogger('xml')

import os, sys, time
import shutil
import random
import string
import subprocess

try:
    import cPickle as pickle
except:
    import pickle
    
try:
    import unittest2 as unittest
except ImportError:
    import unittest
    
from lxml import etree

import iceprod.core.xml
import iceprod.core.functions
import iceprod.core.dataclasses

good_metadata = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE DIF_Plus PUBLIC "-//W3C//DTD XML 1.0 Strict//EN" "http://x2100.icecube.wisc.edu/dtd/iceprod.v3.dtd">
<DIF_Plus xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:noNamespaceSchemaLocation="IceCubeDIFPlus.xsd">
  <DIF>
    <Entry_ID>TBD</Entry_ID>
    <Entry_Title>IC59 triple-coincident CORSIKA</Entry_Title>
    <Parameters>SPACE SCIENCE &gt; Astrophysics &gt; Neutrinos</Parameters>
    <ISO_Topic_Category>geoscientificinformation</ISO_Topic_Category>
    <Data_Center>
      <Data_Center_Name>UWI-MAD/A3RI &gt; Antarctic Astronomy and Astrophysics Research Institute, University of Wisconsin, Madison</Data_Center_Name>
      <Personnel>
        <Role>Data Center Contact</Role>
        <Email>datacenter@icecube.wisc.edu</Email>
      </Personnel>
    </Data_Center>
    <Personnel>
      <Role>Data Center Contact</Role>
      <Email>datacenter@icecube.wisc.edu</Email>
    </Personnel>
    <Summary>IC59 triple-coincident CORSIKA-in-ice polygonato model with weighted (dslope -1) spectrum of Hoerandel, using &lt;b&gt;AHA07v2_SPICE1_i3coords_cos80&lt;/b&gt; photon tables. Angular range of 0deg &lt; theta &lt; 89.99deg and energy range of 600GeV &lt; Eprim &lt; 1e11GeV. </Summary>
    <Metadata_Name>[CEOS IDN DIF]</Metadata_Name>
    <Metadata_Version>9.4</Metadata_Version>
    <Sensor_Name>ICECUBE &gt; IceCube</Sensor_Name>
    <Source_Name>SIMULATION &gt; Data which are numerically generated</Source_Name>
    <DIF_Creation_Date>2011-02-24</DIF_Creation_Date>
  </DIF>
  <Plus>
    <Start_DateTime>2011-01-01T00:00:01</Start_DateTime>
    <End_DateTime>2011-12-31T00:00:00</End_DateTime>
    <Category>filtered</Category>
    <Subcategory>CORSIKA-in-ice</Subcategory>
    <Run_Number>1</Run_Number>
    <I3Db_Key>5359</I3Db_Key>
    <SimDb_Key>5359</SimDb_Key>
    <Steering_File>None</Steering_File>
    <Log_File>None</Log_File>
    <Command_Line>None</Command_Line>
    <Project>
      <Name>Name</Name>
      <Version>1</Version>
    </Project>
  </Plus>
</DIF_Plus>"""

good_configuration = """<?xml version='1.0' encoding='UTF-8'?>
<!DOCTYPE configuration PUBLIC "-//W3C//DTD XML 1.0 Strict//EN" "http://x2100.icecube.wisc.edu/dtd/iceprod.v3.dtd">
<configuration xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:noNamespaceSchemaLocation='config.xsd' version='3.0' iceprod_version='2.0' parentid='0'>
  <options>
    <parameter type='bool' name='n' value='True' />
  </options>
  <steering>
    <parameter type='string' name='steering_param' value='sp' />
    <parameter type='int' name='sp2' value='17' />
    <resource remote='r' local='l' compression='True' />
    <data remote='r' local='l' compression='True' type='permanent' movement='both' />
    <system>
      <parameter type='str_list' name='sys_param' value='["t1","t2"]' />
    </system>
    <batchsys name='condor'>
      <parameter type='string' name='Requirements' value='Machine = "node001"' />
    </batchsys>
  </steering>
  <task depends="152,142" name="task1">
    <batchsys name='condor'>
      <parameter type='string' name='Requirements' value='Machine != "node002"' />
    </batchsys>
    <parameter type='double' name='task_param' value='1.2' />
    <resource remote='r' local='l' compression='True' />
    <data remote='r' local='l' compression='True' type='permanent' movement='both' />
    <class name='n' recursive='True' src='s' resource_name='r' />
    <project name='n' class='c' />
    <tray iter="2" name="Corsika">
      <parameter type='double' name='tray_param' value='1.2' />
      <resource remote='r' local='l' compression='True' />
      <data remote='r' local='l' compression='True' type='permanent' movement='both' />
      <class name='n' recursive='True' src='s' resource_name='r' />
      <project name='n' class='c' />
      <module name="generate_corsika" class="generators.CorsikaIC" src="s" args="a=1">
        <parameter type='double' name='mod_param' value='1.2' />
        <resource remote='r' local='l' compression='True' />
        <data remote='r' local='l' compression='True' type='permanent' movement='both' />
        <class name='n' recursive='True' src='s' resource_name='r' />
        <project name='n' class='c' />
      </module>
    </tray>
  </task>
</configuration>"""


def cmpXML(a, b):
    """Takes two xml roots and compares them, returning the first diff"""
    if a.tag != b.tag:
        return 'tag: %s != %s'%(a.tag,b.tag)
    if sorted(a.attrib.items()) != sorted(b.attrib.items()):
        return 'attrib: %s != %s'%(a.attrib,b.attrib)
    if len(a) != len(b):
        return 'children len: %d != %d'%(len(a),len(b))
    def k(c):
        if c.attrib:
            return c.tag+str(c.attrib)
        return c.tag
    for ac, bc in zip(sorted(a,key=k),sorted(b,key=k)):
        ret = cmpXML(ac,bc)
        if ret:
            return ret
    return None

class xml_test(unittest.TestCase):
    def setUp(self):
        self.test_dir = os.path.join(os.getcwd(),'test')
        if not os.path.exists(self.test_dir):
            os.mkdir(self.test_dir)
        super(xml_test,self).setUp()
    
    def tearDown(self):
        shutil.rmtree(self.test_dir)
        super(xml_test,self).tearDown()
        
    def test_01_fromString(self):
        """Test the fromString function"""
        try:
            for i in xrange(0,5):
                # create valid xml string
                xmlstr = '<test a="b"><new c="d">efgh</new></test>'
                root = iceprod.core.xml.fromString(xmlstr)
                if root.tag != 'test':
                    raise Exception, 'root tag incorrect'
                if 'a' not in root.attrib:
                    raise Exception, 'attrib key not in root.attrib'
                if root.attrib['a'] != 'b':
                    raise Exception, 'attrib value not in root.attrib'
                if len(root) < 1:
                    raise Exception, 'root missing children'
                if root[0].tag != 'new':
                    raise Exception, 'child tag incorrect'
                if 'c' not in root[0].attrib:
                    raise Exception, 'attrib key not in child.attrib'
                if root[0].attrib['c'] != 'd':
                    raise Exception, 'attrib value not in child.attrib'
                if root[0].text != 'efgh':
                    raise Exception, 'child.text incorrect'
                if len(root[0]) != 0:
                    raise Exception, 'child not supposed to have children'
                
                # create invalid xml string
                xmlstr = 'tets<t>inner</t>out'
                try:
                    root = iceprod.core.xml.fromString(xmlstr)
                except:
                    pass
                else:
                    raise Exception, 'bad xml string did not generate an exception'
            
        except Exception, e:
            logger.error('Error running fromString test: %s',str(e))
            printer('Test xml.fromString()',False)
            raise
        else:
            printer('Test xml.fromString()')
    
    def test_02_getParameter(self):
        """Test the getParameter function"""
        try:
            for i in xrange(0,5):
                # do str type
                xmlstr = "<parameter type='str' name='n' value='v' />"
                root = iceprod.core.xml.fromString(xmlstr)
                param = iceprod.core.xml.getParameter(root)
                
                if not param or not isinstance(param,iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad parameter class'
                if param.type != 'str':
                    raise Exception, 'Bad type'
                if param.name != 'n':
                    raise Exception, 'Bad name'
                if param.value != 'v':
                    raise Exception, 'Bad value'
                
                # do double type
                xmlstr = "<parameter type='double' name='n' value='1.2' />"
                root = iceprod.core.xml.fromString(xmlstr)
                param = iceprod.core.xml.getParameter(root)
                
                if not param or not isinstance(param,iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad parameter class'
                if param.type != 'double':
                    raise Exception, 'Bad type'
                if param.name != 'n':
                    raise Exception, 'Bad name'
                if param.value != '1.2':
                    raise Exception, 'Bad value'
                
                # do bool_list type
                xmlstr = "<parameter type='bool_list' name='n' value='[True,False,False,True]' />"
                root = iceprod.core.xml.fromString(xmlstr)
                param = iceprod.core.xml.getParameter(root)
                
                if not param or not isinstance(param,iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad parameter class'
                if param.type != 'bool_list':
                    raise Exception, 'Bad type'
                if param.name != 'n':
                    raise Exception, 'Bad name'
                if param.value != '[True,False,False,True]':
                    raise Exception, 'Bad value'
                    
                # do pickle type
                obj = iceprod.core.dataclasses.Resource()
                obj.remote = 'http://iceprod'
                obj.local = 'lo'
                obj.compression = True
                xmlstr = "<parameter type='pickle' name='n'><![CDATA["+pickle.dumps(obj)+"]]></parameter>"
                root = iceprod.core.xml.fromString(xmlstr)
                param = iceprod.core.xml.getParameter(root)
                
                if not param or not isinstance(param,iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad parameter class'
                if param.type != 'pickle':
                    raise Exception, 'Bad type'
                if param.name != 'n':
                    raise Exception, 'Bad name'
                newobj = pickle.loads(param.value)
                if ((not isinstance(newobj,iceprod.core.dataclasses.Resource)) or 
                    newobj.remote != obj.remote or 
                    newobj.local != obj.local or 
                    newobj.compression != obj.compression):
                    raise Exception, 'Bad value'
                
        except Exception, e:
            logger.error('Error running getParameter test: %s',str(e))
            printer('Test xml.getParameter()',False)
            raise
        else:
            printer('Test xml.getParameter()')
    
    def test_03_getResource(self):
        """Test the getResource function"""
        try:
            for i in xrange(0,5):
                # good resource
                xmlstr = "<resoure remote='http://remote' local='lo' compression='True' />"
                root = iceprod.core.xml.fromString(xmlstr)
                resource = iceprod.core.xml.getResource(root)
                
                if not resource or not isinstance(resource,iceprod.core.dataclasses.Resource):
                    raise Exception, 'Bad resource class'
                if resource.remote != 'http://remote':
                    raise Exception, 'Bad remote'
                if resource.local != 'lo':
                    raise Exception, 'Bad local'
                if resource.compression != 'True':
                    raise Exception, 'Bad compression'
                    
                # bad resource
                xmlstr = "<resoure compression='True' />"
                root = iceprod.core.xml.fromString(xmlstr)
                try:
                    resource = iceprod.core.xml.getResource(root)
                except iceprod.core.xml.XMLException:
                    pass
                else:
                    raise Exception, 'bad resource did not generate an exception'
                
        except Exception, e:
            logger.error('Error running getResource test: %s',str(e))
            printer('Test xml.getResource()',False)
            raise
        else:
            printer('Test xml.getResource()')
    
    def test_04_getData(self):
        """Test the getData function"""
        try:
            for i in xrange(0,5):
                # good data
                xmlstr = "<data remote='http://remote' local='lo' compression='True' type='job_temp' movement='input' />"
                root = iceprod.core.xml.fromString(xmlstr)
                resource = iceprod.core.xml.getData(root)
                
                if not resource or not isinstance(resource,iceprod.core.dataclasses.Data):
                    raise Exception, 'Bad resource class'
                if resource.remote != 'http://remote':
                    raise Exception, 'Bad remote'
                if resource.local != 'lo':
                    raise Exception, 'Bad local'
                if resource.compression != 'True':
                    raise Exception, 'Bad compression'
                if resource.type != 'job_temp':
                    raise Exception, 'Bad type'
                if resource.movement != 'input':
                    raise Exception, 'Bad movement'
                    
                # bad data
                xmlstr = "<data remote='http://remote' local='lo' type='permanent' />"
                root = iceprod.core.xml.fromString(xmlstr)
                try:
                    resource = iceprod.core.xml.getData(root)
                except iceprod.core.xml.XMLException:
                    pass
                else:
                    raise Exception, 'bad data did not generate an exception'
                
                # bad data
                xmlstr = "<data remote='http://remote' local='lo' movement='both' />"
                root = iceprod.core.xml.fromString(xmlstr)
                try:
                    resource = iceprod.core.xml.getData(root)
                except iceprod.core.xml.XMLException:
                    pass
                else:
                    raise Exception, 'bad data did not generate an exception'
                
        except Exception, e:
            logger.error('Error running getData test: %s',str(e))
            printer('Test xml.getData()',False)
            raise
        else:
            printer('Test xml.getData()')

    def test_05_getClass(self):
        """Test the getClass function"""
        try:
            for i in xrange(0,5):
                # good data
                xmlstr = "<class name='n' src='http://remote' resource_name='test' recursive='False' libs='lib,lib/tools' env_vars='PATH=test' />"
                root = iceprod.core.xml.fromString(xmlstr)
                resource = iceprod.core.xml.getClass(root)
                
                if not resource or not isinstance(resource,iceprod.core.dataclasses.Class):
                    raise Exception, 'Bad Class class'
                if resource.name != 'n':
                    raise Exception, 'Bad name'
                if resource.src != 'http://remote':
                    raise Exception, 'Bad src'
                if resource.resource_name != 'test':
                    raise Exception, 'Bad resource_name'
                if resource.recursive != 'False':
                    raise Exception, 'Bad recursive'
                if resource.libs != 'lib,lib/tools':
                    raise Exception, 'Bad libs'
                if resource.env_vars != 'PATH=test':
                    raise Exception, 'Bad env vars'
                    
                # bad data
                xmlstr = "<class src='http://remote' resource_name='test' recursive='False' />"
                root = iceprod.core.xml.fromString(xmlstr)
                try:
                    resource = iceprod.core.xml.getClass(root)
                except iceprod.core.xml.XMLException:
                    pass
                else:
                    raise Exception, 'bad data did not generate an exception'
                
        except Exception, e:
            logger.error('Error running getClass test: %s',str(e))
            printer('Test xml.getClass()',False)
            raise
        else:
            printer('Test xml.getClass()')

    def test_06_getProject(self):
        """Test the getProject function"""
        try:
            for i in xrange(0,5):
                # good data
                xmlstr = "<project name='n' class='c' />"
                root = iceprod.core.xml.fromString(xmlstr)
                resource = iceprod.core.xml.getProject(root)
                
                if not resource or not isinstance(resource,iceprod.core.dataclasses.Project):
                    raise Exception, 'Bad Project class'
                if resource.name != 'n':
                    raise Exception, 'Bad name'
                if resource.class_name != 'c':
                    raise Exception, 'Bad class name'
                    
                # bad data
                xmlstr = "<project name='n' />"
                root = iceprod.core.xml.fromString(xmlstr)
                try:
                    resource = iceprod.core.xml.getProject(root)
                except iceprod.core.xml.XMLException:
                    pass
                else:
                    raise Exception, 'bad data did not generate an exception'
                
        except Exception, e:
            logger.error('Error running getProject test: %s',str(e))
            printer('Test xml.getProject()',False)
            raise
        else:
            printer('Test xml.getProject()')

    def test_07_getPersonnel(self):
        """Test the getPersonnel function"""
        try:
            for i in xrange(0,5):
                # good data
                xmlstr = "<Personnel><Role>R</Role><First_Name>F</First_Name><Last_Name>L</Last_Name><Email>e</Email></Personnel>"
                root = iceprod.core.xml.fromString(xmlstr)
                resource = iceprod.core.xml.getPersonnel(root)
                
                if not resource or not isinstance(resource,iceprod.core.dataclasses.Personnel):
                    raise Exception, 'Bad Personnel class'
                if resource.first_name != 'F':
                    raise Exception, 'Bad first name'
                if resource.last_name != 'L':
                    raise Exception, 'Bad last name'
                if resource.role != 'R':
                    raise Exception, 'Bad role'
                if resource.email != 'e':
                    raise Exception, 'Bad email'
                    
                # bad data
                xmlstr = "<Personnel name='n'><tset>sdf</tset></Personnel>"
                root = iceprod.core.xml.fromString(xmlstr)
                try:
                    resource = iceprod.core.xml.getPersonnel(root)
                except:
                    raise Exception, 'bad data generated an exception intead of ignoring it'
                
                if not resource or not isinstance(resource,iceprod.core.dataclasses.Personnel):
                    raise Exception, 'Bad Personnel class in bad data'
                if resource.first_name != None:
                    raise Exception, 'First name exists when none specified in bad data'
                if resource.last_name != None:
                    raise Exception, 'Last name exists when none specified in bad data'
                if resource.role != None:
                    raise Exception, 'Role exists when none specified in bad data'
                if resource.email != None:
                    raise Exception, 'Email exists when none specified in bad data'
                
        except Exception, e:
            logger.error('Error running getPersonnel test: %s',str(e))
            printer('Test xml.getPersonnel()',False)
            raise
        else:
            printer('Test xml.getPersonnel()')

    def test_10_loadDifPlus(self):
        """Test the loadDifPlus function"""
        try:
            for i in xrange(0,5):
                # good data
                file = os.path.join(self.test_dir,'test_metadata.xml')
                with open(file,'w') as f:
                    f.write(good_metadata)
                resource = iceprod.core.xml.loadDifPlus(file,False)
                
                if not resource or not isinstance(resource,iceprod.core.dataclasses.Job):
                    raise Exception, 'Bad Job class'
                if not resource.difplus or not isinstance(resource.difplus,iceprod.core.dataclasses.DifPlus):
                    raise Exception, 'Bad difplus'
                if not resource.difplus.dif or not isinstance(resource.difplus.dif,iceprod.core.dataclasses.Dif):
                    raise Exception, 'Bad dif'
                if not resource.difplus.plus or not isinstance(resource.difplus.plus,iceprod.core.dataclasses.Plus):
                    raise Exception, 'Bad plus'
                if resource.difplus.dif.entry_id != 'TBD':
                    raise Exception, 'Bad dif.entry_id'
                if resource.difplus.dif.entry_title != 'IC59 triple-coincident CORSIKA':
                    raise Exception, 'Bad dif.entry_title'
                if resource.difplus.dif.parameters != 'SPACE SCIENCE > Astrophysics > Neutrinos':
                    raise Exception, 'Bad dif.parameters'
                if resource.difplus.dif.iso_topic_category != 'geoscientificinformation':
                    raise Exception, 'Bad dif.iso_topic_category'
                if resource.difplus.dif.summary != 'IC59 triple-coincident CORSIKA-in-ice polygonato model with weighted (dslope -1) spectrum of Hoerandel, using <b>AHA07v2_SPICE1_i3coords_cos80</b> photon tables. Angular range of 0deg < theta < 89.99deg and energy range of 600GeV < Eprim < 1e11GeV. ':
                    raise Exception, 'Bad dif.summary'
                if resource.difplus.dif.metadata_name != '[CEOS IDN DIF]':
                    raise Exception, 'Bad dif.metadata_name'
                if resource.difplus.dif.metadata_version != '9.4':
                    raise Exception, 'Bad dif.metadata_version'
                if resource.difplus.dif.sensor_name != 'ICECUBE > IceCube':
                    raise Exception, 'Bad dif.sensor_name'
                if resource.difplus.dif.source_name != 'SIMULATION > Data which are numerically generated':
                    raise Exception, 'Bad dif.source_name'
                if resource.difplus.dif.dif_creation_date != '2011-02-24':
                    raise Exception, 'Bad dif.dif_creation_date'
                if not resource.difplus.dif.data_center or not isinstance(resource.difplus.dif.data_center,iceprod.core.dataclasses.DataCenter):
                    raise Exception, 'Bad dif'
                if resource.difplus.dif.data_center.name != 'UWI-MAD/A3RI > Antarctic Astronomy and Astrophysics Research Institute, University of Wisconsin, Madison':
                    raise Exception, 'Bad dif.data_center.name'
                if not resource.difplus.dif.data_center.personnel or not isinstance(resource.difplus.dif.data_center.personnel,iceprod.core.dataclasses.Personnel):
                    raise Exception, 'Bad dif.data_center.personnel'
                if resource.difplus.dif.data_center.personnel.first_name != None:
                    raise Exception, 'Bad dif.data_center.personnel.first_name'
                if resource.difplus.dif.data_center.personnel.last_name != None:
                    raise Exception, 'Bad dif.data_center.personnel.last_name'
                if resource.difplus.dif.data_center.personnel.role != 'Data Center Contact':
                    raise Exception, 'Bad dif.data_center.personnel.role'
                if resource.difplus.dif.data_center.personnel.email != 'datacenter@icecube.wisc.edu':
                    raise Exception, 'Bad dif.data_center.personnel.email'
                if not resource.difplus.dif.personnel or not isinstance(resource.difplus.dif.data_center.personnel,iceprod.core.dataclasses.Personnel):
                    raise Exception, 'Bad dif.personnel'
                if resource.difplus.dif.personnel.first_name != None:
                    raise Exception, 'Bad dif.personnel.first_name'
                if resource.difplus.dif.personnel.last_name != None:
                    raise Exception, 'Bad dif.personnel.last_name'
                if resource.difplus.dif.personnel.role != 'Data Center Contact':
                    raise Exception, 'Bad dif.personnel.role'
                if resource.difplus.dif.personnel.email != 'datacenter@icecube.wisc.edu':
                    raise Exception, 'Bad dif.personnel.email'
                if resource.difplus.plus.start != '2011-01-01T00:00:01':
                    raise Exception, 'Bad plus.start'
                if resource.difplus.plus.end != '2011-12-31T00:00:00':
                    raise Exception, 'Bad plus.end'
                if resource.difplus.plus.category != 'filtered':
                    raise Exception, 'Bad plus.category'
                if resource.difplus.plus.subcategory != 'CORSIKA-in-ice':
                    raise Exception, 'Bad plus.subcategory'
                if resource.difplus.plus.run_number != '1':
                    raise Exception, 'Bad plus.run_number'
                if resource.difplus.plus.i3db_key != '5359':
                    raise Exception, 'Bad plus.i3db_key'
                if resource.difplus.plus.simdb_key != '5359':
                    raise Exception, 'Bad plus.simdb_key'
                if 'Name' not in resource.difplus.plus.project or resource.difplus.plus.project['Name'] != '1':
                    raise Exception, 'Bad plus.project'
                if resource.difplus.plus.steering_file != 'None':
                    raise Exception, 'Bad plus.steering_file'
                if resource.difplus.plus.log_file != 'None':
                    raise Exception, 'Bad plus.log_file'
                if resource.difplus.plus.command_line != 'None':
                    raise Exception, 'Bad plus.command_line'
                    
                # bad data
                file = os.path.join(self.test_dir,'test_bad_metadata.xml')
                f = open(file,'w')
                f.write('<badroot><DIF></DIF><Plus></Plus></badroot>')
                f.close()
                try:
                    resource = iceprod.core.xml.loadDifPlus(file,False)
                except iceprod.core.xml.XMLException:
                    pass # exception expected
                else:
                    raise Exception, 'bad data did not generate an exception'
                    
                # ok data, test that it ignores the other tags
                file = os.path.join(self.test_dir,'test_ok_metadata.xml')
                f = open(file,'w')
                f.write('<DIF_Plus><DIF><test/></DIF><Plus><test/></Plus><test/></DIF_Plus>')
                f.close()
                resource = iceprod.core.xml.loadDifPlus(file,False)
                
                
        except Exception, e:
            logger.error('Error running loadDifPlus test: %s',str(e))
            printer('Test xml.loadDifPlus()',False)
            raise
        else:
            printer('Test xml.loadDifPlus()')

    def test_15_loadXML(self):
        """Test the loadXML function"""
        try:
            for i in xrange(0,5):
                # good data
                file = os.path.join(self.test_dir,'test_metadata.xml')
                with open(file,'w') as f:
                    f.write(good_configuration)
                resource = iceprod.core.xml.loadXML(file,False)
                
                if not resource or not isinstance(resource,iceprod.core.dataclasses.Job):
                    raise Exception, 'Bad Job class'
                if resource.xml_version != 3.0:
                    raise Exception, 'Bad xml version'
                if resource.iceprod_version != 2.0:
                    raise Exception, 'Bad iceprod version'
                if resource.parent_id != 0:
                    raise Exception, 'Bad parent_id'
                if 'n' not in resource.options or not isinstance(resource.options['n'],iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad options - parameter not present'
                if resource.options['n'].name != 'n' or resource.options['n'].type != 'bool' or resource.options['n'].value != 'True':
                    raise Exception, 'Bad option.parameter'
                if not resource.steering or not isinstance(resource.steering,iceprod.core.dataclasses.Steering):
                    raise Exception, 'Bad steering'
                if 'steering_param' not in resource.steering.parameters or not isinstance(resource.steering.parameters['steering_param'],iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad steering.parameter'
                if resource.steering.parameters['steering_param'].name != 'steering_param' or resource.steering.parameters['steering_param'].type != 'string' or resource.steering.parameters['steering_param'].value != 'sp':
                    raise Exception, 'Bad steering.parameter'
                if 'sp2' not in resource.steering.parameters or not isinstance(resource.steering.parameters['sp2'],iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad steering.parameter 2'
                if resource.steering.parameters['sp2'].name != 'sp2' or resource.steering.parameters['sp2'].type != 'int' or resource.steering.parameters['sp2'].value != '17':
                    raise Exception, 'Bad steering.parameter 2'
                if len(resource.steering.resources) < 1 or resource.steering.resources[0].remote != 'r' or resource.steering.resources[0].local != 'l' or resource.steering.resources[0].compression != 'True':
                    raise Exception, 'Bad steering.resource'
                if len(resource.steering.data) < 1 or resource.steering.data[0].remote != 'r' or resource.steering.data[0].local != 'l' or resource.steering.data[0].compression != 'True' or resource.steering.data[0].type != 'permanent' or resource.steering.data[0].movement != 'both':
                    raise Exception, 'Bad steering.data'
                if 'sys_param' not in resource.steering.system or not isinstance(resource.steering.system['sys_param'],iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad steering.system'
                if resource.steering.system['sys_param'].name != 'sys_param' or resource.steering.system['sys_param'].type != 'str_list' or resource.steering.system['sys_param'].value != '["t1","t2"]':
                    raise Exception, 'Bad steering.system'
                if 'condor' not in resource.steering.batchsys:
                    raise Exception, 'Bad steering.batchsys'
                if 'Requirements' not in resource.steering.batchsys['condor'] or not isinstance(resource.steering.batchsys['condor']['Requirements'],iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad steering.batchsys'
                if resource.steering.batchsys['condor']['Requirements'].name != 'Requirements' or resource.steering.batchsys['condor']['Requirements'].type != 'string' or resource.steering.batchsys['condor']['Requirements'].value != 'Machine = "node001"':
                    raise Exception, 'Bad steering.batchsys'
                    
                if 'task1' not in resource.tasks or not isinstance(resource.tasks['task1'],iceprod.core.dataclasses.Task):
                    raise Exception, 'Bad task'
                task = resource.tasks['task1']
                if len(task.depends) < 2 or '152' not in task.depends or '142' not in task.depends:
                    raise Exception, 'Bad task.depends'
                if 'task_param' not in task.parameters or not isinstance(task.parameters['task_param'],iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad task.parameter'
                if task.parameters['task_param'].name != 'task_param' or task.parameters['task_param'].type != 'double' or task.parameters['task_param'].value != '1.2':
                    raise Exception, 'Bad task.parameter'                
                if 'condor' not in task.batchsys:
                    raise Exception, 'Bad task.batchsys'
                if 'Requirements' not in task.batchsys['condor'] or not isinstance(task.batchsys['condor']['Requirements'],iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad task.batchsys'
                if task.batchsys['condor']['Requirements'].name != 'Requirements' or task.batchsys['condor']['Requirements'].type != 'string' or task.batchsys['condor']['Requirements'].value != 'Machine != "node002"':
                    raise Exception, 'Bad task.batchsys'
                if len(task.resources) < 1 or task.resources[0].remote != 'r' or task.resources[0].local != 'l' or task.resources[0].compression != 'True':
                    raise Exception, 'Bad task.resource'
                if len(task.data) < 1 or task.data[0].remote != 'r' or task.data[0].local != 'l' or task.data[0].compression != 'True' or task.data[0].type != 'permanent' or task.data[0].movement != 'both':
                    raise Exception, 'Bad task.data'
                if len(task.classes) < 1 or task.classes[0].name != 'n' or task.classes[0].src != 's' or task.classes[0].recursive != 'True' or task.classes[0].resource_name != 'r':
                    raise Exception, 'Bad task.class'
                if len(task.projects) < 1 or task.projects[0].name != 'n' or task.projects[0].class_name != 'c':
                    raise Exception, 'Bad task.project'
                
                if 'Corsika' not in task.trays or not isinstance(task.trays['Corsika'],iceprod.core.dataclasses.Tray):
                    raise Exception, 'Bad tray'
                task = task.trays['Corsika']
                if task.iterations != 2:
                    raise Exception, 'Bad tray.iterations'
                if 'tray_param' not in task.parameters or not isinstance(task.parameters['tray_param'],iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad tray.parameter'
                if task.parameters['tray_param'].name != 'tray_param' or task.parameters['tray_param'].type != 'double' or task.parameters['tray_param'].value != '1.2':
                    raise Exception, 'Bad tray.parameter'
                if len(task.resources) < 1 or task.resources[0].remote != 'r' or task.resources[0].local != 'l' or task.resources[0].compression != 'True':
                    raise Exception, 'Bad tray.resource'
                if len(task.data) < 1 or task.data[0].remote != 'r' or task.data[0].local != 'l' or task.data[0].compression != 'True' or task.data[0].type != 'permanent' or task.data[0].movement != 'both':
                    raise Exception, 'Bad tray.data'
                if len(task.classes) < 1 or task.classes[0].name != 'n' or task.classes[0].src != 's' or task.classes[0].recursive != 'True' or task.classes[0].resource_name != 'r':
                    raise Exception, 'Bad tray.class'
                if len(task.projects) < 1 or task.projects[0].name != 'n' or task.projects[0].class_name != 'c':
                    raise Exception, 'Bad tray.project'
                    
                if 'generate_corsika' not in task.modules or not isinstance(task.modules['generate_corsika'],iceprod.core.dataclasses.Module):
                    raise Exception, 'Bad tray'
                task = task.modules['generate_corsika']
                if task.src != 's':
                    raise Exception, 'Bad module.src'
                if task.running_class != 'generators.CorsikaIC':
                    raise Exception, 'Bad module.running_class'
                if 'mod_param' not in task.parameters or not isinstance(task.parameters['mod_param'],iceprod.core.dataclasses.Parameter):
                    raise Exception, 'Bad module.parameter'
                if task.parameters['mod_param'].name != 'mod_param' or task.parameters['mod_param'].type != 'double' or task.parameters['mod_param'].value != '1.2':
                    raise Exception, 'Bad module.parameter'
                if len(task.resources) < 1 or task.resources[0].remote != 'r' or task.resources[0].local != 'l' or task.resources[0].compression != 'True':
                    raise Exception, 'Bad module.resource'
                if len(task.data) < 1 or task.data[0].remote != 'r' or task.data[0].local != 'l' or task.data[0].compression != 'True' or task.data[0].type != 'permanent' or task.data[0].movement != 'both':
                    raise Exception, 'Bad module.data'
                if len(task.classes) < 1 or task.classes[0].name != 'n' or task.classes[0].src != 's' or task.classes[0].recursive != 'True' or task.classes[0].resource_name != 'r':
                    raise Exception, 'Bad module.class'
                if len(task.projects) < 1 or task.projects[0].name != 'n' or task.projects[0].class_name != 'c':
                    raise Exception, 'Bad module.project'
                    
                # bad data
                file = os.path.join(self.test_dir,'test_bad_metadata.xml')
                f = open(file,'w')
                f.write('<badroot parent_id="1" version="2.0" />')
                f.close()
                try:
                    resource = iceprod.core.xml.loadXML(file,False)
                except iceprod.core.xml.XMLException:
                    pass # exception expected
                else:
                    raise Exception, 'bad data did not generate an exception'
                    
                # ok data, with tags that should be ignored
                file = os.path.join(self.test_dir,'test_ok_metadata.xml')
                f = open(file,'w')
                f.write("""
                <configuration parentid="p" version="v" iceprod_version="i">
                <options><test/></options>
                <steering>
                    <test/>
                    <batchsys><test/></batchsys>
                    <system><test/></system>
                </steering>
                <task>
                    <test/>
                    <tray iter='5.1'>
                        <test/>
                        <module class='r'>
                            <test/>
                        </module>
                        <module name='test,test2' class='r' />
                        <module name='module 0' class='r' />
                        <module name='module 5' />
                    </tray>
                    <tray name='test,test2' />
                    <tray name='tray 0' />
                </task>
                <task name='test,test2' />
                <task name='task 0' />
                </configuration>""")
                f.close()
                resource = iceprod.core.xml.loadXML(file,False)
                if not resource or not isinstance(resource,iceprod.core.dataclasses.Job):
                    raise Exception, 'Bad Job class'
                if resource.xml_version != 'v':
                    raise Exception, 'Bad xml version'
                if resource.iceprod_version != 'i':
                    raise Exception, 'Bad iceprod version'
                if resource.parent_id != 'p':
                    raise Exception, 'Bad parent_id'
                if 'task 0' not in resource.tasks:
                    raise Exception, 'Bad task0 name'
                if 'tray 0' not in resource.tasks['task 0'].trays:
                    raise Exception, 'Bad tray0 name'
                if 1 != resource.tasks['task 0'].trays['tray 0'].iterations:
                    raise Exception, 'Bad tray0 iterations'
                if 'module 0' not in resource.tasks['task 0'].trays['tray 0'].modules:
                    raise Exception, 'Bad module0 name'
                if 'module 1' not in resource.tasks['task 0'].trays['tray 0'].modules:
                    raise Exception, 'Bad module1 name'
                if 'module 2' not in resource.tasks['task 0'].trays['tray 0'].modules:
                    raise Exception, 'Bad module2 name'
                if 'module 5' in resource.tasks['task 0'].trays['tray 0'].modules:
                    raise Exception, 'Bad module5 - running_class should be required'
                if 'tray 1' not in resource.tasks['task 0'].trays:
                    raise Exception, 'Bad tray1 name'
                if 'tray 2' not in resource.tasks['task 0'].trays:
                    raise Exception, 'Bad tray2 name'
                if 'task 1' not in resource.tasks:
                    raise Exception, 'Bad task1 name'
                if 'task 2' not in resource.tasks:
                    raise Exception, 'Bad task2 name'
                
        except Exception, e:
            logger.error('Error running loadXML test: %s',str(e))
            printer('Test xml.loadXML()',False)
            raise
        else:
            printer('Test xml.loadXML()')

    def test_20_makeParameter(self):
        """Test the makeParameter function"""
        try:
            for i in xrange(0,5):
                # make root node
                root = etree.Element('root')
                
                # set up parameter
                p = iceprod.core.dataclasses.Parameter()
                p.name = 'n'
                p.value = pickle.dumps('v')
                p.type = 'pickle'
                
                # make parameter
                iceprod.core.xml.makeParameter(root,p)
                
                # check parameter
                if len(root) < 1:
                    raise Exception, 'parameter does not exist in xml'
                if len(root) > 1:
                    raise Exception, 'too many root.children'
                if root[0].tag != 'parameter':
                    raise Exception, 'Bad parameter tag'
                if 'name' not in root[0].attrib or root[0].attrib['name'] != 'n':
                    raise Exception, 'Bad name attribute'
                if 'type' not in root[0].attrib or root[0].attrib['type'] != 'pickle':
                    raise Exception, 'Bad type attribute'
                if pickle.loads(root[0].text) != 'v':
                    raise Exception, 'Bad value attribute'
                if len(root[0].attrib) > 3:
                    raise Exception, 'Too many attributes'
                if root[0].tail:
                    raise Exception, 'param.tail exists'
                
        except Exception, e:
            logger.error('Error running makeParameter test: %s',str(e))
            printer('Test xml.makeParameter()',False)
            raise
        else:
            printer('Test xml.makeParameter()')

    def test_21_makeResource(self):
        """Test the makeResource function"""
        try:
            for i in xrange(0,5):
                # make root node
                root = etree.Element('root')
                
                # set up resource
                p = iceprod.core.dataclasses.Resource()
                p.remote = 'r'
                p.local = 'l'
                p.compression = 'True'
                
                # make resource
                iceprod.core.xml.makeResource(root,p)
                
                # check resource
                if len(root) < 1:
                    raise Exception, 'resource does not exist in xml'
                if len(root) > 1:
                    raise Exception, 'too many root.children'
                if root[0].tag != 'resource':
                    raise Exception, 'Bad resource tag'
                if 'remote' not in root[0].attrib or root[0].attrib['remote'] != 'r':
                    raise Exception, 'Bad remote attribute'
                if 'local' not in root[0].attrib or root[0].attrib['local'] != 'l':
                    raise Exception, 'Bad local attribute'
                if 'compression' not in root[0].attrib or root[0].attrib['compression'] != 'True':
                    raise Exception, 'Bad compression attribute'
                if len(root[0].attrib) > 3:
                    raise Exception, 'Too many attributes'
                if root[0].text:
                    raise Exception, 'resource.text exists'
                if root[0].tail:
                    raise Exception, 'resource.tail exists'
                
        except Exception, e:
            logger.error('Error running makeResource test: %s',str(e))
            printer('Test xml.makeResource()',False)
            raise
        else:
            printer('Test xml.makeResource()')

    def test_22_makeData(self):
        """Test the makeData function"""
        try:
            for i in xrange(0,5):
                # make root node
                root = etree.Element('root')
                
                # set up data
                p = iceprod.core.dataclasses.Data()
                p.remote = 'r'
                p.local = 'l'
                p.compression = 'True'
                p.type = 'permanent'
                p.movement = 'both'
                
                # make data
                iceprod.core.xml.makeData(root,p)
                
                # check data
                if len(root) < 1:
                    raise Exception, 'data does not exist in xml'
                if len(root) > 1:
                    raise Exception, 'too many root.children'
                if root[0].tag != 'data':
                    raise Exception, 'Bad data tag'
                if 'remote' not in root[0].attrib or root[0].attrib['remote'] != 'r':
                    raise Exception, 'Bad remote attribute'
                if 'local' not in root[0].attrib or root[0].attrib['local'] != 'l':
                    raise Exception, 'Bad local attribute'
                if 'compression' not in root[0].attrib or root[0].attrib['compression'] != 'True':
                    raise Exception, 'Bad compression attribute'
                if 'type' not in root[0].attrib or root[0].attrib['type'] != 'permanent':
                    raise Exception, 'Bad type attribute'
                if 'movement' not in root[0].attrib or root[0].attrib['movement'] != 'both':
                    raise Exception, 'Bad movement attribute'
                if len(root[0].attrib) > 5:
                    raise Exception, 'Too many attributes'
                if root[0].text:
                    raise Exception, 'data.text exists'
                if root[0].tail:
                    raise Exception, 'data.tail exists'
                
        except Exception, e:
            logger.error('Error running makeData test: %s',str(e))
            printer('Test xml.makeData()',False)
            raise
        else:
            printer('Test xml.makeData()')

    def test_23_makeClass(self):
        """Test the makeClass function"""
        try:
            for i in xrange(0,5):
                # make root node
                root = etree.Element('root')
                
                # set up class
                p = iceprod.core.dataclasses.Class()
                p.name = 'n'
                p.src = 's'
                p.resource_name = 'r'
                p.recursive = 'True'
                p.libs = 'libs,lib/tools'
                p.env_vars = 'PATH=test'
                
                # make class
                iceprod.core.xml.makeClass(root,p)
                
                # check class
                if len(root) < 1:
                    raise Exception, 'class does not exist in xml'
                if len(root) > 1:
                    raise Exception, 'too many root.children'
                if root[0].tag != 'class':
                    raise Exception, 'Bad class tag'
                if 'name' not in root[0].attrib or root[0].attrib['name'] != 'n':
                    raise Exception, 'Bad name attribute'
                if 'src' not in root[0].attrib or root[0].attrib['src'] != 's':
                    raise Exception, 'Bad src attribute'
                if 'resource_name' not in root[0].attrib or root[0].attrib['resource_name'] != 'r':
                    raise Exception, 'Bad resource_name attribute'
                if 'recursive' not in root[0].attrib or root[0].attrib['recursive'] != 'True':
                    raise Exception, 'Bad recursive attribute'
                if 'libs' not in root[0].attrib or root[0].attrib['libs'] != 'libs,lib/tools':
                    raise Exception, 'Bad libs'
                if 'env_vars' not in root[0].attrib or root[0].attrib['env_vars'] != 'PATH=test':
                    raise Exception, 'Bad env vars'
                if len(root[0].attrib) > 6:
                    raise Exception, 'Too many attributes'
                if root[0].text:
                    raise Exception, 'class.text exists'
                if root[0].tail:
                    raise Exception, 'class.tail exists'
                
        except Exception, e:
            logger.error('Error running makeClass test: %s',str(e))
            printer('Test xml.makeClass()',False)
            raise
        else:
            printer('Test xml.makeClass()')

    def test_24_makeProject(self):
        """Test the makeProject function"""
        try:
            for i in xrange(0,5):
                # make root node
                root = etree.Element('root')
                
                # set up project
                p = iceprod.core.dataclasses.Project()
                p.name = 'n'
                p.class_name = 'c'
                
                # make project
                iceprod.core.xml.makeProject(root,p)
                
                # check project
                if len(root) < 1:
                    raise Exception, 'project does not exist in xml'
                if len(root) > 1:
                    raise Exception, 'too many root.children'
                if root[0].tag != 'project':
                    raise Exception, 'Bad project tag'
                if 'name' not in root[0].attrib or root[0].attrib['name'] != 'n':
                    raise Exception, 'Bad name attribute'
                if 'class' not in root[0].attrib or root[0].attrib['class'] != 'c':
                    raise Exception, 'Bad class attribute'
                if len(root[0].attrib) > 2:
                    raise Exception, 'Too many attributes'
                if root[0].text:
                    raise Exception, 'project.text exists'
                if root[0].tail:
                    raise Exception, 'project.tail exists'
                
        except Exception, e:
            logger.error('Error running makeProject test: %s',str(e))
            printer('Test xml.makeProject()',False)
            raise
        else:
            printer('Test xml.makeProject()')

    def test_25_makePersonnel(self):
        """Test the makePersonnel function"""
        try:
            for i in xrange(0,5):
                # make root node
                root = etree.Element('root')
                
                # set up personnel
                p = iceprod.core.dataclasses.Personnel()
                p.role = 'r'
                p.first_name = 'f'
                p.last_name = 'l'
                p.email = 'e'
                
                # make personnel
                iceprod.core.xml.makePersonnel(root,p)
                
                # check personnel
                if len(root) < 1:
                    raise Exception, 'personnel does not exist in xml'
                if len(root) > 1:
                    raise Exception, 'too many root.children'
                if root[0].tag != 'Personnel':
                    raise Exception, 'Bad personnel tag'
                for n in root[0]:
                    if n.tag == 'Role':
                        if n.text != 'r':
                            raise Exception, 'Bad role subelement'
                    elif n.tag == 'First_Name':
                        if n.text != 'f':
                            raise Exception, 'Bad first name subelement'
                    elif n.tag == 'Last_Name':
                        if n.text != 'l':
                            raise Exception, 'Bad last name subelement'
                    elif n.tag == 'Email':
                        if n.text != 'e':
                            raise Exception, 'Bad email subelement'
                    else:
                        raise Exception, 'unknown subelement'
                if len(root[0]) > 4:
                    raise Exception, 'Too many attributes'
                if root[0].text:
                    raise Exception, 'personnel.text exists'
                if root[0].tail:
                    raise Exception, 'personnel.tail exists'
                
        except Exception, e:
            logger.error('Error running makePersonnel test: %s',str(e))
            printer('Test xml.makePersonnel()',False)
            raise
        else:
            printer('Test xml.makePersonnel()')

    def test_30_todifplus(self):
        """Test the todifplus function"""
        try:
            for i in xrange(0,5):
                # make config
                config = iceprod.core.dataclasses.Job()
                config.difplus = iceprod.core.dataclasses.DifPlus()
                dif = iceprod.core.dataclasses.Dif()
                config.difplus.dif = dif
                plus = iceprod.core.dataclasses.Plus()
                config.difplus.plus = plus
                
                # set up personnel
                p = iceprod.core.dataclasses.Personnel()
                p.role = 'r'
                p.first_name = 'f'
                p.last_name = 'l'
                p.email = 'e'
                
                # make dif
                dif.entry_id = 'eid'
                dif.entry_title = 'etitle'
                dif.parameters = 'param'
                dif.iso_topic_category = 'iso'
                dif.data_center = iceprod.core.dataclasses.DataCenter()
                dif.data_center.name = 'dc_name'
                dif.data_center.personnel = p
                dif.summary = 'sum'
                dif.metadata_name = 'mname'
                dif.metadata_version = 'mver'
                dif.personnel = p
                dif.sensor_name = 'sensorn'
                dif.source_name = 'sourcen'
                dif.dif_creation_date = '2011-06-20'
                
                # make plus
                plus.start = 'st'
                plus.end = 'en'
                plus.category = 'cat'
                plus.subcategory = 'sub'
                plus.run_number = '0'
                plus.i3db_key = 'i3'
                plus.simdb_key = 'sim'
                plus.project['n'] = 'v'
                plus.project['n2'] = 'v2'
                plus.steering_file = 'steer'
                plus.log_file = 'log'
                plus.command_line = 'cmd'
                
                # make difplus output
                tree = iceprod.core.xml.todifplus(config)
                root = tree.getroot()
                
                # check difplus
                if root.tag != 'DIF_Plus':
                    raise Exception, 'Bad root tag'
                if len(root) < 2:
                    raise Exception, 'not enough root.children'
                if len(root) > 2:
                    raise Exception, 'too many root.children'
                for m in root:
                    if m.tag == 'DIF':
                        if len(m) < 9:
                            raise Exception, 'Not enough children'
                        if len(m) > 12:
                            raise Exception, 'Too many children'
                        for n in m:
                            if n.tag == 'Entry_ID':
                                if n.text != 'eid':
                                    raise Exception, 'Bad entry_id subelement'
                            elif n.tag == 'Entry_Title':
                                if n.text != 'etitle':
                                    raise Exception, 'Bad entry_title subelement'
                            elif n.tag == 'Parameters':
                                if n.text != 'param':
                                    raise Exception, 'Bad parameters subelement'
                            elif n.tag == 'ISO_Topic_Category':
                                if n.text != 'iso':
                                    raise Exception, 'Bad iso_topic_category subelement'
                            elif n.tag == 'Data_Center':
                                if len(n) != 2:
                                    raise Exception, 'wrong number of data_center subelements'
                                for o in n:
                                    if o.tag == 'Data_Center_Name':
                                        if o.text != 'dc_name':
                                            raise Exception, 'Bad data_center.name'
                                    elif o.tag == 'Personnel':
                                        for p in o:
                                            if p.tag == 'Role':
                                                if p.text != 'r':
                                                    raise Exception, 'Bad data_center.personnel.role subelement'
                                            elif p.tag == 'First_Name':
                                                if p.text != 'f':
                                                    raise Exception, 'Bad data_center.personnel.first name subelement'
                                            elif p.tag == 'Last_Name':
                                                if p.text != 'l':
                                                    raise Exception, 'Bad data_center.personnel.last name subelement'
                                            elif p.tag == 'Email':
                                                if p.text != 'e':
                                                    raise Exception, 'Bad data_center.personnel.email subelement'
                                            else:
                                                raise Exception, 'unknown data_center.personnel subelement'
                                    else:
                                        raise Exception, 'Bad data_center subelement'
                            elif n.tag == 'Summary':
                                if n.text != 'sum':
                                    raise Exception, 'Bad sumary subelement'
                            elif n.tag == 'Metadata_Name':
                                if n.text != 'mname':
                                    raise Exception, 'Bad metadata_name subelement'
                            elif n.tag == 'Metadata_Version':
                                if n.text != 'mver':
                                    raise Exception, 'Bad metadata_version subelement'
                            elif n.tag == 'Personnel':
                                for p in n:
                                    if p.tag == 'Role':
                                        if p.text != 'r':
                                            raise Exception, 'Bad data_center.personnel.role subelement'
                                    elif p.tag == 'First_Name':
                                        if p.text != 'f':
                                            raise Exception, 'Bad data_center.personnel.first name subelement'
                                    elif p.tag == 'Last_Name':
                                        if p.text != 'l':
                                            raise Exception, 'Bad data_center.personnel.last name subelement'
                                    elif p.tag == 'Email':
                                        if p.text != 'e':
                                            raise Exception, 'Bad data_center.personnel.email subelement'
                                    else:
                                        raise Exception, 'unknown data_center.personnel subelement'
                            elif n.tag == 'Sensor_Name':
                                if n.text != 'sensorn':
                                    raise Exception, 'Bad sensor name subelement'
                            elif n.tag == 'Source_Name':
                                if n.text != 'sourcen':
                                    raise Exception, 'Bad source name subelement'
                            elif n.tag == 'DIF_Creation_Date':
                                if n.text != '2011-06-20':
                                    raise Exception, 'Bad creation date subelement'
                            else:
                                raise Exception, 'unknown subelement'
                        if m.text:
                            raise Exception, 'DIF.text exists'
                        if m.tail:
                            raise Exception, 'DIF.tail exists'
                    elif m.tag == 'Plus':
                        for n in m:
                            if n.tag == 'Start_DateTime':
                                if n.text != 'st':
                                    raise Exception, 'Bad start_datetime subelement'
                            elif n.tag == 'End_DateTime':
                                if n.text != 'en':
                                    raise Exception, 'Bad end_datetime subelement'
                            elif n.tag == 'Category':
                                if n.text != 'cat':
                                    raise Exception, 'Bad category subelement'
                            elif n.tag == 'Subcategory':
                                if n.text != 'sub':
                                    raise Exception, 'Bad subcategory subelement'
                            elif n.tag == 'Run_Number':
                                if n.text != '0':
                                    raise Exception, 'Bad run number subelement'
                            elif n.tag == 'I3Db_Key':
                                if n.text != 'i3':
                                    raise Exception, 'Bad i3db key subelement'
                            elif n.tag == 'SimDb_Key':
                                if n.text != 'sim':
                                    raise Exception, 'Bad simdb key subelement'
                            elif n.tag == 'Steering_File':
                                if n.text != 'steer':
                                    raise Exception, 'Bad steering file subelement'
                            elif n.tag == 'Log_File':
                                if n.text != 'log':
                                    raise Exception, 'Bad log file subelement'
                            elif n.tag == 'Command_Line':
                                if n.text != 'cmd':
                                    raise Exception, 'Bad command line subelement'
                            elif n.tag == 'Project':
                                for o in n:
                                    if o.tag == 'Name':
                                        if o.text[0] != 'n':
                                            raise Exception, 'Bad project.name subelement'
                                    elif o.tag == 'Version':
                                        if o.text[0] != 'v':
                                            raise Exception, 'Bad project.version subelement'
                                    else:
                                        raise Exception, 'unknown project subelement'
                            else:
                                raise Exception, 'unknown subelement'
                        if m.text:
                            raise Exception, 'DIF.text exists'
                        if m.tail:
                            raise Exception, 'DIF.tail exists'
                    else:
                        raise Exception, 'Bad DIF_Plus child tag'
                        
                # make bad config
                config = iceprod.core.dataclasses.Job()
                try:
                    root = iceprod.core.xml.todifplus(config)
                except iceprod.core.xml.XMLException, e:
                    if str(e) != 'No Dif Plus in this config':
                        raise
                else:
                    raise Exception, 'Bad config did not raise an exception'
                
        except Exception, e:
            logger.error('Error running todifplus test: %s',str(e))
            printer('Test xml.todifplus()',False)
            raise
        else:
            printer('Test xml.todifplus()')

    def test_31_toDifPlusstring(self):
        """Test the toDifPlusstring function"""
        try:
            for i in xrange(0,5):
                # get config from file
                file = os.path.join(self.test_dir,'test_metadata.xml')
                with open(file,'w') as f:
                    f.write(good_metadata)
                config = iceprod.core.xml.loadDifPlus(file,False)
                
                # output to string
                xml = iceprod.core.xml.toDifPlusstring(config)
                
                # compare to file
                orig_xml = ''
                f = open(file,'r')
                for l in f:
                    l = l.strip()
                    orig_xml += l                
                f.close()
                
                ret = cmpXML(iceprod.core.xml.fromString(xml),iceprod.core.xml.fromString(orig_xml))
                if ret:
                    raise Exception, 'xml does not match original: %s'%ret
                    
                # bad case
                config = iceprod.core.dataclasses.Job()
                try:
                    iceprod.core.xml.toDifPlusstring(config)
                except Exception, e:
                    pass # exception expected
                else:
                    raise Exception, 'Bad case did not raise exception'             
                
        except Exception, e:
            logger.error('Error running toDifPlusstring test: %s',str(e))
            printer('Test xml.toDifPlusstring()',False)
            raise
        else:
            printer('Test xml.toDifPlusstring()')

    def test_32_writeDifPlus(self):
        """Test the writeDifPlus function"""
        try:
            for i in xrange(0,5):
                # get config from file
                file = os.path.join(self.test_dir,'test_metadata.xml')
                with open(file,'w') as f:
                    f.write(good_metadata)
                config = iceprod.core.xml.loadDifPlus(file,False)
                
                # test file output
                file2 = os.path.join(self.test_dir,'test_metadata2.xml')
                iceprod.core.xml.writeDifPlus(file2,config,False)
                
                # compare to file
                orig_xml = ''
                f = open(file,'r')
                for l in f:
                    l = l.strip()
                    orig_xml += l                
                f.close()
                xml = ''
                f = open(file2,'r')
                for l in f:
                    l = l.strip()
                    xml += l                
                f.close()
                
                ret = cmpXML(iceprod.core.xml.fromString(xml),iceprod.core.xml.fromString(orig_xml))
                if ret:
                    raise Exception, 'xml does not match original: %s'%ret
                
                # test pretty print
                file3 = os.path.join(self.test_dir,'test_metadata3.xml')
                iceprod.core.xml.writeDifPlus(file3,config,True)
                
                # compare to file
                orig_xml = ''
                f = open(file,'r')
                for l in f:
                    l = l.strip()
                    orig_xml += l                
                f.close()
                xml = ''
                f = open(file3,'r')
                for l in f:
                    l = l.strip()
                    xml += l                
                f.close()
                
                ret = cmpXML(iceprod.core.xml.fromString(xml),iceprod.core.xml.fromString(orig_xml))
                if ret:
                    raise Exception, 'pretty xml does not match original: %s'%ret
                    
                # bad case
                file4 = os.path.join(self.test_dir,'test_configuration4.xml')
                config = iceprod.core.dataclasses.Job()
                try:
                    iceprod.core.xml.writeDifPlus(file4,config,True)
                except Exception, e:
                    pass # exception expected
                else:
                    raise Exception, 'Bad case did not raise exception'                
                
        except Exception, e:
            logger.error('Error running writeDifPlus test: %s',str(e))
            printer('Test xml.writeDifPlus()',False)
            raise
        else:
            printer('Test xml.writeDifPlus()')

    def test_35_toxml(self):
        """Test the toxml function"""
        try:
            for i in xrange(0,5):
                # make config
                config = iceprod.core.dataclasses.Job()
                steering = iceprod.core.dataclasses.Steering()
                config.steering = steering
                
                # set up parameter
                p = iceprod.core.dataclasses.Parameter()
                p.name = 'n'
                p.value = 'v'
                p.type = 'string' 
                
                # set up resource
                r = iceprod.core.dataclasses.Resource()
                r.remote = 'r'
                r.local = 'l'
                r.compression = 'True'
                
                # set up data
                d = iceprod.core.dataclasses.Data()
                d.remote = 'r'
                d.local = 'l'
                d.compression = 'True'
                d.type = 'permanent'
                d.movement = 'both'
                
                # set up project
                proj = iceprod.core.dataclasses.Project()
                proj.name = 'n'
                proj.class_name = 'c'
                
                # set up class
                c = iceprod.core.dataclasses.Class()
                c.name = 'n'
                c.src = 's'
                c.resource_name = 'r'
                c.recursive = 'True'
                
                # set up options                
                config.options[p.name] = p
                
                # set up steering
                steering.parameters[p.name] = p
                steering.resources.append(r)      
                steering.data.append(d)
                steering.batchsys['condor'] = {p.name: p}
                steering.system[p.name] = p
                
                # set up task
                task = iceprod.core.dataclasses.Task()
                task.name = 'task1'
                task.depends.append('0')
                task.parameters[p.name] = p
                task.resources.append(r)      
                task.data.append(d)
                task.projects.append(proj)
                task.classes.append(c)
                config.tasks[task.name] = task
                
                # set up tray
                tray = iceprod.core.dataclasses.Tray()
                tray.name = 'tray1'
                tray.parameters[p.name] = p
                tray.resources.append(r)      
                tray.data.append(d)
                tray.projects.append(proj)
                tray.classes.append(c)
                task.trays[tray.name] = tray
                
                # set up module
                mod = iceprod.core.dataclasses.Module()
                mod.name = 'tray1'
                mod.running_class = 'rc'
                mod.src = 'src'
                mod.parameters[p.name] = p
                mod.resources.append(r)      
                mod.data.append(d)
                mod.projects.append(proj)
                mod.classes.append(c)
                tray.modules[mod.name] = mod
                
                # make config output
                tree = iceprod.core.xml.toxml(config)
                root = tree.getroot()
                
                # check config
                if root.tag != 'configuration':
                    raise Exception, 'Bad root tag'
                for m in root:
                    if m.tag == 'options':
                        if len(m) < 1 or m[0].tag != 'parameter' or m[0].attrib['name'] != p.name or m[0].attrib['value'] != p.value or m[0].attrib['type'] != p.type:
                            raise Exception, 'Bad options parameter'
                        if m.text:
                            raise Exception, 'options.text exists'
                        if m.tail:
                            raise Exception, 'options.tail exists'
                    elif m.tag == 'steering':
                        for n in m:
                            if n.tag == 'parameter':
                                if n.attrib['name'] != p.name or n.attrib['value'] != p.value or n.attrib['type'] != p.type:
                                    raise Exception, 'Bad steering parameter'
                            elif n.tag == 'resource':
                                if n.attrib['remote'] != r.remote or n.attrib['local'] != r.local or n.attrib['compression'] != r.compression:
                                    raise Exception, 'Bad steering resource'
                            elif n.tag == 'data':
                                if n.attrib['remote'] != d.remote or n.attrib['local'] != d.local or n.attrib['compression'] != d.compression or n.attrib['type'] != d.type or n.attrib['movement'] != d.movement:
                                    raise Exception, 'Bad steering data'
                            if n.tag == 'batchsys':
                                if n.attrib['name'] != 'condor':
                                    raise Exception, 'Bad batchsys name'
                                if len(m) < 1 or n[0].tag != 'parameter' or n[0].attrib['name'] != p.name or n[0].attrib['value'] != p.value or n[0].attrib['type'] != p.type:
                                    raise Exception, 'Bad batchsys parameter'
                            if n.tag == 'system':
                                if len(m) < 1 or n[0].tag != 'parameter' or n[0].attrib['name'] != p.name or n[0].attrib['value'] != p.value or n[0].attrib['type'] != p.type:
                                    raise Exception, 'Bad batchsys parameter'
                    elif m.tag == 'task':
                        if len(m) < 1 or m.attrib['name'] != task.name or m.attrib['depends'] != '0':
                            raise Exception, 'Bad task'
                        for n in m:
                            if n.tag == 'parameter':
                                if n.attrib['name'] != p.name or n.attrib['value'] != p.value or n.attrib['type'] != p.type:
                                    raise Exception, 'Bad task parameter'
                            elif n.tag == 'resource':
                                if n.attrib['remote'] != r.remote or n.attrib['local'] != r.local or n.attrib['compression'] != r.compression:
                                    raise Exception, 'Bad task resource'
                            elif n.tag == 'data':
                                if n.attrib['remote'] != d.remote or n.attrib['local'] != d.local or n.attrib['compression'] != d.compression or n.attrib['type'] != d.type or n.attrib['movement'] != d.movement:
                                    raise Exception, 'Bad task data'
                            elif n.tag == 'project':
                                if n.attrib['name'] != proj.name or n.attrib['class'] != proj.class_name:
                                    raise Exception, 'Bad task project'
                            elif n.tag == 'class':
                                if n.attrib['name'] != c.name or n.attrib['src'] != c.src or n.attrib['resource_name'] != c.resource_name or n.attrib['recursive'] != c.recursive:
                                    raise Exception, 'Bad task class'
                            elif n.tag == 'tray':
                                if n.attrib['name'] != tray.name or n.attrib['iter'] != '1':
                                    raise Exception, 'Bad tray'
                                for o in n:
                                    if o.tag == 'parameter':
                                        if o.attrib['name'] != p.name or o.attrib['value'] != p.value or o.attrib['type'] != p.type:
                                            raise Exception, 'Bad tray parameter'
                                    elif o.tag == 'resource':
                                        if o.attrib['remote'] != r.remote or o.attrib['local'] != r.local or o.attrib['compression'] != r.compression:
                                            raise Exception, 'Bad tray resource'
                                    elif o.tag == 'data':
                                        if o.attrib['remote'] != d.remote or o.attrib['local'] != d.local or o.attrib['compression'] != d.compression or o.attrib['type'] != d.type or o.attrib['movement'] != d.movement:
                                            raise Exception, 'Bad tray data'
                                    elif o.tag == 'project':
                                        if o.attrib['name'] != proj.name or o.attrib['class'] != proj.class_name:
                                            raise Exception, 'Bad tray project'
                                    elif o.tag == 'class':
                                        if o.attrib['name'] != c.name or o.attrib['src'] != c.src or o.attrib['resource_name'] != c.resource_name or o.attrib['recursive'] != c.recursive:
                                            raise Exception, 'Bad tray class'
                                    elif o.tag == 'module':
                                        if o.attrib['name'] != mod.name or o.attrib['class'] != mod.running_class or o.attrib['src'] != mod.src:
                                            raise Exception, 'Bad module'
                                        for q in o:
                                            if q.tag == 'parameter':
                                                if q.attrib['name'] != p.name or q.attrib['value'] != p.value or q.attrib['type'] != p.type:
                                                    raise Exception, 'Bad tray parameter'
                                            elif q.tag == 'resource':
                                                if q.attrib['remote'] != r.remote or q.attrib['local'] != r.local or q.attrib['compression'] != r.compression:
                                                    raise Exception, 'Bad tray resource'
                                            elif q.tag == 'data':
                                                if q.attrib['remote'] != d.remote or q.attrib['local'] != d.local or q.attrib['compression'] != d.compression or q.attrib['type'] != d.type or q.attrib['movement'] != d.movement:
                                                    raise Exception, 'Bad tray data'
                                            elif q.tag == 'project':
                                                if q.attrib['name'] != proj.name or q.attrib['class'] != proj.class_name:
                                                    raise Exception, 'Bad tray project'
                                            elif q.tag == 'class':
                                                if q.attrib['name'] != c.name or q.attrib['src'] != c.src or q.attrib['resource_name'] != c.resource_name or q.attrib['recursive'] != c.recursive:
                                                    raise Exception, 'Bad tray class'
                                            else:
                                                raise Exception, 'unknown tag in module: %s'%p.tag
                                    else:
                                        raise Exception, 'unknown tag in tray: %s'%o.tag
                            else:
                                raise Exception, 'unknown tag in task: %s'%n.tag
                    else:
                        raise Exception, 'Bad configuration child tag: %s'%m.tag
                
        except Exception, e:
            logger.error('Error running toxml test: %s',str(e))
            printer('Test xml.toxml()',False)
            raise
        else:
            printer('Test xml.toxml()')

    def test_36_toXMLstring(self):
        """Test the toXMLstring function"""
        try:
            for i in xrange(0,5):
                # get config from file
                file = os.path.join(self.test_dir,'test_configuration.xml')
                with open(file,'w') as f:
                    f.write(good_configuration)
                config = iceprod.core.xml.loadXML(file,False)
                
                # output to string
                xml = iceprod.core.xml.toXMLstring(config)
                
                # compare to file
                orig_xml = ''
                f = open(file,'r')
                for l in f:
                    l = l.strip()
                    orig_xml += l                
                f.close()
                
                ret = cmpXML(iceprod.core.xml.fromString(xml),iceprod.core.xml.fromString(orig_xml))
                if ret:
                    raise Exception, 'xml does not match original: %s'%ret
                
                # bad case
                config = iceprod.core.dataclasses.Job()
                config.options['test'] = 'error'
                try:
                    xml = iceprod.core.xml.toXMLstring(config)
                except Exception, e:
                    pass # exception expected
                else:
                    raise Exception, 'Bad case did not raise exception'  
                
        except Exception, e:
            logger.error('Error running toXMLstring test: %s',str(e))
            printer('Test xml.toXMLstring()',False)
            raise
        else:
            printer('Test xml.toXMLstring()')

    def test_37_writeXML(self):
        """Test the writeXML function"""
        try:
            for i in xrange(0,5):
                # get config from file
                file = os.path.join(self.test_dir,'test_configuration.xml')
                with open(file,'w') as f:
                    f.write(good_configuration)
                config = iceprod.core.xml.loadXML(file,False)
                
                # test file output
                file2 = os.path.join(self.test_dir,'test_configuration2.xml')
                iceprod.core.xml.writeXML(file2,config,False)
                
                # compare to file
                orig_xml = ''
                f = open(file,'r')
                for l in f:
                    l = l.strip()
                    orig_xml += l
                f.close()
                xml = ''
                f = open(file2,'r')
                for l in f:
                    l = l.strip()
                    xml += l
                f.close()
                
                ret = cmpXML(iceprod.core.xml.fromString(xml),iceprod.core.xml.fromString(orig_xml))
                if ret:
                    raise Exception, 'xml does not match original: %s'%ret
                
                # test pretty print
                file3 = os.path.join(self.test_dir,'test_configuration3.xml')
                iceprod.core.xml.writeXML(file3,config,True)
                
                # compare to file
                orig_xml = ''
                f = open(file,'r')
                for l in f:
                    l = l.strip()
                    orig_xml += l                
                f.close()
                xml = ''
                f = open(file3,'r')
                for l in f:
                    l = l.strip()
                    xml += l                
                f.close()
                
                ret = cmpXML(iceprod.core.xml.fromString(xml),iceprod.core.xml.fromString(orig_xml))
                if ret:
                    raise Exception, 'pretty xml does not match original: %s'%ret
                    
                # bad case
                file4 = os.path.join(self.test_dir,'test_configuration4.xml')
                config = iceprod.core.dataclasses.Job()
                config.options['test'] = 'error'
                try:
                    iceprod.core.xml.writeXML(file4,config,True)
                except Exception, e:
                    pass # exception expected
                else:
                    raise Exception, 'Bad case did not raise exception'                
                
        except Exception, e:
            logger.error('Error running writeXML test: %s',str(e))
            printer('Test xml.writeXML()',False)
            raise
        else:
            printer('Test xml.writeXML()')

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    alltests = glob_tests(loader.getTestCaseNames(xml_test))
    suite.addTests(loader.loadTestsFromNames(alltests,xml_test))
    return suite
