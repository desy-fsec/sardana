#!/bin/env python
""" NeXus components setting """

import PyTango
import re
import json
import xml.dom.minidom
from sardana.macroserver.macro import iMacro, Macro, Type, imacro, macro


class nxs_list_settings(Macro):
    """ Lists avaliable components """

    def __printDict(self, name):
        self.output("%s:" % name)
        try:
            data = dict(self.getEnv(name))
            for dt in data:
                self.output("  '%s': %s" % (dt, data[dt]))
        except:
            pass


    def __printList(self, name):
        self.output("%s:" % name)
        try:
            data = list(self.getEnv(name))
            self.output("  %s" % str(data))
        except:
            pass

    def __printString(self, name, default = ""):
        try:
            string = self.getEnv(name)
            de = ''
        except:
            de = default
            string = None
        self.output("%s: %s %s" % (name, string, str(de)))
        

    def run(self):
        db = PyTango.Database()

        self.output("")
        self.__printString("NeXusConfigDevice",
                           db.get_device_exported_for_class(
                "NXSConfigServer").value_string )

        self.__printString("NeXusWriterDevice",
                           db.get_device_exported_for_class(
                "NXSDataWriter").value_string )

        self.output("")
        self.__printList("NeXusComponents")
        self.output("")
        self.__printDict("NeXusDataRecord")

        self.output("")
        self.__printList("NeXusDataSources")
        
        self.output("")
        self.__printString("NeXusAppendEntry", '[False]')
        self.__printString("NeXusComponentsFromMntGrp", '[False]')

        self.output("")
        self.__printString("NeXusDynamicComponents", '[True]') 
        self.__printString("NeXusDynamicLinks", '[True]')
        self.__printString(
            "NeXusDynamicPath", 
            '[/entry$var.serialno:NXentry/NXinstrument/NXcollection]')
        self.output("")
        self.__printDict("NeXusConfigVariables")


        self.output("")
        self.__printString("ScanFile")
        self.__printString("ScanDir")
        self.__printString("ScanID")
        self.output("")
        self.__printString("ActiveMntGrp")
        self.output("")
        self.__printString("timezone",'[Europe/Berlin]')




class nxs_list_server_settings(Macro):
    """ Lists avaliable components """

    def __printDict(self, name, decode=True):
        self.output("%s:" % name)
        try:
            data = self.__nexussettings_device.read_attribute(name).value
            if decode:
                data = json.loads(data) 
        except:
            pass
        self.output("  %s" % str(data))


    def __printList(self, name, decode=True):
        self.output("%s:" % name)
        try:
            data = self.__nexussettings_device.read_attribute(name).value
            if decode:
                data = json.loads(data) 
        except:
            pass
        self.output("  %s" % str(data))

    def __printString(self, name):
        string = self.__nexussettings_device.read_attribute(name).value
        self.output("%s: %s" % (name, string))
        

    def run(self):
        db = PyTango.Database()

        self.output("")   
        try:
            servers = [self.getEnv("NeXusSettingsDevice")]
        except:
            servers = db.get_device_exported_for_class(
                "NXSRecSettings").value_string 
        if len(servers) > 0:
            self.__nexussettings_device = PyTango.DeviceProxy(servers[0])
            self.output("SettingsDevice: %s" % str(servers[0]))
        
        

        self.output("")
        self.__printString("ConfigDevice")

        self.__printString("WriterDevice")

        self.output("") 
        self.__printList("Components", True)
        self.output("")
        self.__printDict("DataRecord" , True)

        self.output("")
        self.__printList("DataSources", True)
        
        self.output("")
        self.__printString("AppendEntry")
        self.__printString("ComponentsFromMntGrp")

        self.output("")
        self.__printString("DynamicComponents") 
        self.__printString("DynamicLinks")
        self.__printString("DynamicPath")
        self.output("")
        self.__printDict("ConfigVariables", True)
        self.output("")
        self.__printString("TimeZone")


class nxs_components(Macro):
    """ Lists avaliable components """
    def run(self):
        db = PyTango.Database()
        try:
            servers = [self.getEnv("NeXusSettingsDevice")]
        except:   
            servers = db.get_device_exported_for_class(
                "NXSRecSettings").value_string 

        if len(servers) > 0:
            self.__nexussetting_device = PyTango.DeviceProxy(servers[0])
            self.output("Setting Server: %s" % servers[0])
            servers = [self.__nexussetting_device.ConfigDevice]

        else:
            try:
                servers = [self.getEnv("NeXusConfigDevice")]
            except:   
                servers = db.get_device_exported_for_class(
                    "NXSConfigServer").value_string 
        if len(servers) > 0:
            self.output("Configuration Server: %s" % servers[0])
            nexusconfig_device = PyTango.DeviceProxy(servers[0])
            nexusconfig_device.Open()
            cps = nexusconfig_device.AvailableComponents()  
            mand = nexusconfig_device.MandatoryComponents()
            nomand = list(set(cps) - set(mand))
        
            self.output("Mandatory Components: %s" % str(sorted(mand)))
            self.output("Other Components: %s" % str(sorted(nomand)))



class nxs_datasources(Macro):
    """ Lists avaliable datasources """
    def run(self):
        db = PyTango.Database()
        try:
            servers = [self.getEnv("NeXusSettingsDevice")]
        except:   
            servers = db.get_device_exported_for_class(
                "NXSRecSettings").value_string 

        if len(servers) > 0:
            self.__nexussetting_device = PyTango.DeviceProxy(servers[0])
            self.output("Setting Server: %s" % servers[0])
            servers = [self.__nexussetting_device.ConfigDevice]

        else:
            try:
                servers = [self.getEnv("NeXusConfigDevice")]
            except:   
                servers = db.get_device_exported_for_class(
                    "NXSConfigServer").value_string 
        if len(servers) > 0:
            nexusconfig_device = PyTango.DeviceProxy(servers[0])
            nexusconfig_device.Open()
            dss = nexusconfig_device.AvailableDataSources()  
            ds = list(set(dss))

            self.output("Configuration Server: %s" % servers[0])
            self.output("DataSources: %s" % str(sorted(ds)))



class nxs_component_describe(Macro):
    """ Lists datasources of given component"""

    param_def = [
        ['component', Type.String, '', 'component name']  
        ]

    def run(self, component):
        db = PyTango.Database()
        try:
            servers = [self.getEnv("NeXusSettingsDevice")]
        except:   
            servers = db.get_device_exported_for_class(
                "NXSRecSettings").value_string 

        if len(servers) > 0:
            self.__nexussetting_device = PyTango.DeviceProxy(servers[0])
            self.output("Setting Server: %s" % servers[0])
            servers = [self.__nexussetting_device.ConfigDevice]

        else:
            try:
                servers = [self.getEnv("NeXusConfigDevice")]
            except:   
                servers = db.get_device_exported_for_class(
                    "NXSConfigServer").value_string 
        if len(servers) > 0:
            nexusconfig_device = PyTango.DeviceProxy(servers[0])
            nexusconfig_device.Open()
            cps = nexusconfig_device.AvailableComponents()  
            self.output("Configuration Server: %s" % servers[0])

            if component:
                self.output("Component:")
                if component in cps:
                    dss = nexusconfig_device.ComponentDataSources(component)  
                    self.output("%s: %s" % (component, str(dss)))
            else:
                mand = nexusconfig_device.MandatoryComponents()
                nomand = list(set(cps)- set(mand))
        
                self.output("Mandatory Components:")
                for cp in mand:
                    dss = nexusconfig_device.ComponentDataSources(cp)  
                    self.output("%s: %s" % (cp, str(dss)))

                self.output("Other Components:")
                for cp in nomand:
                    dss = nexusconfig_device.ComponentDataSources(cp)  
                    self.output("%s: %s" % (cp, str(dss)))




class nxs_component_xml(Macro):
    """ Shows component xml"""

    param_def = [
        ['component', Type.String, '', 'component name']  
        ]

    def run(self, component):
        db = PyTango.Database()
        try:
            servers = [self.getEnv("NeXusSettingsDevice")]
        except:   
            servers = db.get_device_exported_for_class(
                "NXSRecSettings").value_string 

        if len(servers) > 0:
            self.__nexussetting_device = PyTango.DeviceProxy(servers[0])
            self.output("Setting Server: %s" % servers[0])
            servers = [self.__nexussetting_device.ConfigDevice]

        else:
            try:
                servers = [self.getEnv("NeXusConfigDevice")]
            except:   
                servers = db.get_device_exported_for_class(
                    "NXSConfigServer").value_string 
        if len(servers) > 0:
            nexusconfig_device = PyTango.DeviceProxy(servers[0])
            nexusconfig_device.Open()
            cps = nexusconfig_device.AvailableComponents()  

            if component in cps:
                xmls = nexusconfig_device.Components([component])
                self.output("Configuration Server: %s" % servers[0])
                self.output("Component:\n%s" % str(xmls[0]))




class nxs_datasource_xml(Macro):
    """ Shows datasource xml"""

    param_def = [
        ['datasource', Type.String, '', 'datasource name']  
        ]

    def run(self, datasource):
        db = PyTango.Database()
        try:
            servers = [self.getEnv("NeXusSettingsDevice")]
        except:   
            servers = db.get_device_exported_for_class(
                "NXSRecSettings").value_string 

        if len(servers) > 0:
            self.__nexussetting_device = PyTango.DeviceProxy(servers[0])
            self.output("Setting Server: %s" % servers[0])
            servers = [self.__nexussetting_device.ConfigDevice]

        else:
            try:
                servers = [self.getEnv("NeXusConfigDevice")]
            except:   
                servers = db.get_device_exported_for_class(
                    "NXSConfigServer").value_string 

        if len(servers) > 0:
            nexusconfig_device = PyTango.DeviceProxy(servers[0])
            nexusconfig_device.Open()
            dss = nexusconfig_device.AvailableDataSources()  

            if datasource in dss:
                xmls = nexusconfig_device.DataSources([datasource])
                self.output("Configuration Server: %s" % servers[0])
                self.output("DataSource:\n%s" % str(xmls[0]))



class nxs_component_describe_full(Macro):
    """ Lists datasources, strategy and dstype of given component """

    
    param_def = [
        ['component', Type.String, '', 
         'component name [default \'\' for all]'],
        ['strategy', Type.String, '', 
         'strategy mode filter [default \'\' for all]'],
        ['dstype', Type.String, '', 
         'datasource type filter [default \'\' for all]'],  
        ['env_components', Type.Boolean, False, 
         'lists components from the NeXusComponents '\
             +'environment variable [default False]']
        ]

    
    def __checkNode(self, node):
        label = 'datasources'
        dstype = None
        name = None
        if node.nodeName == 'datasource':
            if node.hasAttribute("type"):
                dstype  = node.attributes["type"].value
            if node.hasAttribute("name"):
                name = node.attributes["name"].value
#            self.output("DSNODE: %s, %s", name, dstype)        

        elif node.nodeType == node.TEXT_NODE:
            dstxt = node.data
#            self.output("TXT: '%s'" % dstxt)
            index = dstxt.find("$%s." % label)
#            self.output("dstxt:\n  %s"  % (dstxt))
#            self.output("DS0: %s %s"  % (name,index))
            while index != -1 and not dstype:
                try:
                    subc = re.finditer(
                        r"[\w]+", 
                        dstxt[(index+len(label)+2):]).next().group(0)
                except Exception:
#                    self.output("EXC: %s" % str(e))
                    subc = ''
                name = subc.strip() if subc else ""
                try:
                    dsource = self.__nexusconfig_device.DataSources([str(name)])
                except:
                    dsource = []
#                self.output("DS: %s %s"  % (name,str(dsource)))
                if len(dsource)>0:
                    indom = xml.dom.minidom.parseString(dsource[0])
                    dss = indom.getElementsByTagName("datasource")
                    for ds in dss:
                        if ds.nodeName == 'datasource':
                            if ds.hasAttribute("type"):
                                dstype  = ds.attributes["type"].value
                            if ds.hasAttribute("name"):
                                name = ds.attributes["name"].value
                index = dstxt.find("$%s." % label, index+1)
#                self.output("DSTXT: %s, %s", name, dstype)        
        return name, dstype
                

    def __appendNode(self, node, dss, mode, counter): 
#        self.output("NODE: %s" % node.nodeName)
        prefix = '__unnamed__'
        name, dstype = self.__checkNode(node)
        if name:
            if name not in dss:
                dss[name] = [] 
            dss[name].append((str(mode), str(dstype) if dstype else None))
        elif node.nodeName == 'datasource':
            name = prefix + str(counter) 
            while name in dss.keys():
                name = prefix + str(counter) 
                counter = counter + 1
            dss[name] = [] 
            dss[name].append((str(mode), str(dstype) if dstype else None))
        
        counter = counter +1
        return (name, counter)

    def __getDataSourceAttributes(self, cp):         
        dss = {}
        xmlc = self.__nexusconfig_device.Components([cp])
        names = []
        if not len(xmlc)>0:
            return names
        indom = xml.dom.minidom.parseString(xmlc[0])
        strategy = indom.getElementsByTagName("strategy")
        counter = 1

        for sg in strategy:
            if sg.hasAttribute("mode"):
                mode = sg.attributes["mode"].value
                name = None
                nxt = sg.nextSibling
                while nxt and not name:
                    name, counter = self.__appendNode(nxt, dss, mode, counter)
                    nxt = nxt.nextSibling    

                prev = sg.previousSibling
                while prev and not name:
                    name, counter = self.__appendNode(prev, dss, mode, counter)
                    prev = prev.previousSibling  
        return dss

    def prepare(self, component, strategy, dstype, env_components):
        self.__result = [{}, {}]
        self.silent = False

    def run(self, component, strategy, dstype, env_components):
        self.__nexussetting_device = None
        db = PyTango.Database()
        try:
            servers = [self.getEnv("NeXusSettingsDevice")]
        except:   
            servers = db.get_device_exported_for_class(
                "NXSRecSettings").value_string 

        if len(servers) > 0:
            self.__nexussetting_device = PyTango.DeviceProxy(servers[0])
            self.output("Setting Server: %s" % servers[0])
            servers = [self.__nexussetting_device.ConfigDevice]

        else:
            try:
                servers = [self.getEnv("NeXusConfigDevice")]
            except:   
                servers = db.get_device_exported_for_class(
                    "NXSConfigServer").value_string 

        if len(servers) > 0:
            self.__nexusconfig_device = PyTango.DeviceProxy(servers[0])
            self.__nexusconfig_device.Open()
            if env_components:
                if self.__nexussetting_device:
                    if self.__nexussetting_device.Components:
                        cps = self.__nexussetting_device.Components
                    else:
                        cps = []
                else:
                    try:
                        cps = self.getEnv("NeXusComponents")
                    except:
                        cps = set()
            elif component:
                cpp = self.__nexusconfig_device.AvailableComponents()  
                cps = [component] if component in cpp else []
               
            else:
                cps = self.__nexusconfig_device.AvailableComponents()  
            if not component:
                mand = self.__nexusconfig_device.MandatoryComponents()
                cps = list(set(cps)- set(mand))

            if not self.silent:
                self.output("Configuration Server: %s" % servers[0])

            if not component:
                if not self.silent:
                    self.output("\nMandatory Components: %s" %  mand)
                for cp in mand:
                    dss = self.__getDataSourceAttributes(cp)  
                    tr = {}
                    for ds in dss.keys():
                        for vds in dss[ds]:
                            if (not strategy or vds[0] == strategy) and \
                                    (not dstype or vds[1] == dstype):
                                if ds not in tr:
                                    tr[ds] = []
                                tr[ds].append(vds)
                    if not self.silent:
                        self.output("%s: %s" % (cp, str(tr)))
                    self.__result[0][cp] = tr

            if not self.silent and not component:
                self.output("\nOther Components: %s" % (str(cps)))
            for cp in cps:
                dss = self.__getDataSourceAttributes(cp)  
                tr = {}
                for ds in dss.keys():
                    for vds in dss[ds]:
                        if (not strategy or vds[0] == strategy) and \
                                (not dstype or vds[1] == dstype):
                            if ds not in tr:
                                tr[ds] = []
                            tr[ds].append(vds)
                if not self.silent:
                    self.output("%s: %s" % (cp, str(tr)))
                self.__result[1][cp] = tr
     
    @property
    def data(self):
        return self.__result


@imacro()
def nxs_select_components(self):
    """Macro nxs_select_components"""
    db = PyTango.Database()
    try:
        servers = [self.getEnv("NeXusSettingsDevice")]
    except:   
        servers = db.get_device_exported_for_class(
            "NXSRecSettings").value_string 

    if len(servers) > 0:
        self.__nexussetting_device = PyTango.DeviceProxy(servers[0])
        self.output("Setting Server: %s" % servers[0])
        servers = [self.__nexussetting_device.ConfigDevice]

    else:
        try:
            servers = [self.getEnv("NeXusConfigDevice")]
        except:   
            servers = db.get_device_exported_for_class(
                "NXSConfigServer").value_string 
 
    if len(servers) > 0:
        self.__nexusconfig_device = PyTango.DeviceProxy(servers[0])
        self.__nexusconfig_device.Open()
        mancps = self.__nexusconfig_device.MandatoryComponents()  
        
        dt =  self.createMacro("nxs_datasource_components", 
                               '', 'STEP', '')
        dt[0].silent = True
        self.runMacro(dt[0])
        res = dt[0].data

        
        loop = True
        while loop:
            try:
                envcps = set(self.getEnv("NeXusComponents"))
            except:
                envcps = set()
            
            self.output("Mandatory Components: %s" % mancps)
            self.output("Selected Components: %s" % list(envcps))
            
            others = list(set(res[0]) - set(mancps) - envcps)
            self.output("Other Components: %s" % others)
            self.output("Non-selected Datasources: %s" % res[1])
#            cmd = 'n'
            cmd = self.input(
                "Would you like to [A]dd or [R]emove components? [A/R/N]")
            if cmd.lower() == 'r':
                mcps = self.input("Components to remove: ")
                lcps = mcps.strip().split(' ')
                for lcp in lcps:
                    try:
                        envcps.remove(lcp)
                    except:
                        self.warning("'%s' not in %s" % (lcp, envcps))
                self.setEnv("NeXusComponents", list(envcps))
            elif cmd.lower() == 'a':
                mcps = self.input("Components to add: ")
                lcps = mcps.strip().split(' ')
                for lcp in lcps:
                    if lcp in res[0]:
                        envcps.add(lcp)
                    else:
                        self.warning("'%s' not in %s" % (lcp, res[0]))
                self.setEnv("NeXusComponents", list(envcps))
            elif cmd.lower() == 'n':
                loop = False

#            self.output("Non-selected Datasources: %s" % res[1])



class nxs_select_elements(iMacro):
    """Macro nxs_select_elements"""

    def __selectComponents(self, mancps):
        if self.__nexussetting_device:
            if self.__nexussetting_device.Components:
                envcps = set(self.__nexussetting_device.Components) 
            else:
                envcps = set()
        else:
            try:
                envcps = set(self.getEnv("NeXusComponents"))
            except:
                envcps = set()

        dt =  self.createMacro("nxs_datasource_components", 
                               '', 'STEP', '')
        dt[0].silent = True
        self.runMacro(dt[0])
        res = dt[0].data

        self.output("Mandatory Components: %s" % mancps)
            
        nav = []
        for el in envcps:
            if el not in res[0]:
                nav.append(el)
                self.warning(
                    "Component %s not available. It will be deselected." % el)
        envcps = list(set(envcps) - set(nav))

        sel = self.input("Select Components", 
                         data_type=tuple(sorted(set(res[0]) - set(mancps))), 
                         allow_multiple=True,
                         default_value=tuple(envcps))
            

        if self.__nexussetting_device:
            try:
                cpgrp = json.loads(self.__nexussetting_device.ComponentGroup)
                self.output("CPG1 %s" % str(cpgrp))
            except:
                cpgrp = {}
            for cp in cpgrp:
                cpgrp[cp] = False
            if sel:    
                for sl in sel:
                    cpgrp[sl] = True
            self.__nexussetting_device.ComponentGroup = json.dumps(cpgrp)
        else:
            self.setEnv("NeXusComponents", list(sel))
        return sel


    def __selectDataSources(self):
        if self.__nexussetting_device:
#            print  "DEVICE '%s'" % str(self.__nexussetting_device.DataSources)
#            print  "DEVICE2 '%s'" % str(self.__nexussetting_device.Components)
            if self.__nexussetting_device.DataSources:
                envdss = set(self.__nexussetting_device.DataSources)
            else:
                envdss = set()
        else:
            try:
                envdss = set(self.getEnv("NeXusDataSources"))
            except:
                envdss = set()


        dt =  self.createMacro("nxs_datasource_components", 
                               '', 'STEP', '', True)
        dt[0].silent = True
        self.runMacro(dt[0])
        res = dt[0].data

        nav = []
        for el in envdss:
            if el not in res[1]:
                nav.append(el)
                self.warning(
                    "DataSource %s will be deselected." % el)
        envdss = list(set(envdss) - set(nav))

        sel = self.input("Select Additional Devices" , 
                         data_type=tuple(sorted(set(res[1]))), 
                         allow_multiple=True,
                         default_value=tuple(envdss))
            
        if self.__nexussetting_device:
            try:
                dsgrp = json.loads(self.__nexussetting_device.DataSourceGroup)
            except:
                dsgrp = {}
            for ds in dsgrp:
                dsgrp[ds] = False
            if sel:    
                for sl in sel:
                    dsgrp[sl] = True
            self.__nexussetting_device.DataSourceGroup = json.dumps(dsgrp)
        else:
            self.setEnv("NeXusDataSources", list(sel))
        return sel

    def run(self):
        db = PyTango.Database()
        self.__nexussetting_device = None
        try:
            servers = [self.getEnv("NeXusSettingsDevice")]
        except:   
            servers = db.get_device_exported_for_class(
                "NXSRecSettings").value_string 

        if len(servers) > 0:
            self.__nexussetting_device = PyTango.DeviceProxy(servers[0])
            self.output("Setting Server: %s" % servers[0])
            servers = [self.__nexussetting_device.ConfigDevice]

        else:
            try:
                servers = [self.getEnv("NeXusConfigDevice")]
            except:   
                servers = db.get_device_exported_for_class(
                    "NXSConfigServer").value_string 
 
        ready = False    
        while len(servers) > 0 and not ready:
            self.__nexusconfig_device = PyTango.DeviceProxy(servers[0])
            self.__nexusconfig_device.Open()
            mancps = self.__nexusconfig_device.MandatoryComponents()  
            cpsel = self.__selectComponents(mancps)
            self.output("Selected Components: %s" % list(cpsel))
            dssel = self.__selectDataSources()
            self.output("Selected DataSources: %s" % list(dssel))
            try:
                self.__nexusconfig_device.CreateConfiguration(list(cpsel))
                ready = True
            except:
                self.warning(
                    "Selected components are not compatible. Please reselect them.")
        ready = False    
        while len(servers) > 0 and not ready:
            timer = self.input(
                "Please provide the timer\n"
                "(otherwise Measurement Group will not be updated)")
            if timer.strip() == '':
                ready = True
            else:
                try:
                    dt =  self.createMacro("nxs_set_mntgrp_from_components",
                                           timer,  True)
                    dt[0].silent = True
                    dt[0].datasources = list(dssel)
                    self.runMacro(dt[0])
                    ready = True
                    self.output("Selected Timer: %s" % timer)
                except Exception as e:
                    self.warning("Warning: %s" % str(e))
            
        



        



class nxs_datasource_components(Macro):
    """Macro nxs_datasource_components"""


    param_def = [
        ['datasource', Type.String, '', 'datasource name'],
        ['strategy', Type.String, '', 
         'strategy mode filter [default \'\' for all]'],
        ['dstype', Type.String, '', 
         'datasource type filter [default \'\' for all]'],
        ['env_components', Type.Boolean, False, 
         'lists components from the NeXusComponents '\
             +'environment variable [default False]']
        ]


    def prepare(self, datasource, strategy, dstype, env_components):
        self.__result = [[], [], []]

        db = PyTango.Database()
        try:
            servers = [self.getEnv("NeXusSettingsDevice")]
        except:   
            servers = db.get_device_exported_for_class(
                "NXSRecSettings").value_string 

        if len(servers) > 0:
            self.__nexussetting_device = PyTango.DeviceProxy(servers[0])
#            self.output("Setting Server: %s" % servers[0])
            servers = [self.__nexussetting_device.ConfigDevice]

        else:
            try:
                servers = [self.getEnv("NeXusConfigDevice")]
            except:   
                servers = db.get_device_exported_for_class(
                    "NXSConfigServer").value_string 
 
        if len(servers) > 0:
            self.__nexusconfig_device = PyTango.DeviceProxy(servers[0])
            self.__nexusconfig_device.Open()
        else:
            self.error("Please select the NeXusConfigDevice")

        self.silent = False
        
    def run(self, datasource, strategy, dstype, env_components):

        self.__result[0] = self.__nexusconfig_device.AvailableComponents()  
        avldss = self.__nexusconfig_device.AvailableDataSources()  
        if datasource:
            mydss = [datasource]
        else:
            mydss = avldss
                

        dt =  self.createMacro("nxs_component_describe_full", 
                               '', '', '', env_components)
        dt[0].silent = True
        self.runMacro(dt[0])
        res = dt[0].data
        for mds  in mydss:
            found = False
            for grp in res:
                for cp, dss in grp.items():
                    for ds,props in dss.items():
                        if mds == ds:
                            #                                self.output("df: %s : %s" % (strategy, props))
                            for prop in props:
                                if (not strategy or \
                                        strategy == prop[0]) \
                                        and (not dstype or \
                                                 dstype == prop[1]) :
                                    found = True
                                    self.__result[2].append((mds, cp, prop))
                                    if not self.silent:
                                        self.output(
                                            "'%s' found in '%s' : %s" % (
                                                mds, cp, prop))
            if not found:
                self.__result[1].append(mds) 
        if not datasource and not self.silent:        
            self.output("Lonely Datasources: %s" % str(self.__result[1]))
                

    @property
    def data(self):
        return self.__result

class nxs_set_mntgrp_from_components(Macro):
    """Macro nxs_set_mntgrp_from_components"""
    

    param_def = [
        ['timer', Type.String, '', 'master timer'],
        ['flagClear', Type.Boolean, False, 'clear measurement group']
        ]

    def prepare(self, timer, flagClear):
        ## tango database
        self.__db = PyTango.Database()
        ## pools
        self.__pools = []
        ## configuration
        self.__hsh = {}
        self.__hsh['controllers'] = {} 
        self.__hsh['description'] = "Measurement Group" 
        self.__hsh['label'] = "" 
        self.__masterTimer = 'exp_t01'

        self.datasources = []
        self.silent = False

    def __getDeviceNamesByClass(self, className):
        srvs = self.__getServerNameByClass(className)
        argout = []
        for srv in srvs:
            lst = self.__db.get_device_name( srv, className).value_string
            for i in range(0, len( lst)):
                argout.append( lst[i])
        return argout


    def __getServerNameByClass(self,  argin): 
        srvs = self.__db.get_server_list( "*").value_string
        argout = []
        for srv in srvs:
            classList = self.__db.get_server_class_list( srv).value_string
            for clss in classList:
                if clss == argin:
                    argout.append(srv)
                    break
        return argout


    def __setpools(self):
        poolNames = self.__getDeviceNamesByClass( "Pool")
        self.__pools = []
        for pool in poolNames:
            dp = PyTango.DeviceProxy(pool)
            try:
                dp.ping()
                self.__pools.append(dp)    
                if not self.silent:
                    self.output("APOOL: %s" % pool)
            except:
                pass

    def run(self, timer, flagClear):
        aliases = []
        if isinstance(self.datasources, list):
            aliases = self.datasources
        if timer:
            aliases.append(timer)

        self.__setpools()
        dt =  self.createMacro("nxs_component_describe_full",
                               '', 'STEP', 'CLIENT', True)
        dt[0].silent = True
        self.runMacro(dt[0])
        res = dt[0].data
        for grp in res:
            for dss in grp.values():
                for ds in dss.keys():
                    aliases.append(str(ds))
        aliases = list(set(aliases))
        if not self.silent:
            self.output("devices:\n %s" % (str(aliases)))


        mntGrpName = self.getEnv('ActiveMntGrp')
        self.__mg = self.getObj(mntGrpName, type_class=Type.MeasurementGroup).getObj()
        cfg = self.__mg.Configuration
        if not self.silent:
            self.output("CONF:\n%s" % str(cfg))
        if flagClear:
            self.__hsh['label'] = mntGrpName
            self.index = 0
        else:
            self.__hsh = json.loads(self.__mg.Configuration)
            if u'controllers' in self.__hsh.keys():
                self.index = len(self.__hsh[ u'controllers'].keys())
            else:
                self.index = 0 
        if timer:
            self.__masterTimer = timer
        elif not flagClear:
            self.__masterTimer = self.__db.get_alias(str(
                    "/".join((self.__hsh['timer'].split("/"))[1:])))
        if not self.silent:
            self.output("TIMER: %s" % self.__masterTimer)    
        fullname = self.__findFullDeviceName(self.__masterTimer)
        if not fullname:
            raise Exception("Timer or Monitor cannot be found amount the servers")
        self.__hsh[ u'monitor'] = fullname
        self.__hsh[ u'timer'] = fullname
            
            
        pool = self.__mg.getPoolObj()
        if not self.silent:
            self.output("POOL:\n%s" % str(pool))
        ctrls = pool.read_attribute("ControllerList").value
        if not self.silent:
            self.output("CTRLS:\n%s" % str(ctrls))

        for alias in aliases:
            self.__addDevice(alias)
        if not self.silent:
            self.output("RESULT:\n%s" % str(self.__hsh))
            
        self.__updateConfiguration()

        
    def __findDeviceController( self, device):
        """
        returns the controller that belongs to a device
        """
        lst = []
        for pool in self.__pools:
            if not pool.ExpChannelList is None:
                lst += pool.ExpChannelList
        ctrl = None
        for elm in lst:
            chan = json.loads( elm)
            if device == chan['name']:
                ctrl = chan['controller']
                break
        return ctrl


    def __findFullDeviceName( self, device):
        """
          input: exp_c01
          returns: expchan/hasylabvirtualcounterctrl/1
        """
        lst = []
        for pool in self.__pools:
            lst += pool.AcqChannelList
        argout = None
        for elm in lst:
            chan = json.loads( elm)
            if device == chan['name']:
                #
                # from: expchan/hasysis3820ctrl/1/value
                # to:   expchan/hasysis3820ctrl/1
                #
                arr = chan['full_name'].split("/")
                argout = "/".join(arr[0:-1])
        return argout


    def __updateConfiguration( self):
        """
        json-dump the dictionary self.__hsh to the Mg configuration
        """
        self.__mg.setConfiguration(self.__hsh)

    def __addDevice( self, device):
        ctrl = self.__findDeviceController( device)
        self.debug("DEVCTRL: %s -> %s" % (ctrl, device))
        if not ctrl:
            return
        if not self.__hsh[ u'controllers'].has_key( ctrl):
#            self.masterTimer = device
#            self.__hsh[ u'monitor'] = self.__findFullDeviceName( device)
#            self.__hsh[ u'timer'] = self.__findFullDeviceName( device)
            self.__hsh[ u'controllers'][ ctrl] = {}
            self.__hsh[ u'controllers'][ ctrl][ u'units'] = {}
            self.__hsh[ u'controllers'][ ctrl][ u'units'][u'0'] = {}
            self.__hsh[ u'controllers'][ ctrl][ u'units'][u'0'][
                u'channels'] = {}
            self.__hsh[ u'controllers'][ ctrl][ u'units'][u'0'][ u'id'] = 0
            self.__hsh[ u'controllers'][ ctrl][ u'units'][u'0'][
                u'monitor'] = self.__findFullDeviceName(self.__masterTimer)
            self.__hsh[ u'controllers'][ ctrl][ u'units'][u'0'][ 
                u'timer'] = self.__findFullDeviceName(self.__masterTimer)
            self.__hsh[ u'controllers'][ ctrl][ u'units'][u'0'][ 
                u'trigger_type'] = 0

        ctrlChannels = self.__hsh[ u'controllers'][ctrl][ u'units'][ u'0'][
            u'channels']
        
        full_name = self.__findFullDeviceName( device) 
        if not full_name in ctrlChannels.keys():
            dp  = PyTango.DeviceProxy(full_name.encode())
            da =  dp.read_attribute('value')
            self.debug("adding index %s %s" % (self.index, device))
            dct = {}
            dct[ u'_controller_name'] = unicode(ctrl)
            dct[ u'_unit_id'] = u'0'
            dct[ u'conditioning'] = u''
            dct[ u'data_type'] = u'float64'
            dct[ u'data_units'] = u'No unit'
            dct[ u'enabled'] = True
            dct[ u'full_name'] = full_name
            dct[ u'index'] = self.index
            self.index += 1
            dct[ u'instrument'] = None
            dct[ u'label'] = unicode(device)
            dct[ u'name'] = unicode(device)
            dct[ u'ndim'] = 0
            dct[u'nexus_path'] = u''
            dct[ u'normalization'] = 0
            dct[ u'output'] = True
            dct[ u'plot_axes'] = []
            dct[ u'plot_type'] = 0
            if da.dim_x and da.dim_x > 1 :
                dct[ u'shape'] = [da.dim_y, da.dim_x] \
                    if da.dim_y \
                    else [da.dim_x]
            else:
                dct[ u'shape'] = [] 
            dct[ u'source'] = dct['full_name'] + "/value"
            ctrlChannels[full_name] = dct
            









