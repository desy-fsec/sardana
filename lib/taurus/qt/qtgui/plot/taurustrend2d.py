#!/usr/bin/env python

#############################################################################
##
## This file is part of Taurus, a Tango User Interface Library
## 
## http://www.tango-controls.org/static/taurus/latest/doc/html/index.html
##
## Copyright 2011 CELLS / ALBA Synchrotron, Bellaterra, Spain
## 
## Taurus is free software: you can redistribute it and/or modify
## it under the terms of the GNU Lesser General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
## 
## Taurus is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU Lesser General Public License for more details.
## 
## You should have received a copy of the GNU Lesser General Public License
## along with Taurus.  If not, see <http://www.gnu.org/licenses/>.
##
#############################################################################

"""
taurustrend.py: Generic trend widget for Taurus
"""
__all__=["TaurusTrend2D"]

from guiqwt.plot import ImageWidget, ImageDialog
from PyQt4 import Qt
import taurus.core
from taurus.qt.qtgui.base import TaurusBaseWidget
from taurus.qt.extra_guiqwt.image import TaurusTrend2DItem
from taurus.qt.extra_guiqwt.tools import TaurusModelChooserTool, TimeAxisTool


class TaurusTrend2D(ImageDialog, TaurusBaseWidget):
    '''
    This is a widget for displaying trends from 1D Taurus attributes (i.e.,
    representing the variation over time of a 1D array). Sometimes this kind of
    plots are also known as "spectrograms".
    
    The widget shows a 3D plot (Z represented with colors) where the values in
    the 1D array are plotted in the Y-Z plane and are stacked along the X axis.
    '''
    def __init__(self, parent=None, designMode=False, toolbar=True, xIsTime=True, buffersize=512, options=None, **kwargs):
        '''see :class:`guiqwt.plot.ImageWidget` for other valid initialization parameters'''
        name = "TaurusTrend2D"
        if options is None:
            options = dict(lock_aspect_ratio=False)
        ImageDialog.__init__(self, parent=parent, toolbar=toolbar, options=options, **kwargs)
        TaurusBaseWidget.__init__(self, name)
        self.trendItem = None
        #add some tools
        for toolklass in (TaurusModelChooserTool, TimeAxisTool):
            self.add_tool(toolklass)   
        #manage time mode
        self.xIsTime = xIsTime
        timetool = self.get_tool(TimeAxisTool)
        timetool.set_scale_y_t(self.xIsTime)
        self.buffersize = buffersize
        self._useArchiving = False
    
    def getModelClass(self):
        '''reimplemented from :class:`TaurusBaseWidget`'''
        return taurus.core.TaurusAttribute
        
    def setModel(self, model):
        '''reimplemented from :class:`TaurusBaseWidget`'''
        plot = self.get_plot()
        if self.trendItem is not None:
            plot.del_item(self.trendItem)
        self.trendItem = TaurusTrend2DItem(xIsTime=self.xIsTime, buffersize = self.buffersize)
        self.trendItem.setModel(model)
        plot.add_item(self.trendItem)
        try:
            plot.set_axis_title('left', self.trendItem.getModelObj().getSimpleName())
        except:
            self.debug('cannot set title for left axis')
            sef.traceback()
        try:
            plot.set_axis_unit('left', self.trendItem.getModelObj().getConfig().getUnit())
        except:
            self.debug('cannot set units for left axis')
            self.traceback(level = taurus.Info)
        
        self.connect(self.trendItem.getSignaller(), Qt.SIGNAL("dataChanged"), self.update_cross_sections)
        
    def getModel(self):
        '''reimplemented from :class:`TaurusBaseWidget`'''
        if self.trendItem is None:
            return None
        else:
            return self.trendItem.getModel()
    
    @classmethod
    def getQtDesignerPluginInfo(cls):
        """reimplemented from :class:`TaurusBaseWidget`"""
        ret = TaurusBaseWidget.getQtDesignerPluginInfo()
        ret['module'] = 'taurus.qt.qtgui.plot'
        ret['group'] = 'Taurus Display Widgets'
        ret['icon'] =':/designer/qwtplot.png'
        return ret  
    
    def setUseArchiving(self, enable):
        '''enables/disables looking up in the archiver for data stored before
        the Trend was started
        
        :param enable: (bool) if True, archiving values will be used if available
        '''
        if not self.xIsTime:
            self.info('ignoring setUseArchiving. Reason: not in X time scale')
        self._useArchiving = enable
        
    def getUseArchiving(self):
        '''whether TaurusTrend is looking for data in the archiver when needed
        
        :return: (bool)
        
        .. seealso:: :meth:`setUseArchiving`
        '''
        return self._useArchiving

    def resetUseArchiving(self):
        '''Same as setUseArchiving(False)'''
        self.setUseArchiving(False)
        
    def setMaxDataBufferSize(self, maxSize):
        '''sets the maximum number of events that will be stacked
        
        :param maxSize: (int) the maximum limit
        
        .. seealso:: :class:`TaurusTrendSet`
        '''
        if self.trendItem is not None:
            self.trendItem.setBufferSize(maxSize)
    
        self.buffersize = maxSize
        
    def getMaxDataBufferSize(self):
        '''returns the maximum number of events that can be plotted in the trend
        
        :return: (int)
        '''
        return self.buffersize
            
    def resetMaxDataBufferSize(self):
        '''Same as setMaxDataBufferSize(512)  (i.e. 512 events)'''
        self.setMaxDataBufferSize(512) 
    
    model = Qt.pyqtProperty("QString", getModel, setModel, TaurusBaseWidget.resetModel)
    useArchiving = Qt.pyqtProperty("bool", getUseArchiving, setUseArchiving, resetUseArchiving)
    maxDataBufferSize = Qt.pyqtProperty("int", getMaxDataBufferSize, setMaxDataBufferSize, resetMaxDataBufferSize)   
        
        
def taurusTrend2DMain():
    from taurus.qt.qtgui.application import TaurusApplication
    import taurus.core
    import sys
    
    #prepare options
    parser = taurus.core.util.argparse.get_taurus_parser()
    parser.set_usage("%prog [options] <model>")
    parser.set_description('a Taurus application for plotting trends of arrays (aka "spectrograms")')
    parser.add_option("-x", "--x-axis-mode", dest="x_axis_mode", default='e', metavar="t|e",
                  help="interprete X values as either timestamps (t) or event numbers (e). Accepted values: t|e")    
    parser.add_option("-b", "--buffer", dest="max_buffer_size", default='512', 
                      help="maximum number of values to be stacked (when reached, the oldest values will be discarded)")
    parser.add_option("-a", "--use-archiving", action="store_true", dest="use_archiving", default=False)
    parser.add_option("--demo", action="store_true", dest="demo", default=False)
    app = TaurusApplication(cmd_line_parser=parser, app_name="Taurus Trend 2D", app_version=taurus.Release.version)
    args = app.get_command_line_args()
    options = app.get_command_line_options()
    
    #check & process options
    if options.x_axis_mode.lower() not in ['t', 'e']:
        parser.print_help(sys.stderr)
        sys.exit(1)
    xIsTime = options.x_axis_mode.lower() == 't'
      
    if options.demo:
        args.append('eval://sin(x+t)?x=linspace(0,3,40);t=rand()')
        
    w = TaurusTrend2D(xIsTime=xIsTime, wintitle="Taurus Trend 2D", buffersize=int(options.max_buffer_size))
    
    #set archiving
    if options.use_archiving:
        raise NotImplementedError('Archiving support is not yet implemented')
        w.setUseArchiving(True)
    
    #set model
    if len(args) == 1:
        w.setModel(args[0])
    else:
        parser.print_help(sys.stderr)
        sys.exit(1)
    
    w.show()
    sys.exit(app.exec_())      
        

if __name__ == "__main__":
    taurusTrend2DMain()    