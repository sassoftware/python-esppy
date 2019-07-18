import logging
import uuid
import sys
import matplotlib

from base64 import b16encode

import plotly.colors as clrs

from matplotlib import cm

import numpy as np

class Options(object):
    def __init__(self,**kwargs):
        self._options = {}

        for name,value in kwargs.items():
            self.set(name,value)

    def has(self,name):
        code = False

        if isinstance(name,list):
            for n in name:
                if n.lower() in self._options:
                    code = True
                    break
        else:
            code = name.lower() in self._options

        return(code)

    def get(self,name,dv = None,clear = False):
        value = None
        s = name.lower()

        if s in self._options:
            value = self._options[s]
            if clear:
                self.clear(s)

        if value == None and dv != None:
            value = dv
        
        return(value)

    def set(self,name,value):
        s = name.lower()
        if value == None:
            if s in self._options:
                del self._options[s]
        else:
            self._options[s] = value

    def clear(self,name):
        s = name.lower()
        if s in self._options:
            del self._options[s]

    def setOptions(self,**kwargs):
        for name,value in kwargs.items():
            self.set(name,value)

    def items(self):
        return(self._options.items())

    @property
    def options(self):
        return(self._options)

    @options.setter
    def options(self,options):
        for name,value in six.iteritems(options):
            self.set(name,value)

class Gradient(object):
    def __init__(self,color,**kwargs):
        if len(color) != 7:
            raise Exception("invalid color: " + str(color))

        self._color = color
        self._options = Options(**kwargs)
        self._levels = self._options.get("levels",100)

        minv = self._options.get("min",0)
        maxv = self._options.get("max",100)

        self._a = np.arange(minv,maxv,(maxv - minv) / self._levels)

    def darken(self,value):
        a = np.where(value >= self._a)[0]
        level = len(a) - 1

        rgbHex = [self._color[x:x + 2] for x in [1, 3, 5]]
        rgb = [int(v, 16) - level for v in rgbHex]
        rgb = [min([255, max([0,i])]) for i in rgb]

        s = "#{0:02x}{1:02x}{2:02x}".format(rgb[0],rgb[1],rgb[2])

        return(s)

    def brighten(self,value):
        a = np.where(value >= self._a)[0]
        level = len(a) - 1

        rgbHex = [self._color[x:x + 2] for x in [1, 3, 5]]
        rgb = [int(value, 16) + level for value in rgbHex]
        rgb = [min([255, max([0,i])]) for i in rgb]

        s = "#{0:02x}{1:02x}{2:02x}".format(rgb[0],rgb[1],rgb[2])

        return(s)

    @property
    def color(self):
        return(self._color)

    @color.setter
    def color(self,value):
        self._color = value

class Colors(object):
    _sasThemes = {
        "sas_base":["#00929f", "#f08000", "#90b328", "#3d5aae", "#ffca39", "#a6427c", "#9c2910", "#736519"],
        "sas_dark":["#90b328", "#9c2910", "#ffca39", "#00929f", "#736519", "#f08000", "#a6427c"],
        "sas_highcontrast":["#a1d73b", "#ff791d", "#ffd736", "#cb66ff", "#ff5252", "#57b2ff", "#fa96e0", "#33f7b0"],
        "sas_light":["#3d5aae", "#90b328", "#9c2910", "#ffca39", "#00929f", "#736519", "#f08000", "#a6427c"],
        "sas_marine":["#00929f", "#f08000", "#90b328", "#3d5aae", "#ffca39", "#a6427c", "#9c2910", "#736519"],
        "sas_midnight":["#2470ad", "#98863c", "#5954ad", "#985b30", "#238a92", "#84414b", "#17785f", "#985186"],
        "sas_opal":["#33a3ff", "#ffcc32", "#9471ff", "#ff8224", "#2ad1d1", "#dd5757", "#15b57b", "#ff6fbd"],
        "sas_sail":["#21b9b7", "#4141e0", "#7db71a", "#8e2f8a", "#d38506", "#0abf85", "#2f90ec", "#db3851"],
        "sas_snow":["#3d5aae", "#90b328", "#9c2910", "#ffca39", "#00929f", "#736519", "#f08000", "#a6427c"],
        "sas_umstead":["#00929f", "#f08000", "#90b328", "#3d5aae", "#ffca39", "#a6427c", "#9c2910", "#736519"]
    }
    def __init__(self,colormap = None):
        colors = []
        colorscale = []
        luma = []

        if colormap != None:
            if colormap.index("sas_") == 0:
                if colormap in Colors._sasThemes:
                    colors.extend(Colors._sasThemes[colormap])
            elif colormap in clrs.PLOTLY_SCALES:
                cmap = clrs.PLOTLY_SCALES[colormap]
                interval = 1 / (len(cmap) - 1)
                index = 0
                for i,c in enumerate(cmap):
                    s = c[1]
                    if s[0] == '#':
                        colors.append(s)
                    else:
                        i1 = s.index("(")
                        i2 = s.index(")")
                        s = s[i1 + 1:i2]
                        colorscale.append([index,"rgb(" + s + ")"])
                        a = s.split(",")
                        r = int(a[0])
                        g = int(a[1])
                        b = int(a[2])
                        value = (r,g,b)
                        luma.append(0.2126 * r + 0.7152 * g + 0.0722 * b)
                        s = "#" + b16encode(bytes(value)).decode()
                        colors.append(s)

                    if i == (len(cmap) - 2):
                        index = 1
                    else:
                        index += interval
            else:
                try:
                    cmap = matplotlib.cm.get_cmap(colormap)
                    norm = matplotlib.colors.Normalize(vmin = 0,vmax = 255)
                    rgb = []

                    for i in range(0, 255):
                        k = matplotlib.colors.colorConverter.to_rgb(cmap(norm(i)))
                        rgb.append(k)

                    entries = 255

                    h = 1.0 / (entries - 1)

                    prev = None

                    a = []

                    for i in range(entries):
                        c = list(map(np.uint8,np.array(cmap(i * h)[:3]) * 255))
                        value = (c[0],c[1],c[2])
                        if value == prev:
                            continue
                        luma.append(0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2])
                        prev = value
                        a.append(["#" + b16encode(bytes(value)).decode(),"rgb(" + str(c[0]) + "," + str(c[1]) + "," + str(c[2]) + ")"])

                    if len(a) > 1:
                        interval = 1 / (len(a) - 1)
                        index = 0

                        for i,x in enumerate(a):
                            colors.append(x[0])
                            colorscale.append([index,x[1]])
                            if i == (len(a) - 2):
                                index = 1
                            else:
                                index += interval

                except:
                    pass

        if len(colors) == 0:
            interval = 1 / (len(clrs.DEFAULT_PLOTLY_COLORS) - 1)
            index = 0
            for i,c in enumerate(clrs.DEFAULT_PLOTLY_COLORS):
                i1 = c.index("(")
                i2 = c.index(")")
                s = c[i1 + 1:i2]
                colorscale.append([index,"rgb(" + s + ")"])
                a = s.split(",")
                r = int(a[0])
                g = int(a[1])
                b = int(a[2])
                luma.append(0.2126 * r + 0.7152 * g + 0.0722 * b)
                value = (r,g,b)
                colors.append("#" + b16encode(bytes(value)).decode())

                if i == (len(clrs.DEFAULT_PLOTLY_COLORS) - 2):
                    index = 1
                else:
                    index += interval
        elif len(colorscale) == 0:
            interval = 1 / (len(colors) - 1)
            index = 0
            for i,c in enumerate(colors):
                r = int(c[1:3],16)
                g = int(c[3:5],16)
                b = int(c[5:7],16)

                colorscale.append([index,"rgb(" + str(r) + "," + str(g) + "," + str(b) + ")"])
                luma.append(0.2126 * r + 0.7152 * g + 0.0722 * b)

                if i == (len(colors) - 2):
                    index = 1
                else:
                    index += interval

        self._colors = colors
        self._colorscale = colorscale
        self._luma = luma 

    def getColors(self,num,increment):
        index = 0
        colors = []

        for i in range(0,num):
            colors.append(self._colors[index])
            index += increment
            if index == len(self._colors):
                index = 0

        return(colors)

    def getSpread(self,num):
        delta = int(len(self._colors) / num)
        return(self.getColors(num,delta))

    def getFirst(self,num = 1):
        colors = []

        index = 0

        for i in range(0,num):
            colors.append(self._colors[index])
            index += 1
            if index == len(self._colors):
                index = 0

        return(colors)

    def getClosestTo(self,luma):
        color = None
        if len(self._colors) > 0:
            index = -1
            diff = sys.maxsize
            for i,l in enumerate(self._luma):
                d = abs(luma - l)
                if d < diff:
                    diff = d
                    index = i

            if index >= 0:
                color = self._colors[index]

        return(color)

    @property
    def colors(self):
        return(self._colors)

    @property
    def colorscale(self):
        return(self._colorscale)

    @property
    def first(self):
        color = None
        if len(self._colors) > 0:
            color = self._colors[0]
        return(color)

    @property
    def last(self):
        color = None
        if len(self._colors) > 0:
            color = self._colors[len(self._colors) - 1]
        return(color)

    @property
    def lightest(self):
        color = None
        if len(self._colors) > 0:
            maxLuma = 0
            index = -1
            for i,l in enumerate(self._luma):
                if l > maxLuma:
                    maxLuma = l
                    index = i

            if index >= 0:
                color = self._colors[index]

        return(color)

def supports(o,method):
    code = False
    if (o != None):
        try:
            value = getattr(o,method)
            if (value != None):
                code = callable(value)
        except:
            pass
    return(code)

def contains(list,o):
    for item in list:
        if item == o:
            return(True)

    return(False)

def indexOf(list,o):
    i = 0
    for item in list:
        if item == o:
            return(i)
        i += 1

    return(-1)

def addTo(list,o):
    if contains(list,o) == False:
        list.append(o);
        return(True);

    return(False);

def removeFrom(list,o):
    index = indexOf(list,o)
    if index >= 0:
        list.pop(index)

def guid():
    return(str(uuid.uuid4()).replace('-', '_'))

def brighten(color,offset):
    if len(color) != 7:
        return("#ffffff")

    rgbHex = [color[x:x + 2] for x in [1, 3, 5]]
    rgb = [int(value, 16) + offset for value in rgbHex]
    rgb = [min([255, max([0,i])]) for i in rgb]

    c = "#" + "".join([hex(i)[2:] for i in rgb])

    return(c)

def darken(color,offset):
    if len(color) != 7:
        return("#ffffff")

    rgbHex = [color[x:x + 2] for x in [1, 3, 5]]
    rgb = [int(value, 16) - offset for value in rgbHex]
    rgb = [min([255, max([0,i])]) for i in rgb]

    c = "#" + "".join([hex(i)[2:] for i in rgb])

    return(c)

def convertColormap(name):
    cmap = matplotlib.cm.get_cmap(name)
    norm = matplotlib.colors.Normalize(vmin = 0,vmax = 255)
    rgb = []
 
    for i in range(0, 255):
        k = matplotlib.colors.colorConverter.to_rgb(cmap(norm(i)))
        rgb.append(k)

    entries = 255

    h = 1.0 / (entries - 1)
    colorscale = []

    for k in range(entries):
        C = list(map(np.uint8,np.array(cmap(k * h)[:3]) * 255))
        colorscale.append([k * h,"rgb" + str((C[0], C[1], C[2]))])

    return(colorscale)

def convertColormapToPalette(name):
    cmap = matplotlib.cm.get_cmap(name)
    norm = matplotlib.colors.Normalize(vmin = 0,vmax = 255)
    rgb = []
 
    for i in range(0, 255):
        k = matplotlib.colors.colorConverter.to_rgb(cmap(norm(i)))
        rgb.append(k)

    entries = 255

    h = 1.0 / (entries - 1)
    colorscale = []

    prev = None

    for k in range(entries):
        c = list(map(np.uint8,np.array(cmap(k * h)[:3]) * 255))
        value = (c[0],c[1],c[2])
        if value == prev:
            continue
        prev = value
        s = "#" + b16encode(bytes(value)).decode()
        colorscale.append(s)

    return(colorscale)
