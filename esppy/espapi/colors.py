import logging
import sys
import matplotlib

from base64 import b16encode, b64encode

from esppy.espapi.tools import Options

from matplotlib import cm

import matplotlib.colors as mcolors

import numpy as np

class Gradient(Options):
    def __init__(self,color,**kwargs):
        Options.__init__(self,**kwargs)

        c = Colors.getColorFromName(color)

        if c == None:
            raise Exception("invalid color: " + str(color))

        if len(c) != 7:
            raise Exception("invalid color: " + str(color))

        self._color = c
        self._levels = self.getOpt("levels",100)

        minv = self.getOpt("min",0)
        maxv = self.getOpt("max",100)

        self._a = []

        if maxv > minv:
            self._a = np.arange(minv,maxv,(maxv - minv) / self._levels)

    def darken(self,value):

        s = self._color

        if len(self._a) > 0:
            a = np.where(value >= self._a)[0]
            level = len(a) - 1

            rgbHex = [self._color[x:x + 2] for x in [1, 3, 5]]
            rgb = [int(v, 16) - level for v in rgbHex]
            rgb = [min([255, max([0,i])]) for i in rgb]

            s = "#{0:02x}{1:02x}{2:02x}".format(rgb[0],rgb[1],rgb[2])

        return(s)

    def lighten(self,value):
        s = self._color

        if len(self._a) > 0:
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

class Colors(Options):
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
        "sas_umstead":["#00929f", "#f08000", "#90b328", "#3d5aae", "#ffca39", "#a6427c", "#9c2910", "#736519"],
        "sas_corporate":["#00929f", "#f08000", "#90b328", "#3d5aae", "#ffca39", "#a6427c", "#9c2910", "#736519"],
        "sas_hcb":["#7cbf00", "#f77107", "#f1d700", "#bd77ff", "#ff6d65", "#4aacff", "#ff6fbd", "#00d692"],
        "sas_ignite":["#2470ad", "#98863c", "#5954ad", "#985b30", "#238a92", "#84414b", "#17785f", "#985186"],
        "sas_inspire":["#21b9b7", "#4141e0", "#7db71a", "#8e2f8a", "#d38506", "#0abf85", "#2f90ec", "#db3851"]
    }

    @staticmethod
    def lighten(color,offset):
        if len(color) != 7:
            return("#ffffff")

        rgbHex = [color[x:x + 2] for x in [1, 3, 5]]
        rgb = [int(value, 16) + offset for value in rgbHex]
        rgb = [min([255, max([0,i])]) for i in rgb]

        s = "#{0:02x}{1:02x}{2:02x}".format(rgb[0],rgb[1],rgb[2])

        return(s)
    @staticmethod
    def darken(color,offset):
        if len(color) != 7:
            return("#ffffff")

        rgbHex = [color[x:x + 2] for x in [1, 3, 5]]
        rgb = [int(value, 16) - offset for value in rgbHex]
        rgb = [min([255, max([0,i])]) for i in rgb]

        s = "#{0:02x}{1:02x}{2:02x}".format(rgb[0],rgb[1],rgb[2])

        return(s)

    @staticmethod
    def createGradient(**kwargs):
        return(Gradient(**kwargs))

    @staticmethod
    def createGradientColors(**kwargs):
        opts = Options(**kwargs)
        c = Colors.getColorFromName(opts.getOpt("color"))
        num = opts.getOpt("num",10)
        end = opts.getOpt("end",False)
        delta = opts.getOpt("delta",25)
        colors = []

        for i in range(0,num):
            if end:
                colors.insert(0,Colors.lighten(c,i * delta))
            else:
                colors.append(darken(c,i * delta))

        return(colors)

    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def getColorFromName(name):
        if name.find("#") == 0:
            return(name)

        colors = mcolors.get_named_colors_mapping()

        color = None

        if name in colors:
            color = colors[name]

        return(color)

    @staticmethod
    def getLuma(name):
        luma = None
        color = Colors.getColorFromName(name)
        if color != None:
            r = int(color[1:3],16)
            g = int(color[3:5],16)
            b = int(color[5:7],16)

            luma = 0.2126 * r + 0.7152 * g + 0.0722 * b

        return(luma)

    def __init__(self,**kwargs):
        Options.__init__(self,**kwargs)

        if self.hasOpt("colormap"):
            self.createFromColorMap(self.getOpt("colormap"))
        elif self.hasOpt("colors"):
            self.createFromColors(self.getOpt("colors"))

    def createFromColorMap(self,colormap):

        if "clrs" not in sys.modules:
            import plotly.colors as clrs

        colors = []
        colorscale = []
        luma = []

        if colormap != None and len(colormap) > 0:
            if colormap.find("sas_") == 0:
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

    def createFromColors(self,colors):
        if len(colors) < 2:
            raise Exception("must have at least 2 colors")
            
        colorscale = []
        luma = []

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

        self._colors = []
        self._colors.extend(colors)

        self._colorscale = colorscale
        self._luma = luma 

    def getColor(self,name):
        if name.find("#") == 0:
            return(name)

        colors = mcolors.get_named_colors_mapping()

        color = None

        if name in colors:
            color = colors[name]
        elif name == "lightest":
            color = self.lightest
        elif name == "darkest":
            color = self.darkest

        return(color)

    def getColors(self,num,increment):
        index = 0
        colors = []

        for i in range(0,num):
            colors.append(self._colors[index])
            index += increment
            if index == len(self._colors):
                index = 0

        return(colors)

    def createColors(self,values,**kwargs):
        colors = []

        if len(values) == 0:
            return(colors)

        opts = Options(**kwargs)
        minValue = min(values)
        maxValue = max(values)
        range = opts.getOpt("range")

        if range == None:
            range = [minValue,maxValue]

        if opts.hasOpt("gradient"):
            gopts = Options(**opts.getOpt("gradient"))
            c = self.getColor(gopts.getOpt("color","lightest"))
            levels = opts.getOpt("levels",100)
            gradient = Colors.createGradient(color=c,levels=levels,min=range[0],ma=range[1])

            for value in values:
                if gopts.getOpt("end",False):
                    value = maxValue - (value - minValue)
                    colors.append(gradient.lighten(value))
                else:
                    colors.append(gradient.darken(value))
        else:
            a = self._colors

            if opts.hasOpt("colors"):
                a = []
                c = opts.getOpt("colors")

                for cv in c:
                    a.append({"color":cv})

            cr = ColorRange(a,range[0],range[1])

            for value in values:
                colors.append(cr.get(value))

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
    def size(self):
        return(len(self._colors))

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

    @property
    def darkest(self):
        color = None
        if len(self._colors) > 0:
            minLuma = sys.maxsize
            index = -1
            for i,l in enumerate(self._luma):
                if l < minLuma:
                    minLuma = l
                    index = i

            if index >= 0:
                color = self._colors[index]

        return(color)

class ColorRange(object):
    def __init__(self,colors,minv,maxv):
        self._colors = colors
        self._minv = minv
        self._maxv = maxv
        self._a = []
        if self._maxv > self._minv and len(self._colors.colors) > 0:
            self._a = np.arange(self._minv,self._maxv,(self._maxv - self._minv) / len(self._colors.colors))

    def getColor(self,value):
        color = None

        if len(self._a) > 0:
            index = np.where(value >= self._a)[0]
            color = self._colors.colors[len(index) - 1]
        elif len(self._colors.colors) > 0:
            color = self._colors.colors[0]
        else:
            color = "#ffffff"

        return(color)
