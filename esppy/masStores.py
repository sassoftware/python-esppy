#!/usr/bin/env python
# encoding: utf-8
#
# Copyright SAS Institute
#
#  Licensed under the Apache License, Version 2.0 (the License);
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import xml.etree.ElementTree as ET
import esppy

class MasStores(object):
    def __init__(self,esppy):
        self._url = esppy.url
        self._session = esppy.session
        self._stores = {}
        self.load()

    def load(self):
        response = self._session.get(self._url + "masStores")

        xml = ET.fromstring(response.text)

        stores = xml.findall(".//mas-store")

        names = []

        for s in stores:
            name = s.attrib["name"]
            names.append(name)

            if (name in self._stores) == False:
                self._stores[name] = MasStore(name,self)

            self._stores[name].set(s)

        remove = []

        for store in self._stores.keys():
            if (store in names) == False:
                remove.append(store)

        for store in remove:
            del self._stores[store]

    def getStores(self):
        return(self._stores)

    def getStore(self,name):
        store = None
        if name in self._stores:
            store = self._stores[name]
        return(store)

    def createStore(self,name):
        if name in self._stores:
            raise Exception("Mas store " + name + " already exists")

        response = self._session.put(self._url + "masStores/" + name)

        store = MasStore(name,self)

        self._stores[name] = store

        return(store)

    def __str__(self):
        s = ""
        for store in self._stores.values():
            s += "\t" + str(store) + "\n"
        return(s)

class MasStore(object):
    def __init__(self,name,stores):
        self._name = name
        self._stores = stores
        self._versions = {}

    def set(self,xml):
        versions = xml.findall(".//version")

        for v in versions:
            number = v.attrib["number"]
            version = MasStoreVersion(number,self)
            version.set(v)
            self._versions[number] = version

    def getObject(self,name,version="1.0"):
        o = None
        if version in self._versions:
            v = self._versions[version]
            o = v.getObject(name)
        return(o)

    def setObject(self,name,code,version="1.0"):
        url = self._stores._url + "masStoreObjects/" + self._name + "/" + name + "/" + version
        response = self._stores._session.put(url,data=code.encode("utf-8"))
        if response.status_code == 200:
            self._stores.load()
        return(response)

    def setObjectFromFile(self,name,path,version="1.0"):
        response = None
        with open(path) as f:
            contents = f.read()
            response = self.setObject(name,contents,version)
        return(response)

    def delete(self):
        url = self._stores._url + "masStores/" + self._name
        print("URL: " + url)
        response = self._stores._session.delete(url)
        self._stores.load()
        return(response.status_code)

    def __str__(self):
        s = ""
        s += self._name
        s += "\n"
        for version in self._versions.values():
            s += "\t" + str(version) + "\n"
        return(s)

class MasStoreVersion(object):
    def __init__(self,number,store):
        self._number = number
        self._store = store
        self._objects = {}

    def getObject(self,name):
        o = None

        if name in self._objects:
            o = self._objects[name]

        return(o)

    def set(self,xml):
        objects = xml.findall(".//mas-object")

        for o in objects:
            name = o.attrib["name"]
            obj = MasStoreObject(name,self)
            self._objects[name] = obj

    def __str__(self):
        s = ""
        s += self._number
        s += "\n"
        for o in self._objects.values():
            s += "\t" + str(o) + "\n"
        return(s)

class MasStoreObject(object):
    def __init__(self,name,version):
        self._name = name
        self._version = version
  
    @property
    def code(self):
        url = self._version._store._stores._url + "masStoreObjects/" + self._version._store._name + "/" + self._name + "/" + self._version._number
        response = self._version._store._stores._session.get(url)
        content = None
        if response.status_code == 200:
            content = response.text
        return(content)

    def delete(self):
        url = self._version._store._stores._url + "masStoreObjects/" + self._version._store._name + "/" + self._name + "/" + self._version._number
        response = self._version._store._stores._session.delete(url)
        self._version._store._stores.load()
        return(response.status_code)

    @property
    def name(self):
        return(self._name)

    @property
    def version(self):
        return(self._version)

    def __str__(self):
        s = ""
        s += "\t" + self._name + "\n"
        return(s)
