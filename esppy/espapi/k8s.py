from xml.etree import ElementTree
from urllib.parse import urlparse
from base64 import b64encode, b64decode

import requests
import logging
import time
import json
import sys

import esppy.espapi.tools as tools

from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class K8S(tools.Options):
    def __init__(self,url,esp = None,**kwargs):
        tools.Options.__init__(self,**kwargs)

        self._url = urlparse(url)
        self._esp = esp
        self._proxy = False

        if self._url.scheme.find("-") != -1:

            a = self._url.scheme.split("-")

            if a[1] != "proxy":
                raise Exception("invalid protocol: " + self._url.scheme)

            self._url = self._url._replace(scheme=a[0])
            self._proxy = True

            self._ns = None
            self._project = None
            self._pod = None

        a = []

        if len(self._url.path) > 0:
            a = self._url.path[1:].split("/")

            if len(a) > 0:
                self._ns = a[0]

                if len(a) > 1 and len(a[1]) > 0:
                    self._project = a[1]

    @property
    def httpProtocol(self):
        protocol = ""

        if self._proxy == True:
            protocol = "http:"
        elif self.protocol == "k8s:" or self.protocol == "https:":
            protocol = "https:"
        else:
            protocol = "http:"

        return(protocol)

    @property
    def wsProtocol(self):
        protocol = ""

        if self._proxy == True:
            protocol = "ws:"
        elif self.protocol == "k8s:" or self.protocol == "https:":
            protocol = "wss:"
        else:
            protocol = "ws:"

        return(protocol)

    @property
    def baseUrl(self):
        s = self.httpProtocol + "//" + self.host + ":" + str(self.port) + "/"
        return(s)

    @property
    def baseWsUrl(self):
        s = self.wsProtocol + "//" + self.host + ":" + self.port + "/"
        return(s)

    @property
    def protocol(self):
        return(self._url.scheme + ":")

    @property
    def host(self):
        s = None
        a = self._url.netloc.split(":")
        if len(a) > 0:
            s = a[0]
        return(s)

    @property
    def port(self):
        num = None
        a = self._url.netloc.split(":")
        if len(a) > 1:
            num = int(a[1])
        return(num)

    @property
    def path(self):
        return(self._url.path)

    @property
    def url(self):
        url = self.baseUrl
        url += "apis/iot.sas.com/v1alpha1"
        return(url)

    @property
    def httpUrl(self):
        url = ""

        if self.protocol == "k8s:" or self.protocol == "https:":
            url += "https:"
        else:
            url += "http:"

        if self._proxy != None:
            url += "-proxy"

        url += "://"
        url += self.host
        url += ":"
        url += self.port

        return(url)

    @property
    def k8sUrl(self):
        url = self.k8sProtocol + "//" + self.host + ":" + self.port
        return(url)

    @property
    def namespaceUrl(self):
        if self.namespace == None:
            return(None)
        url = self.k8sUrl + "/" + self.namespace
        return(url)

    @property
    def projectUrl(self):
        if self.namespace == None or self.projet == None:
            return(None)
        url = self.k8sUrl + "/" + self.namespace + "/" + self.project
        return(url)

    @property
    def namespace(self):
        return(self._ns)

    @property
    def project(self):
        return(self._project)

    @property
    def pod(self):
        return(self._pod)

    def getNamespaces(self,delegate):
        if tools.supports(delegate,"handleNamespaces") == False:
            raise Exception("the delegate must implement the handleNamespaces function")

        url = self.baseUrl
        url += "api/v1/namespaces"

        response = requests.get(url)

        a = []

        if response.status_code == 200:
            data = response.json()
            a = data["items"]

        delegate.handleNamespaces(a)

    def getMyProjects(self,delegate):
        return(self.getProjects(delegate,namespace=self.namespace,name=self.project))

    def getProjects(self,delegate,**kwargs):
        if tools.supports(delegate,"handleProjects") == False:
            raise Exception("the delegate must implement the handleProjects function")

        url = self.url

        opts = tools.Options(**kwargs)

        if opts.hasOpt("namespace"):
            url += "/namespaces/" + opts.getOpt("namespace")

        url += "/espservers"

        if opts.hasOpt("name"):
            url += "/" + opts.getOpt("name")

        response = requests.get(url)

        if response.status_code == 404:
            if tools.supports(delegate,"notFound"):
                delegate.notFound()
            else:
                raise Exception("error: not found")
            return

        data = response.json()

        if "code" in data and data["code"] == 404:
            s = ""
            if self._ns != None:
                s += self._ns + "/"
            s += self._project

            if tools.supports(delegate,"notFound"):
                delegate.notFound(s)
            else:
                raise Exception("project not found: " + s)
        else:
            kind = data["kind"]
            a = None

            if kind == "ESPServer":
                a = [data]
            elif kind == "ESPServerList":
                a = data["items"]

            delegate.handleProjects(a)

    def getProject(self,delegate,**kwargs):

        opts = tools.Options(**kwargs)

        namespace = opts.getOpt("namespace")
        name = opts.getOpt("name")

        if (namespace == None or name == None):
            raise Exception("you must specify project namespace and name")

        class Tmp(object):
            def handleProjects(self,projects):
                delegate.handleProject(projects[0])
            def notFound(self):
                if tools.supports(delegate,"notFound"):
                    delegate.notFound()
                else:
                    raise Exception("error: not found")

        self.getProjects(Tmp(),namespace=namespace,name=name)

    def getPods(self):
        url = self.baseUrl
        url += "api/v1"

        if self.namespace != None:
            url += "/namespaces/" + self.namespace

        url += "/pods"

        response = requests.get(url)

        data = response.json()

        pods = None

        if response.status_code == 200:
            pods = []

            if self.project != None:
                match = self.project + "-"
                for item in data["items"]:
                    if item["metadata"]["name"].startswith(match):
                        pods.append(item)
                        break
            else:
                pods = data.items

        return(pods)

    def getPod(self):
        pod = None
        pods = self.getPods()
        if len(pods) == 1:
            pod = pods[0]
        return(pod)

    def getLog(self,delegate):
        if tools.supports(delegate,"handleLog") == False:
            raise Exception("the delegate must implement the handleLog function")

        if self.namespace == None or self.project == None:
            raise Exception("the instance requires both namespace and project name to get the pod")

        class Tmp(object):
            def __init__(self,k8s):
                self._k8s = k8s

            def handlePod(self,pod):
                url = self._k8s.baseUrl
                url += "api/v1"
                url += "/namespaces/" + self._k8s.namespace
                url += "/pods/" + pod["metadata"]["name"]
                url += "/log"

                response = requests.get(url)

                if (response.status_code == 200):
                    log = []
                    o = None

                    for line in response.text.split("\n"):
                        if len(line) > 0:
                            try:
                                o = json.loads(line)
                            except:
                                o = {}
                                o["message"] = line
                            log.append(o)

                    delegate.handleLog(log)
                else:
                    if tools.supports(delegate,"error"):
                        delegate.error(request,error)
                    else:
                        raise Exception("error: " + error)

        self.getPod(Tmp(self))

class K8SProject(K8S):
    def __init__(self,url,esp,**kwargs):
        K8S.__init__(self,url,esp,**kwargs)

        if self._project == None:
            raise Exception("URL must be in form protocol://server/<namespace>/<project>")

        self._config = None

        model = None

        if self.hasOpt("model_file"):
            with open(self.getOpt("model_file")) as reader:
                model = reader.read()
        elif self.hasOpt("model_data"):
            model = self.getOpt("model_data")

        self.authenticate()

        if self.loadConfig() == False:
            if model == None:
                model = self.getDefaultModel()

        if model != None:
            self.load(model)

    def authenticate(self):
        while True:
            status = self.getAuthToken()
            if status == 401:
                user = input("User: ")
                pw = input("Password: ")
                self.setOpt("username",user)
                self.setOpt("password",pw)
            else:
                break

    def getAuthToken(self):

        status = 0

        ingress = self.getIngress("sas-logon-app")

        if ingress is not None:
            o = self.saslogon(ingress)
            if o is not None:
                status = o["status"]
                if "token" in o:
                    self.setOpt("viya",True);
                    self.setOpt("access_token",o["token"])
        else:
            ingress = self.getIngress("oauth2-proxy")
            if ingress is not None:
                ingress = self.getIngress("uaa")
                if ingress is not None:
                    o = self.uaa(ingress)
                    if o is not None:
                        status = o["status"]
                        if "token" in o:
                            self.setOpt("access_token",o["token"])

        return(status)

    def getIngress(self,name):
        url = self.baseUrl
        url += "apis/networking.k8s.io/v1beta1"

        if self.namespace is not None:
            url += "/namespaces/" + self.namespace
        url += "/ingresses/" + name
        response = requests.get(url)
        if response.status_code >= 403:
            return(None)
        else:
            return(json.loads(response.text))

    def saslogon(self,data):

        secret = self.getSecret()

        url = "https://";
        url += data["spec"]["tls"][0]["hosts"][0];
        url += "/SASLogon/oauth/clients/consul";
        url += "?callback=false&serviceId=app";

        ca_bundle = self._esp.ca_bundle

        if ca_bundle != None:
            if ca_bundle == "_noverify_":
                response = requests.post(url,headers={"X-Consul-Token":secret},verify=False);
            else:
                response = requests.post(url,headers={"X-Consul-Token":secret},verify=ca_bundle);
        else:
            response = requests.post(url,headers={"X-Consul-Token":secret});

        if response.status_code >= 400:
            return(None)

        o = json.loads(response.text)

        saslogon = {"status":response.status_code,"token":o["access_token"]}

        return(saslogon)

    def uaa(self,data):
        url = "https://"
        url += data["spec"]["tls"][0]["hosts"][0]
        url += "/uaa/oauth/token"

        user = self.getOpt("username","")
        pw = self.getOpt("password","")

        send = ""
        send += "client_id=sv_client"
        send += "&client_secret=secret"
        send += "&grant_type=password"
        send += "&username=" + user
        send += "&password=" + pw

        ca_bundle = self._esp.ca_bundle

        if ca_bundle != None:
            if ca_bundle == "_noverify_":
                response = requests.post(url,data=send,headers={"Content-Type":"application/x-www-form-urlencoded","Accept":"application/json"},verify=False)
            else:
                response = requests.post(url,data=send,headers={"Content-Type":"application/x-www-form-urlencoded","Accept":"application/json"},verify=ca_bundle)
        else:
            response = requests.post(url,data=send,headers={"Content-Type":"application/x-www-form-urlencoded","Accept":"application/json"})

        if response.status_code >= 400:
            return({"status":response.status_code})

        o = json.loads(response.text)

        uaa = {"status":response.status_code,"token":o["access_token"]}

        return(uaa)

    def getSecret(self):

        secret = None

        url = self.baseUrl;
        url += "api/v1";

        if self.namespace is not None:
            url += "/namespaces/" + self.namespace

        url += "/secrets/sas-consul-client";

        response = requests.get(url)
        if response.status_code >= 404:
            return(None)
        else:
            o = json.loads(response.text)
            if "code" in o and o["code"] == 404:
                return(None)
            else:
                secret = b64decode(o["data"]["CONSUL_HTTP_TOKEN"]).decode()

        return(secret)

    def loadConfig(self):

        self._config = None

        url = self.url

        if self._ns != None:
            url += "/namespaces/" + self._ns

        url += "/espservers"
        url += "/" + self._project

        ca_bundle = self._esp.ca_bundle

        if ca_bundle != None:
            if ca_bundle == "_noverify_":
                response = requests.get(url,verify=False)
            else:
                response = requests.get(url,verify=ca_bundle)
        else:
            response = requests.get(url)

        code = False

        if response.status_code == 200:
            data = json.loads(response.text)

            if "code" in data and data["code"] == 404:
                pass
            else:
                self._config = data
                code = True

        return(code)

    def load(self,model,**kwargs):
        opts = tools.Options(**kwargs)
        xml = ElementTree.fromstring(str(model))
        xml.set("name",self._project)
        model = ElementTree.tostring(xml,method="xml").decode()

        newmodel = "b64" + b64encode(model.encode("utf-8")).decode()

        if newmodel == self.modelXml:
        #if False:
            if opts.getOpt("overwrite",False) == False and opts.getOpt("force",False) == False:
                return

        if self._config != None:
            self.delete()

        url = self.url

        if self._ns != None:
            url += "/namespaces/" + self._ns

        url += "/espservers/"
        url += self._project

        content = self.getYaml(newmodel,pv = False)
        headers = {"content-type":"application/yaml","accept":"application/json"}

        response = requests.post(url,data=content,headers=headers)

        if response.status_code >= 400:
            raise Exception(response.text)

        self.isReady()

    def delete(self):
        url = self.url

        if self._ns != None:
            url += "/namespaces/" + self._ns

        url += "/espservers/"
        url += self._project

        response = requests.delete(url)

    def getYaml(self,model,**kwargs):

        opts = tools.Options(**kwargs)

        s = ""

        s += "apiVersion: iot.sas.com/v1alpha1\n"
        s += "kind: ESPServer\n"
        s += "metadata:\n"
        s += "  name: " + self._project + "\n"
        if self._ns != None:
            s += "  namespace: " + self._ns + "\n"
        s += "spec:\n"
        s += "    loadBalancePolicy: \"default\" \n"
        s += "    espProperties:\n"
        s += "      server.xml: \"" + model + "\"\n"
        s += "      meta.meteringhost: \"sas-event-stream-processing-metering-app." + self._ns + "\"\n"
        s += "      meta.meteringport: \"80\"\n"
        s += "    projectTemplate:\n"
        s += "      autoscale:\n"
        s += "        minReplicas: 1\n"
        s += "        maxReplicas: 1\n"
        s += "        metrics:\n"
        s += "        - type: Resource\n"
        s += "          resource:\n"
        s += "            name: cpu\n"
        s += "            target:\n"
        s += "              type: Utilization\n"
        s += "              averageUtilization: 50\n"
        s += "      deployment:\n"
        s += "        spec:\n"
        s += "          selector:\n"
        s += "            matchLabels:\n"
        s += "          template:\n"
        s += "            spec:\n"

        if (self.getOpt("viya",False) == False):
            s += "               volumes:\n"
            s += "               - name: data\n"
            s += "                 persistentVolumeClaim:\n"
            s += "                   claimName: esp-pv\n"

        s += "               containers:\n"
        s += "               - name: ((PROJECT_SERVICE_NAME))\n"
        s += "                 resources:\n"
        s += "                   requests:\n"
        s += "                     memory: \"1Gi\"\n"
        s += "                     cpu: \"1\"\n"
        s += "                   limits:\n"
        s += "                     memory: \"2Gi\"\n"
        s += "                     cpu: \"2\"\n"

        if (self.getOpt("viya",False) == False):
            s += "                 volumeMounts:\n"
            s += "                 - mountPath: /mnt/data\n"
            s += "                   name: data\n"

        s += "    loadBalancerTemplate:\n"
        s += "      deployment:\n"
        s += "        spec:\n"
        s += "          template:\n"
        s += "            spec:\n"
        s += "              containers:\n"
        s += "              - name: ((PROJECT_SERVICE_NAME)) \n"
        s += "access:\n"
        s += "  state: \"Pending\" \n"
        s += "  internalHostName:  foo\n"
        s += "  internalHttpPort:  0\n"
        s += "  externalURL: foo\n"

        return(s)

    def getDefaultModel(self):
        s = ""
        s += "<project pubsub='auto' threads='4'>\n"
        s += "    <contqueries>\n"
        s += "        <contquery name='cq'>\n"
        s += "            <windows>\n"
        s += "                <window-source name='s'>\n"
        s += "                    <schema>\n"
        s += "                        <fields>\n"
        s += "                            <field name='id' type='string' key='true'/>\n"
        s += "                            <field name='data' type='string'/>\n"
        s += "                        </fields>\n"
        s += "                    </schema>\n"
        s += "                </window-source>\n"
        s += "            </windows>\n"
        s += "        </contquery>\n"
        s += "    </contqueries>\n"
        s += "</project>\n"
        return(s)

    def isReady(self):
        ready = False

        while ready == False:
            state = ""

            if self._config != None:
                state = self._config["access"]["state"]

            if state == "Succeeded":
                pod = self.getPod()
                status = pod["status"]
                phase = status["phase"]

                if "containerStatuses" in status:
                    containerStatus = status["containerStatuses"][0];

                    if "lastState" in containerStatus:
                        lastState = containerStatus["lastState"]

                        if "terminated" in lastState:
                            if lastState["terminated"]["exitCode"] != 0 or lastState["terminated"]["reason"] == "Error":
                                raise Exception(json.dumps(containerStatus,indent=2))

                if phase == "Running":
                    conditions = pod["status"]["conditions"]
                    ready = True
                    for i in range(0,len(conditions)):
                        condition = conditions[i]
                        if condition["status"] != "True":
                            ready = False
                            break
            else:
                self.loadConfig()

            time.sleep(1)

        self.readiness()

    def readiness(self):
        url = self.espUrl
        url += "/internal/ready"

        success = False

        while success == False:
            try:
                response = requests.get(url,verify=False)
                if response.status_code == 200:
                    success = True
            except Exception as e:
                print("exception: " + str(e))

            time.sleep(1)

    @property
    def espUrl(self):
        url = ""
        if self._config != None:
            if self.protocol == "k8s:" or self.protocol == "https:":
                url += "https://"
            elif self.protocol == "k8s-proxy:" or self.protocol == "https-proxy:":
                url += "https://"
            else:
                url += "http://"
            url += self._config["access"]["externalURL"]
            url += "/SASEventStreamProcessingServer"
            url += "/" + self._project

        return(url)

    @property
    def modelXml(self):
        xml = ""
        if self._config != None:
            xml = self._config["spec"]["espProperties"]["server.xml"]
        return(xml)

def create(url,esp = None,**kwargs):
    u = urlparse(url)

    o = None

    if u.path != None and len(u.path.split("/")) == 3:
        o = K8SProject(url,esp,**kwargs)
    else:
        o = K8S(url,esp,**kwargs)

    return(o)
