from urllib.parse import urlparse

import esppy.espapi.connections as connections

def connect(url,delegate = None,options = None):

    u = urlparse(url)

    secure = False

    if u[0] == "https":
        secure = True

    s = u[1].split(":")

    host = s[0]
    port = s[1]

    return(connections.ServerConnection(host,port,secure,delegate,options))
