from urllib.parse import urlparse

import esppy.espapi.connections as connections

def connect(url,delegate = None,**kwargs):

    u = urlparse(url)

    secure = False

    if u[0] == "https":
        secure = True

    s = u[1].split(":")

    host = s[0]
    port = s[1]

    conn = connections.ServerConnection(host,port,secure,delegate,**kwargs)

    conn.start()

    return(conn)
