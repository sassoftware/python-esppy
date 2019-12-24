import esppy.espapi.connections as connections

def connect(session,delegate = None,**kwargs):

    conn = connections.ServerConnection(session,delegate,**kwargs)
    conn.start()
    return(conn)
