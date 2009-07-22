from digg.transport.cassandra import Cassandra
from digg.exceptions import *
from thrift import Thrift
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from digg.transport.cassandra.ttypes import *

import digg.config as config
import inspect
import random, os
import threading

_CLIENTS = {}
def getClient(name):
    key = str(os.getpid()) + threading.currentThread().getName() + name
    if key in _CLIENTS:
        return _CLIENTS[key]
        
    cass_config = config.factory("cassandra")
    try:
        _CLIENTS[key] = Client(cass_config.servers[name])
        return _CLIENTS[key]
    except Exception, e:
        raise ErrorCassandraClientNotFound
    
class Client(object):
    def __init__(self, servers):
        self._clients = []
        for server in servers:
            host, port = server.split(":")
            self._addServer(host,port)
        
        self._current_server = 0
        
    def _addServer(self, host, port):
        try:
            socket = TSocket.TSocket(host, int(port))
            # socket.setTimeout(200)
            transport = TTransport.TBufferedTransport(socket)
            protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
            client = Cassandra.Client(protocol)
            client.transport = transport
            self._clients.append(client)
        finally:
            return True
                 
        return False
        
    def _getServer(self):
        if self._clients is None:
            raise ErrorCassandraNoServersConfigured
            
        next_server = self._current_server % len(self._clients)
        self._current_server += 1
        
        return self._clients[next_server]
        
    def listServers(self):
        return self._clients
        
    def _connect(self, client):
        """Connect to Cassandra if not connected"""
        if client.transport.isOpen():
            return True
                    
        try:
            client.transport.open()
            return True
        except Thrift.TException, tx:
            if tx.message:
                message = tx.message
            else:
                message = "Transport error, reconnect"
            client.transport.close()
            raise ErrorThriftMessage(message)
        except Exception, e:
            client.transport.close()

        return False
            
    def __getattr__(self, attr):
        """Wrap every __func__ call to Cassandra client and connect()"""
        def func(*args, **kwargs):
            client = self._getServer()
            if self._connect(client):
                try:
                    # print 'Stack', attr, args
                    return getattr(client, attr).__call__(*args, **kwargs)
                except Thrift.TException, tx:
                    if tx.message:
                        message = tx.message
                    else:
                        message = "Transport error, reconnect"
                    client.transport.close()
                    raise ErrorThriftMessage(message)
                except Exception, e:
                    client.transport.close()
                    raise e
                    
        return func