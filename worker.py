import threading
import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer

class ThreadedRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer):
    pass

class Worker(threading.Thread):

    def __init__(self,port,host="localhost"):
        self.port = port
        self.stop = False
        print "binding to port",port
        threading.Thread.__init__(self)
        self.daemon = True

        self.server = ThreadedRPCServer((host,port))
        self.server.register_function(self.get_data)
        self.server.register_function(self.transform)
        self.server.register_function(self.ping)
        self.server.register_function(self.stop_server)

        self.data = dict()

    def run(self):
        self.stop_flag = False
        while (not self.stop_flag):
            self.server.handle_request()
        #self._Thread__stop()
        #self._stopevent.set()
        #threading.Thread.join(self)


    def stop_server(self):
        self.stop_flag = True
        return "OK"

    def transform(self,key_in,op,key_out,data_location = None):
        #unserialize op
        #get data
        #if data is on remote server:

        data = data[key]
        out = map(op,data)
        data[key] = out

        return "OK"

    def delete_data(self,key):
        del(self.data[key])    

    def get_data(self,key):
        if key in self.data:
            return self.data[key]
        else:
            raise KeyError
    def ping(self):
         return "alive, listening on port",self.port 

    def get_remote(self,host,key):
        pass
        #connect to remote server
        #get data
        #close connection
        #return
    
