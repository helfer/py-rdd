import threading
import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import types
import marshal
import base64

#todo: custom timeout
#todo: way of dropping rpc calls before processing or after processing
#todo: way of introducing random delays

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
        self.server.register_function(self.put_data)
        #self.server.register_function()

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

    def transform(self,keys_in,key_out,code_string,data_location = None):
        #unserialize op
        #get data
        #if data is on remote server:
        #print code_string
        code_string = base64.b64decode(code_string)
        code = marshal.loads(code_string)
        func = types.FunctionType(code, globals(), "some_func_name")
        data_in = []
        for key in keys_in:
            if key in self.data:
                data_in.append(self.data[key])
            else:
                data_in.append(self.get_data_remote(data_location[key],key))

        #self.data[key_out] = data_in
        self.data[key_out] = func(data_in)
        #data = data[key]
        #out = map(op,data)
        #data[key] = out

        return "OK"

    def put_data(self,key,value):
        self.data[key] = value
        return "OK"

    def delete_data(self,key):
        del(self.data[key])   
        return "OK" 

    def get_data(self,key):
        if key in self.data:
            return self.data[key]
        else:
            print repr(key)
            print repr(self.data)
            print "sever",self.port,"does not have key",key,"in",self.data
            return []
    def ping(self):
         return "alive, listening on port",self.port 

    def get_data_remote(self,hostport,key):
        host,port = hostport
        #todo: keep the servers in memory, don't redo every time
        s = xmlrpclib.ServerProxy('http://'+host+':'+str(port))
        data = s.get_data(key)
        return data

        pass
        #connect to remote server
        #get data
        #close connection
        #return
    
