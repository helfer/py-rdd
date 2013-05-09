import uuid
import util
import rdd
import SocketServer
import threading
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib
import types
import marshal
import scheduler
#todo: custom timeout
#todo: way of dropping rpc calls before processing or after processing
#todo: way of introducing random delays


class ThreadedRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCServer):
    pass

#################
## Worker code ##
#################

class Worker(threading.Thread):
  def __init__(self, hostname, port):#, scheduler_uri):
    self.server = ThreadedRPCServer((hostname,port))
    self.server.register_function(self.query_by_hash_num)
    self.server.register_function(self.query_by_filter)
    self.server.register_function(self.run_task)
    self.server.register_function(self.lookup)
    self.data = {} ## map: (rdd_id, hash_num) -> dict
    #self.proxy = xmlrpclib.ServerProxy(scheduler_uri)
    self.uid = uuid.uuid1()
    self.uri = 'http://%s:%d' % (hostname, port)

    self.port = port
    self.stop = False
    print "binding to port",port
    threading.Thread.__init__(self)
    self.daemon = True

    self.server.register_function(self.read_data)
    self.server.register_function(self.stop_server)

  def __hash__(self):
    return hash(self.uri)

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

  def query_by_hash_num(self, rdd_id, hash_num):
    if self.data.has_key((rdd_id, hash_num)):
      return self.data[(rdd_id, hash_num)]
    else:
      ## TODO
      raise KeyError("RDD data not present on worker")

  def query_by_filter(self, rdd_id, filter_func):
    ## return all key/value pairs in the specified rdd for which filter_func(key) is true.
    func = types.FunctionType(marshal.loads(filter_func), {})
    output = {}
    for key, data in self.data:
      if rdd_id == key[0]:
        output.update([(k, v) for k, v in data.items() if func(k)])
    return output

  def lookup(self, rdd_id, hash_num, key):
    return self.data[(rdd_id, hash_num)][key]

  def run_task(self, pickled_args):
    rdd_id, hash_num, computation, action_args, dependencies = util.pls(pickled_args)
    print "Worker %s running task %s * %s" % (self.uid,rdd_id,hash_num)
    compute_function = util.decode_function(computation)
    ## TODO: Wide dependencies not supported yet
    working_data = {}
    for key in dependencies:
      if not self.data.has_key(key):
        proxy = xmlrpclib.ServerProxy(dependencies[key])
        working_data.update(proxy.query_by_hash_num(key[0], key[1]))
      else:
        working_data.update(self.data[key])
    output = compute_function(working_data, *action_args)
    self.data[(rdd_id, hash_num)] = output

    return "OK"

  def read_data(self,rdd_id,hash_num,part_func,filename):
    print "Worker %s reading %s" % (self.uid,filename)
    return "OK"
