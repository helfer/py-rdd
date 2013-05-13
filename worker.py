import uuid
import collections
import pickle
import util
import rdd
import SocketServer
import threading
from SimpleXMLRPCServer import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import xmlrpclib
import types
import marshal
import scheduler
#todo: custom timeout
#todo: way of dropping rpc calls before processing or after processing
#todo: way of introducing random delays

class SimpleXMLRPCWrapper(SimpleXMLRPCServer):
  def __init__(self, addr):
    SimpleXMLRPCServer.__init__(self, addr, Handler)

class ThreadedRPCServer(SocketServer.ThreadingMixIn,SimpleXMLRPCWrapper):
    pass


class Handler(SimpleXMLRPCRequestHandler):
  def _dispatch(self, method, params):
    try:
      return self.server.funcs[method](*params)
    except:
      import traceback
      traceback.print_exc()
      raise

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
    self.data = collections.defaultdict(dict) ## map: (rdd_id, hash_num) -> dict
    #self.proxy = xmlrpclib.ServerProxy(scheduler_uri)
    self.uid = str(uuid.uuid1())
    self.uri = 'http://%s:%d' % (hostname, port)

    self.port = port
    self.stop = False
    print "binding to port",port
    threading.Thread.__init__(self)
    self.daemon = True

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
      return {}

  def query_by_filter(self, rdd_id, filter_func):
    ## return all key/value pairs in the specified rdd for which filter_func(key) is true.
    func = util.decode_function(filter_func)
    output = {}
    for key, data in self.data:
      if rdd_id == key[0]:
        output.update([(k, v) for k, v in data.items() if func(k)])
    return output

  def lookup(self, rdd_id, hash_num, key):
    return self.data[(rdd_id, hash_num)][key]

  def run_task(self, pickled_args):
    (rdd_id, hash_num, rdd_type, action, dependencies, parents, hash_func,
        peers) = util.pls(pickled_args)
    rdd_type = pickle.loads(rdd_type)
    action = rdd_type.unserialize_action(action)
    hash_func = util.decode_function(hash_func)
    filter_func = util.encode_function(lambda key: hash_func(key) == hash_num)
    working_data = collections.defaultdict(list)
    if rdd_type == rdd.PartitionByRDD:
      print "querying"
      for peer in peers:
        if peer != self.uri:
          proxy = xmlrpclib.ServerProxy(peer)
        else:
          proxy = self
        print parents
        for parent_uid in parents:
          queried_data = proxy.query_by_hash_num(parent_uid, hash_num)
          print "queried_data", queried_data
          for k, v in queried_data.items():
            if type(v) == list:
              working_data[k].extend(v)
            else:
              working_data[k].append(v)
    else:
      for dep_key in dependencies:
        if not self.data.has_key(dep_key):
          print "Querying remote server"
          proxy = xmlrpclib.ServerProxy(dependencies[dep_key][0])
          for k, v in proxy.query_by_hash_num(dep_key[0], dep_key[1]).items():
            if type(v) == list:
              working_data[k].extend(v)
            else:
              working_data[k].append(v)
        else:
          for k, v in self.data[dep_key].items():
            if type(v) == list:
              working_data[k].extend(v)
            else:
              working_data[k].append(v)
    print "processing"
    output = action(working_data, hash_num)
    if rdd_type == rdd.IntermediateFlatMapRDD:
      print output
      ## Split output into partial partitions
      for k, v in output.items():
        self.data[(rdd_id, hash_func(k))][k] = v
    else:
      self.data[(rdd_id, hash_num)] = output

    return "OK"
