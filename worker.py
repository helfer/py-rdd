import uuid
import socket
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
import time
#todo: custom timeout
#todo: way of dropping rpc calls before processing or after processing
#todo: way of introducing random delays

class SimpleXMLRPCWrapper(SimpleXMLRPCServer):
  def __init__(self, addr):
    SimpleXMLRPCServer.__init__(self, addr, Handler, logRequests = False)

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
    self.lock = threading.Lock()
    self.transport = util.TimeoutTransport()
    self.transport.set_timeout(0.5)
    self.server = ThreadedRPCServer((hostname,port))
    self.server.register_function(self.query_by_hash_num)
    self.server.register_function(self.query_by_filter)
    self.server.register_function(self.run_task)
    self.server.register_function(self.lookup)
    self.server.register_function(self.ping)
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
    with self.lock:
      self.stop_flag = False
    while (not self.stop_flag):
      self.server.handle_request()

  def ping(self):
    return "OK"


  def stop_server(self):
    with self.lock:
      self.stop_flag = True
      return "OK"

  def query_by_hash_num(self, key):
    with self.lock:
      rdd_id, hash_num = key
      if self.data.has_key((rdd_id, hash_num)):
        return (True,self.data[(rdd_id, hash_num)].items())
      else:
        return (False,{})

  def query_by_filter(self, rdd_id, filter_func):
    ## return all key/value pairs in the specified rdd for which filter_func(key) is true.
    raise NotImplemented()
    #with self.lock:
    #  func = util.decode_function(filter_func)
    #  output = {}
    #  for key, data in self.data:
    #    if rdd_id == key[0]:
    #      output.update([(k, v) for k, v in data.items() if func(k)])
    #  return output

  def lookup(self, rdd_id, hash_num, key):
    with self.lock:
      return self.data[(rdd_id, hash_num)][key]

  def query_remote(self,key,proxy,default=None):
    #time.sleep(0.2)
    ok, queried_data = proxy.query_by_hash_num(key)
    if ok:
      return dict(queried_data)
    else:
      if default is None:
        raise KeyError("remote data not found")
      else:
##        print "remote data not found, using default"
        return {}

  def run_task(self, pickled_args):
    (rdd_id, hash_num, rdd_type, action, data_src, parents, hash_func,
        peers) = util.pls(pickled_args)
    rdd_type = pickle.loads(rdd_type)
    action = rdd_type.unserialize_action(action)
    hash_func = util.decode_function(hash_func)
    filter_func = util.encode_function(lambda key: hash_func(key) == hash_num)

    if rdd_type == rdd.JoinRDD:
      working_data = [{}, {}]
      for index in [0, 1]:
        parent_uid = parents[index]
        assignment = data_src[index]
        key = (parent_uid, hash_num)
        with self.lock:
          data_is_local = self.data.has_key(key)
        if not data_is_local:
#          print "Join: Querying remote server"
          proxy = xmlrpclib.ServerProxy(assignment,transport=self.transport)
          try:
            working_data[index] = self.query_remote(key,proxy)
          except (socket.timeout,KeyError):
#            print "timeout or key error"
            return assignment
        else:
          with self.lock:
            working_data[index] = self.data[key]
      with self.lock:
        self.data[(rdd_id, hash_num)] = action(working_data[0], working_data[1])
      return "OK"

    if rdd_type == rdd.PartitionByRDD:
      working_data = collections.defaultdict(list)
      for peer in peers:
        if peer != self.uri:
          proxy = xmlrpclib.ServerProxy(peer,transport=self.transport)
        else:
          proxy = self
        for parent_uid in parents:
          key = (parent_uid, hash_num)
          try:
            queried_data = self.query_remote(key,proxy,{})
          except socket.timeout:
            return peer
          #print queried_data
          try:
            for k, v in queried_data.items():
              if type(v) == list:
                working_data[k].extend(v)
              else:
                working_data[k].append(v)
          except ValueError as e:
            print key,queried_data
            raise e
    elif len(parents) > 0:
      ## number of parents should be 1
      parent_uid = parents[0]
      assignment = data_src[0]
      key = (parent_uid, hash_num)
      with self.lock:
        data_is_local = self.data.has_key(key)
      if not data_is_local:
#        print "Querying remote server"
        proxy = xmlrpclib.ServerProxy(assignment,self.transport)
        try:
          working_data = self.query_remote(key,proxy)
        except (socket.timeout , KeyError):
#          print "fetch timeout or KeyError"
          return assignment
      else:
        with self.lock:
          working_data = self.data[key]
    else:
      working_data = {}
    output = action(working_data, hash_num)
    if rdd_type == rdd.IntermediateFlatMapRDD:
      ## Split output into partial partitions
      for k, v in output.items():
        ## v should be a list
        key = (rdd_id, hash_func(k))
        with self.lock:
          if self.data[key].has_key(k):
            self.data[key][k].extend(v)
          else:
            self.data[key][k] = v
    else:
      with self.lock:
        self.data[(rdd_id, hash_num)] = output

    return "OK"
