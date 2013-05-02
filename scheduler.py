import rdd
import Queue
import threading
import uuid
import xmlrpclib
##import SocketServer
from SimpleXMLRPCServer import SimpleXMLRPCServer

class RPCServer(SimpleXMLRPCServer):
  ## TODO: do we need the threading mixin here?
  def __init__(self, addr):
    SimpleXMLRPCServer.__init__(self, addr)
    self.dead = False
  def serve_while_alive(self):
    while not self.dead:
      self.handle_request()

  def kill(self):
    self.dead = True
    self.server_close()


class Dependency:
  Narrow, Wide = 0, 1


class WorkerHandler:
  def __init__(self, uri, uid):
    self.skipcount = 0
    self.proxy = xmlrpclib.ServerProxy(uri)
    self.uid = uid
    self.uri = uri
    ## TODO

  def skip(self):
    self.skipcount += 1

  def reset(self):
    self.skipcount = 0

  def get_skipcount(self):
    return self.skipcount

class Scheduler:
  def __init__(self, hostname, port, max_skipcount = 22):
    self.workers = {}
    self.idle_workers = {}
    self.lock = threading.Lock()
    self.queue = Queue.PriorityQueue()
    self.dead = False
    self.max_skipcount = max_skipcount

    self.server = RPCServer((hostname, port))
    self.server.register_function(self.free_worker)
    self.server.register_function(self.log_completion)
    self.server.register_function(self.add_worker)

  def add_worker(self, worker_uri, worker_uid):
    worker = WorkerHandler(worker_uri, worker_uid)
    self.workers.add(worker)
    self.idle_workers.add(worker)

  def remove_worker(self, worker):
    self.workers.remove(worker)
    try:
      self.idle_workers.remove(worker)
    except KeyError:
      pass

  def execute(self, rdd):
    ## backtrack in the lineage graph from rdd
    ## until all parents are either in-memory
    ## or root nodes
    if rdd.get_mem_status():
      return
    elif len(rdd.parents) == 0:
      schedule(rdd)
    else:
      for parent in rdd.parents:
        parent.execute()

  def schedule(self, rdd):
    ## for now each parent RDD has to complete before we launch any part of
    ## this one
    unfinished_parents = True
    while unfinished_parents:
      unfinished_parents = False
      for parent in rdd.parents:
        if not parent.get_mem_status():
          unfinished_parents = True
          ## TODO: sleep here?
          break
    ## TODO

  def free_worker(self, worker):
    with self.lock:
      self.idle_workers.add(worker)

  def log_completion(self, rdd, hash_num):
    with rdd.lock:
      rdd.done[hash_num] = True
      if len(rdd.done) == rdd.hash_grain:
        rdd.set_mem_status(True)

  def run(self):
    self.server_thread = threading.Thread(target =
                                          self.server.serve_while_alive)
    self.server_thread.start()
    print "scheduler running"


#################
## Worker code ##
#################

class Worker:
  def __init__(self, hostname, port, scheduler_uri):
    self.server = RPCServer((hostname, port))
    self.server.register(self.query)
    self.data = {} ## map: (rdd_id, hash_num) -> dict
    self.proxy = xmlrpclib.ServerProxy(scheduler_uri)
    self.uid = uuid.uuid1()
    self.uri = 'http://%s:%d' % (hostname, port)

  def register(self):
    self.proxy.add_worker(self.uid, self.uri)

  def query(self, rdd_id, hash_num):
   if self.data.has_key((rdd_id, hash_num)):
      return self.data[(rdd_id, hash_num)]
   else:
      ## TODO
      raise KeyError("RDD data not present on worker")

  def process(self):
    ## TODO
    pass
