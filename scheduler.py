import rdd
import itertools
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


class WorkerHandler(xmlrpclib.ServerProxy):
  def __init__(self, uri, uid):
    xmlrpclib.ServerProxy.__init__(self, uri)
    self.skipcount = 0
    self.uid = uid
    self.uri = uri

  def skip(self):
    self.skipcount += 1

  def reset(self):
    self.skipcount = 0

  def get_skipcount(self):
    return self.skipcount


class Scheduler:
  def __init__(self, hostname, port, max_skipcount = 22):
    self.workers = set()
    self.idle_workers = set()
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
    pass

  def schedule(self, rdd, hash_num):
    ## TODO: decide if we want task-oriented or worker-oriented scheduling
    ## right now: task-oriented
    dependencies = rdd.get_locality_info(hash_num)
    preferred_workers = itertools.chain.from_iterable(dependencies.values())
    assigned_worker = None
    while True:
      if len(preferred_workers) == 0 and len(self.idle_workers) > 0:
        with self.lock:
          assigned_worker = preferred_workers.pop()
          break
      else:
        for worker in self.idle_workers:
          if worker.uid in preferred_workers or
             worker.skipcount == self.max_skipcount:
            with self.lock:
              self.idle_workers.remove(worker)
            assigned_worker = worker
            break
          else:
            worker.skip()
    rdd.worker_assignments[hash_num].append(preferred_worker)
    assigned_worker.reset_skipcount()
    threading.Thread(target = self.dispatch,
                     args = ((rdd, hash_num), assigned_worker, dependencies))


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

  def dispatch(self, task, worker, dependencies):
    """Send a single task to a worker. Blocks until the task either completes or
    fails.
    task -- pair (rdd uid, hash num)
    worker -- WorkerHandler instance
    dependencies -- map (rdd uid, hash num) --> [worker uids]"""
    pass





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

  def register_with_scheduler(self):
    self.proxy.add_worker(self.uid, self.uri)

  def query(self, rdd_id, hash_num):
   if self.data.has_key((rdd_id, hash_num)):
      return self.data[(rdd_id, hash_num)]
   else:
      ## TODO
      raise KeyError("RDD data not present on worker")

  def process(self, serialized_rdd):
    rdd = rdd.deserialize(serialized_rdd)


  def run(self):
    self.server_thread = threading.Thread(target =
                                          self.server.serve_while_alive)
    self.server_thread.start()
    print "Worker %s running" % self.uid

