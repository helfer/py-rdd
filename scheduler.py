import time
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
    for parent, dependency in rdd.parents:
      if not parent.fully_scheduled:
        self.execute(parent)
    for hash_num in range(rdd.hash_grain):
      self.schedule(rdd, hash_num)
    rdd.fully_scheduled = True


  def schedule(self, rdd, hash_num):
    ## TODO: decide if we want task-oriented or worker-oriented scheduling
    ## right now: task-oriented. I.e., tasks are strictly scheduled in graph
    ## traversal order
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
          if (worker in preferred_workers or
             worker.skipcount == self.max_skipcount):
            with self.lock:
              self.idle_workers.remove(worker)
            assigned_worker = worker
            break
          else:
            worker.skip()
      time.sleep(0.1)
    rdd.set_assignment(hash_num, preferred_worker)
    assigned_worker.reset_skipcount()
    threading.Thread(target = self.dispatch,
                     args = ((rdd, hash_num), assigned_worker, dependencies))

  def dispatch(self, task, worker, dependencies):
    """Send a single task to a worker. Blocks until the task either completes or
    fails.
    task -- pair (rdd, hash num)
    worker -- WorkerHandler instance
    dependencies -- dictionary (rdd uid, hash num) --> [workers]"""
    ## TODO: worker also needs to know about hash function
    rdd, hash_num = task
    ## serialize compute function
    computation = marshal.dumps(rdd.compute.func_code)
    with self.lock:
      peers = dict([(worker.uid, worker.uri) for worker in workers])
    ## replace WorkerHandler references with uids
    dependencies = dict([(key, worker.uid) for key, value in
      dependencies.items()])
    parent_ids = [parent.uid for parent, dependency in rdd.parents]
    ## Send task to worker and wait for completion
    worker.run_task(rdd.uid, hash_num, computation, parent_ids, peers,
        dependencies)
    ## mark task as complete
    with rdd.lock:
      rdd.task_status[hash_num] = rdd.TaskStatus.Complete
    with self.lock:
      self.idle_workers.add(worker)

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
    self.server.register_function(self.query_by_hash_num)
    self.server.register_function(self.query_by_filter)
    self.server.register_function(self.compute)
    self.server.register_function(self.lookup)
    self.data = {} ## map: (rdd_id, hash_num) -> dict
    self.proxy = xmlrpclib.ServerProxy(scheduler_uri)
    self.uid = uuid.uuid1()
    self.uri = 'http://%s:%d' % (hostname, port)

  def register_with_scheduler(self):
    self.proxy.add_worker(self.uid, self.uri)

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
    return update

  def lookup(self, rdd_id, hash_num, key):
    return self.data[(rdd_id, hash_num)][key]

  def run_task(self, rdd_id, hash_num, computation, parent_ids, peers, dependencies):
    ## TODO
    pass

  def run(self):
    self.server_thread = threading.Thread(target =
                                          self.server.serve_while_alive)
    self.server_thread.start()
    print "Worker %s running" % self.uid

