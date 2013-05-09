import rdd as rdd_module
import util
import itertools
import Queue
import threading
import uuid
import xmlrpclib
import time
import marshal
import pickle
import base64


class WorkerHandler(xmlrpclib.ServerProxy):
  def __init__(self, uri, uid):
    xmlrpclib.ServerProxy.__init__(self, uri)
    self.skipcount = 0
    self.uid = uid
    self.uri = uri

  def skip(self):
    self.skipcount += 1

  def reset_skipcount(self):
    self.skipcount = 0

  def get_skipcount(self):
    return self.skipcount

  def __hash__(self):
    return hash(self.uid)


class Scheduler:
  def __init__(self, hostname, port, max_skipcount = 22):
    self.workers = set()
    self.idle_workers = set()
    self.lock = threading.Lock()
    self.queue = Queue.PriorityQueue()
    self.dead = False
    self.max_skipcount = max_skipcount

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
    preferred_workers = list(itertools.chain.from_iterable(dependencies.values()))
    assigned_worker = None
    while True:
      print "scheduler loops with %d idle workers" % len(self.idle_workers)
      if len(preferred_workers) == 0 and len(self.idle_workers) > 0:
        with self.lock:
          assigned_worker = self.idle_workers.pop()
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
    rdd.set_assignment(hash_num, assigned_worker)
    assigned_worker.reset_skipcount()
    print "worker assigned",str(assigned_worker)
    dispatch_thread = threading.Thread(target = self.dispatch,
                     args = ((rdd, hash_num), assigned_worker, dependencies))
    dispatch_thread.start()

  def dispatch(self, task, assigned_worker, dependencies):
    """Send a single task to a worker. Blocks until the task either completes or
    fails.
    task -- pair (rdd, hash num)
    worker -- WorkerHandler instance
    dependencies -- dictionary (rdd uid, hash num) --> [workers]"""
    rdd, hash_num = task
    ## serialize compute function
    computation = util.encode_function(rdd.get_action())
    ## replace WorkerHandler references with appropriate uris
    dependencies = dict([(key, worker.uri) for key, worker in
      dependencies.items()])
    ## Send task to worker and wait for completion
    print "scheduler calling worker %s" % assigned_worker.uri
    pickled_args = util.pds(rdd.uid, rdd.hash_data,hash_num, computation, rdd.action_args, dependencies)
    assigned_worker.run_task(pickled_args)
    ## mark task as complete
    with rdd.lock:
      rdd.task_status[hash_num] = rdd_module.TaskStatus.Complete
    print "scheduler calling worker %s" % assigned_worker.uri

  #def run(self):
    #self.server_thread = threading.Thread(target = self.server.serve_while_alive)
    #self.server_thread.start()
    #print "scheduler running"
    #pass

