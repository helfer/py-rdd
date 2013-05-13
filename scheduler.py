import rdd as rdd_module
import socket
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
    xmlrpclib.ServerProxy.__init__(self, uri, allow_none = True)
    self.skipcount = 0
    self.uid = uid
    self.uri = uri

  def __eq__(self, other):
    return False if other == None else self.uid == other.uid

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
    self.bad_workers = set()
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

  def execute(self, rdd, reexecuting = False):
    for parent in rdd.parents:
      if reexecuting or not parent.fully_scheduled:
        ## If re-executing, just walk the whole graph and check for bad
        ## assignments
        self.execute(parent, reexecuting)

    threads = []
    bad_worker_queue = Queue.Queue()
    for hash_num in range(rdd.hash_grain):
      if (rdd.task_status[hash_num] == rdd_module.TaskStatus.Unscheduled or
          rdd.worker_assignment[hash_num] in self.bad_workers):
        threads.append(self.schedule(rdd, hash_num, bad_worker_queue))
    for thread in threads:
      thread.join()
    if not bad_worker_queue.empty():
      while not bad_worker_queue.empty():
        self.mark_bad_worker(bad_worker_queue.get())
      self.execute(rdd, reexecuting = True)
    rdd.fully_scheduled = True

  def schedule(self, rdd, hash_num, bad_worker_queue):
    preferred_workers = (set(rdd.get_preferred_workers(hash_num)) -
      self.bad_workers)
    assigned_worker = None
    while assigned_worker == None:
      if len(preferred_workers) == 0 and len(self.idle_workers) > 0:
        with self.lock:
          assigned_worker = self.idle_workers.pop()
          break
      else:
        with self.lock:
          for worker in self.idle_workers:
            if (worker in preferred_workers or
               worker.skipcount == self.max_skipcount):
              self.idle_workers.remove(worker)
              assigned_worker = worker
              break
            else:
              worker.skip()
      time.sleep(0.01)
    rdd.set_assignment(hash_num, assigned_worker)
    assigned_worker.reset_skipcount()
    dispatch_thread = threading.Thread(target = self.dispatch,
                     args = ((rdd, hash_num), assigned_worker, bad_worker_queue))
    dispatch_thread.start()
    return dispatch_thread

  def dispatch(self, task, assigned_worker, bad_worker_queue):
    """Send a single task to a worker. Blocks until the task either completes or
    fails.
    task -- pair (rdd, hash num)
    worker -- WorkerHandler instance
    dependencies -- dictionary (rdd uid, hash num) --> [workers]"""
    rdd, hash_num = task
    hash_func = util.encode_function(rdd.hash_function)
    ## replace WorkerHandler references with appropriate uris
    data_src = [parent.worker_assignment[hash_num].uri for parent in rdd.parents]
    parents = [parent.uid for parent in rdd.parents]
    peers = [worker.uri for worker in self.workers]
    rdd_type = pickle.dumps(rdd.__class__)
    ## Send task to worker and wait for completion
    pickled_args = util.pds(rdd.uid, hash_num, rdd_type,
        rdd.serialize_action(), data_src, parents, hash_func, peers)

    error = False
    try:
      task_outcome = assigned_worker.run_task(pickled_args)
      if task_outcome != "OK":
        ## worker encountered another bad worker
        ## and passed its uid in task_outcome
        bad_worker_queue.put(task_outcome)
        error = True
    except socket.timeout:
      ## assume we hit a bad worker
      bad_worker_queue.put(assigned_worker.uid)
      error = True

    if not error:
      ## mark task as complete
      rdd.task_status[hash_num] = rdd_module.TaskStatus.Complete
    ## free worker
    with self.lock:
      self.idle_workers.add(assigned_worker)

  def mark_bad_worker(self, bad_worker_uid):
    for worker in self.workers:
      if worker.uid == bad_worker_uid:
        ## See if it's realy dead
        try:
          worker.ping()
        except socket.timeout:
          with self.lock:
            self.workers.remove(worker)
            self.idle_workers.remove(worker)
            self.bad_workers.add(worker)
        return
