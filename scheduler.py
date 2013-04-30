import rdd
from Queue import PriorityQueue


class Dependency:
  Narrow, Wide = 0, 1

class Scheduler:
  def __init__(self, workers = [], max_skipcount = 22):
    self.workers = set(workers)
    self.idle_workers = self.workers.copy()
    self.queue = PriorityQueue()
    self.dead = False
    self.max_skipcount = max_skipcount

  def add_worker(self, worker):
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
    raise NotImplementedError


  def worker_listener(self):

