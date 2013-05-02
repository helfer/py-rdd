import uuid
import threading
import collections
import pickle

## for now, each shard of an RDD should just be stored as a dictionary on the worker.

## Scheduler-local class representing a single RDD
class RDD:
  def __init__(self, hash_data, parents = None):
    ## parents should be a list of pairs (RDD, dependency_type)
    self.hash_function, self.hash_grain = hash_data
    self.parents = parents
    for parent in parents:
      self.children.append(child)
    self.children = []
    self.uid = uuid.uuid1()
    self.in_mem = False
    self.worker_assignments = collections.defaultdict(list) ## map: hash_num -> [worker_uids]
    self.done = collections.defaultdict(bool) ## map: hash_num -> bool
    self.lock = threading.Lock()

  def get_mem_status(self):
    return self.in_mem

  def set_mem_status(self, status):
    self.in_mem = status

  def assign(self, hash_num, worker_uid):
    self.worker_assignments[hash_num].append(worker_uid)


class Dependency:
  Narrow, Wide = 0, 1


## For each supported transformation, we have a class derived from RDD
class TextFileRDD(RDD):
  pass


class MapRDD(RDD):
  pass

## etc.
