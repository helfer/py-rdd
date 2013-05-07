import uuid
import threading
import collections
import pickle

## for now, each shard of an RDD should just be stored as a dictionary on the worker.

class TaskStatus:
  Unscheduled = 0
  Assigned = 1
  Complete = 2
  Failed = 3

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
    self.worker_assignment = collections.defaultdict(list) ## map: hash_num -> [worker_uids]
    self.task_status = collections.defaultdict(lambda : TaskStatus.Unscheduled) ## map: hash_num -> bool
    self.lock = threading.Lock()

  def get_mem_status(self):
    return self.in_mem

  def set_mem_status(self, status):
    self.in_mem = status

  def assign(self, hash_num, worker_uid):
    self.worker_assignments[hash_num].append(worker_uid)

  def get_locality_info(self, hash_num):
    """Given a hash number, return the data location of any scheduled or
    completed partitions in parents with narrow dependency."""
    locations = {}
    for parent, dependency in self.parents:
      if dependency == Narrow:
        status = parent.task_status[hash_num]
        if status == TaskStatus.Assigned or status == TaskStatus.Complete:
          locations[(parent.uid, hash_num)] = parent.worker_assignment[hash_num]
    return locations


class Dependency:
  Narrow, Wide = 0, 1


## For each supported transformation, we have a class derived from RDD
class TextFileRDD(RDD):
  pass


class MapRDD(RDD):
  pass

## etc.
