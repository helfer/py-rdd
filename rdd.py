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
    self.scheduled = False
    self.worker_assignment = collections.defaultdict(list) ## map: hash_num -> [workers]
    self.task_status = collections.defaultdict(lambda : TaskStatus.Unscheduled) ## map: hash_num -> bool
    self.lock = threading.Lock()

  def set_assignment(self, hash_num, worker):
    self.worker_assignments[hash_num].append(worker)
    self.task_status[hash_num] = TaskStatus.Assigned

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

  def compute(self, data):
    ## implemented by derived classes
    pass

  def map(self, function):
    return MapValuesRDD(function, self)

  def join(self, coparent):
    return JoinRDD(self, coparent)

  def lookup(self, key):
    hash_num = self.hash_function(key)
    return self.worker_assignments[hash_num][0].lookup(self.uid, hash_num, key)

class Dependency:
  Narrow, Wide = 0, 1


## For each supported transformation, we have a class derived from RDD
class TextFileRDD(RDD):
  pass

class MapValuesRDD(RDD):
  def __init__(self, function, parent):
    RDD.__init__(self, (parent.hash_function, parent.hash_grain), parents =
        [(parent, Dependency.Narrow)])
    self.function = function

  def compute(self, data):
    output = {}
    for key, value in data:
      output[key] = self.function(data)
    return output

class JoinRDD(RDD):
  def __init__(self, parent1, parent2):
    RDD.__init__(self, (parent1.hash_function, parent1.hash_grain), parents =
        [(parent1, Dependency.Narrow), (parent2, Dependency.Narrow)])

  ## TODO

## etc.
