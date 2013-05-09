import uuid
import util
import copy
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
    if parents is None:
        parents = []
    self.hash_data = hash_data
    self.hash_function, self.hash_grain = hash_data
    self.parents = parents
    for parent, dependency in parents:
      parent.children.append(self)
    self.children = []
    self.uid = str(uuid.uuid1())
    self.fully_scheduled = False
    self.worker_assignments = collections.defaultdict(list) ## map: hash_num -> [workers]
    self.task_status = collections.defaultdict(lambda : TaskStatus.Unscheduled) ## map: hash_num -> bool
    self.lock = threading.Lock()
    self.fully_scheduled = False

  def set_assignment(self, hash_num, worker):
    self.worker_assignments[hash_num].append(worker)
    self.task_status[hash_num] = TaskStatus.Assigned

  def get_locality_info(self, hash_num):
    """Given a hash number, return the data location of any scheduled or
    completed partitions in parents with narrow dependency."""
    locations = {}
    for parent, dependency in self.parents:
      if dependency == Dependency.Narrow:
        status = parent.task_status[hash_num]
        if status == TaskStatus.Assigned or status == TaskStatus.Complete:
          locations[(parent.uid, hash_num)] = parent.worker_assignments[hash_num]
    return locations

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
  def __init__(self, filename, hash_data = (hash,3)):
    RDD.__init__(self, hash_data)
    self.filename = filename

  def serialize_action(self):
    return self.filename

  @staticmethod
  def unserialize_action(blob):
    filename = blob
    def action(data, hash_num):
      output = {}
      f = open(filename)
      for line in f.readlines():
        key, value = line.split()
        output[key] = value
      f.close()
      return output
    return action

class MapValuesRDD(RDD):
  def __init__(self, function, parent):
    RDD.__init__(self, (parent.hash_function, parent.hash_grain), parents =
        [(parent, Dependency.Narrow)])
    self.function = function

  def serialize_action(self):
    return util.encode_function(self.function)

  @staticmethod
  def unserialize_action(blob):
    function = util.decode_function(blob)
    def action(data, hash_num):
      output = {}
      for key, value in data.items():
        output[key] = function(value)
      return output
    return action

class NullRDD(RDD):
  def __init__(self):
    RDD.__init__(self, (hash, 22))

## etc.

class JoinRDD(RDD):
  def __init__(self,parent1,parent2):
    RDD.__init__(self, (parent1.hash_function,parent1.hash_grain),[(parent1,Dependency.Narrow),(parent2,Dependency.Narrow)])

  def serialize_action(self):
    return ''

  @staticmethod
  def unserialize_action(blob):
    def action(data,hash_num):
      return data
    return action

def simple_hash(key):
    return hash(key)
