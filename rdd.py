import uuid
import util
import copy
import threading
import collections
import pickle
import xmlrpclib

def simple_hash(key, grain):
    return hash(key) % grain

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
    for parent in parents:
      parent.children.append(self)
    self.children = []
    self.uid = str(uuid.uuid1())
    self.fully_scheduled = False
    self.worker_assignments = collections.defaultdict(list) ## map: hash_num -> [workers]
    self.task_status = collections.defaultdict(lambda : TaskStatus.Unscheduled) ## map: hash_num -> bool
    self.lock = threading.Lock()
    self.fully_scheduled = False

  def __repr__(self):
    output = {}
    for num in range(self.hash_grain):
      proxy = xmlrpclib.ServerProxy(self.worker_assignments[num][0].uri)
      output.update(proxy.query_by_hash_num((self.uid, num)))
    return str(output)


  def set_assignment(self, hash_num, worker):
    self.worker_assignments[hash_num].append(worker)
    self.task_status[hash_num] = TaskStatus.Assigned

  def get_preferred_workers(self, hash_num):
    """Given a hash number, return the data location of any scheduled or
    completed partitions in parents with narrow dependency."""
    locations = []
    for parent in self.parents:
      status = parent.task_status[hash_num]
      if status == TaskStatus.Assigned or status == TaskStatus.Complete:
        locations.extend(parent.worker_assignments[hash_num])
    return locations


  def mapValues(self, function):
    return MapValuesRDD(function, self)

  def join(self, coparent):
    return JoinRDD(self, coparent)

  def flatMap(self, function):
    return PartitionByRDD(IntermediateFlatMapRDD(function, self))

  def reduceByKey(self,function,initializer=None):
    return ReduceByKeyRDD(function, self,initializer)

  def lookup(self, key):
    hash_num = self.hash_function(key)
    return self.worker_assignments[hash_num][0].lookup(self.uid, hash_num, key)

  def map(self, function):
    return PartitionByRDD(IntermediateMapRDD(function, self))

class Dependency:
  Narrow, Wide = 0, 1

## For each supported transformation, we have a class derived from RDD
class TextFileRDD(RDD):
  def __init__(self, filename, function, hash_data = (lambda x: hash(x) % 3,3)):
    RDD.__init__(self, hash_data)
    self.filename = filename
    self.function = function

  def serialize_action(self):
    return self.filename, util.encode_function(self.function)

  @staticmethod
  def unserialize_action(blob):
    filename, function = blob
    function = util.decode_function(function)
    def action(data, hash_num):
      output = collections.defaultdict(list)
      f = open(filename)
      for line in f.readlines():
        key, value = function(line)
        output[key].append(value)
      f.close()
      return output
    return action

class MapValuesRDD(RDD):
  def __init__(self, function, parent):
    RDD.__init__(self, (parent.hash_function, parent.hash_grain), parents =
        [parent])
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
    RDD.__init__(self, (lambda x: hash(x) % 3, 3))


class JoinRDD(RDD):
  def __init__(self,parent1,parent2):
    RDD.__init__(self, (parent1.hash_function,parent1.hash_grain),[parent1, parent2])

  def serialize_action(self):
    return ''

  @staticmethod
  def unserialize_action(blob):
    def action(data,hash_num):
      return data
    return action


class ReduceByKeyRDD(RDD):
  def __init__(self,function,parent,initializer=None):
    RDD.__init__(self,parent.hash_data,[parent])
    self.function = function
    self.initializer = initializer

  def serialize_action(self):
    return (util.encode_function(self.function),self.initializer)

  @staticmethod
  def unserialize_action(blob):
    function = util.decode_function(blob[0])
    initializer = blob[1]
    def action(data, hash_num):
      output = {}
      for key, values in data.items():
        output[key] = reduce(function,values,initializer)
      return output
    return action


class IntermediateFlatMapRDD(RDD):
  def __init__(self, function, parent):
    RDD.__init__(self, parent.hash_data, [parent])
    self.function = function

  def serialize_action(self):
    return util.encode_function(self.function)

  @staticmethod
  def unserialize_action(blob):
    function = util.decode_function(blob)
    def action(data, hash_num):
      output = {}
      for key, seq in data.items():
        for out_key, out_value in map(function, seq):
          ## note: out_key must have type string in order to be sent through
          ## RPC
          output[out_key] = out_value
      return output
    return action


class PartitionByRDD(RDD):
  def __init__(self, parent):
    RDD.__init__(self, parent.hash_data, [parent])

  def serialize_action(self):
    return ''

  @staticmethod
  def unserialize_action(blob):
    def action(data, hash_num):
      return data
    return action
    pass


class IntermediateMapRDD(RDD):
  def __init__(self, function, parent):
    RDD.__init__(self, parent.hash_data, [parent])
    self.function = function

  def serialize_action(self):
    return util.encode_function(self.function)

  @staticmethod
  def unserialize_action(blob):
    function = util.decode_function(blob)
    def action(data, hash_num):
      output = {}
      for key, value in data.items():
          ## note: out_key must have type string in order to be sent through
          ## RPC
          out_key, out_value = function(key, value)
          output[out_key] = out_value
      return output
    return action
