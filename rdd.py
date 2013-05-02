import uuid
import threading
##import collections

## for now, each shard of an RDD should just be stored as a dictionary on the worker.

## Scheduler-local class representing a single RDD
class RDD:
  def __init__(self, hash_data, parents = None):
    self.hash_function, self.hash_grain = hash_data
    self.parents = parents
    for parent in parents:
      parent.add_child(self)
    self.children = []
    self.uid = uuid.uuid1()
    self.in_mem = False
    self.worker_assignments = {} ## map: partition num -> [workers]
    self.done = {} ## map: partition num -> bool
    self.lock = threading.Lock()

  def add_child(self, child):
    self.children.append(child)

  def get_mem_status(self):
    return self.in_mem

  def set_mem_status(self, status):
    self.in_mem = status

  def run(self):
    ## implemented by each derived class
    pass


## For each supported transformation, we have a class derived from RDD

class TextFileRDD(RDD):
  pass


class MapRDD(RDD):
  pass

## etc.
