import uuid

## for now, each shard of an RDD is just stored as a dictionary on the worker.

## Scheduler-local class representing a single RDD
class RDD:
  def __init__(self, hash_data, parents = None):
    self.hash_function, self.hash_num = hash_data
    self.parents = parents
    for parent in parents:
      parent.add_child(self)
    self.children = []
    self.uid = uuid.uuid1()
    ## Plenty of other ways to do that, but this one's convenient for now.
    self.in_mem = False
    self.worker_assignments = {}

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
