
import worker
import xmlrpclib
import marshal
import types
import base64
import traceback
import scheduler
import rdd
import time

# create N clients, different ports
# create one master, give it ports of clients
# have master take one job, split it into multiple partitions, send it to servers that are not busy. P (number of partitions), N (number of Servers): P>N

baseport = 8500
numworkers = 2
workers = []

def getkv(string):
  k, v = string.split()
  v = int(v)
  return k, v

for i in range(numworkers):
  workers.append(worker.Worker("localhost",baseport+i))
  workers[i].start()

sched = scheduler.Scheduler("localhost",8112)
for i in range(numworkers):
  sched.add_worker("http://%s:%d" % ("localhost",baseport+i),i)

N = 11
a = 0.15


## RDD of (url, [link_destinations])
links = rdd.TextFileRDD('./pagerank_data.txt', lambda line:
    line.split(), multivalue = True, scheduler = sched)
## RDD of (url, rank)
seed_ranks = rdd.TextFileRDD('./urls.txt', lambda line: (line.strip(), 1. /
  N), scheduler = sched)

def pagerank(links, seed_ranks, iterations):
  damped_ranks = seed_ranks.mapValues(lambda x: a / N)
  ranks = seed_ranks
  for i in range(iterations):
    ## RDD (targetURL, [floats])
    contribs = links.join(ranks, [], 'Z').flatMap(lambda LR: [(dest, LR[1] /
      len(LR[0])) for dest in LR[0]])
    ## RDD
    ranks = contribs.reduceByKey(lambda x, y: x + y, 0).mapValues(lambda s:
      (1 - a) * s).join(damped_ranks, 0, a / N).mapValues(lambda pair: pair[0] + pair[1])
    sched.execute(ranks)
    time.sleep(0.1)
  return ranks

##pagerank(links, seed_ranks, 3)
