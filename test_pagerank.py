import threading
import worker
import xmlrpclib
import marshal
import types
import base64
import traceback
import scheduler
import rdd
import time
import socket
import random
# create N clients, different ports
# create one master, give it ports of clients
# have master take one job, split it into multiple partitions, send it to servers that are not busy. P (number of partitions), N (number of Servers): P>N

baseport = 8500
numworkers = 5
workers = []
epsilon = 10e-5
#socket.setdefaulttimeout(2) #no custom timeouts yet... sorry

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

def kill(worker,t):
  time.sleep(t)
  print "killing a worker ..."
  worker.stop_server()

def kill_some(hitlist):
    killers = []
    for i in range(len(hitlist)):
        t = random.random()*10
        killer = threading.Thread(target = kill, args = (workers[hitlist[i]],t,))
        killers.append(killer)
        killers[i].start()

kill_some([1,3,4])


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
    print i
    ## RDD (targetURL, [floats])
    contribs = links.join(ranks, [], 'Z').flatMap(lambda LR: [(dest, LR[1] /
      len(LR[0])) for dest in LR[0]])
    ## RDD
    ranks = contribs.reduceByKey(lambda x, y: x + y, 0).mapValues(lambda s:
      (1 - a) * s).join(damped_ranks, 0, a / N).mapValues(lambda pair: pair[0] + pair[1])
    sched.execute(ranks)
    time.sleep(0.1)
    #workers[0].stop_server()
  return ranks

ranks = pagerank(links, seed_ranks, 22)._get_data()
print ranks
correct_ranks = {'A': 0.03278160674806574, 'C': 0.3458609076996311, 'B': 0.3814496256075537, 'E': 0.08088589507077021, 'D': 0.03908723728670415, 'G': 0.016169498060114126, 'F': 0.03908723728670415, 'I': 0.016169498060114126, 'H': 0.016169498060114126, 'K': 0.016169498060114126, 'J': 0.016169498060114126}
print correct_ranks
failed = False
try:
  assert ranks == correct_ranks
except:
  for k,v in correct_ranks.items():
    if ranks[k] - v > epsilon:
      failed = True
      print "value", ranks[k], "of key",k,"is not",v

if failed:
    print "FAIL"
else:
    print "PASS"

print sched.workers

for worker in workers:
  worker.stop_server()
