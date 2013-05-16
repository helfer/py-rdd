import scheduler
import random
import rdd
import worker

baseport = random.randint(1000, 9000)
N = 10
workers = []

for i in range(N):
  workers.append(worker.Worker("localhost",baseport+i))
  workers[i].start()


sched = scheduler.Scheduler("localhost",9112)
for i in range(N):
  sched.add_worker("http://%s:%d" % ("localhost",baseport+i),i)
