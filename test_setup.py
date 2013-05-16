import scheduler
import rdd
import worker

baseport = 9500
N = 2
workers = []

for i in range(N):
  workers.append(worker.Worker("localhost",baseport+i))
  workers[i].start()


sched = scheduler.Scheduler("localhost",9112)
for i in range(N):
  sched.add_worker("http://%s:%d" % ("localhost",baseport+i),i)
