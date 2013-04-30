import worker
import xmlrpclib
# create N clients, different ports
# create one master, give it ports of clients
# have master take one job, split it into multiple partitions, send it to servers that are not busy. P (number of partitions), N (number of Servers): P>N

baseport = 8500
N = 3
servers = []
rpcs = []

for i in range(N):
    w = worker.Worker(baseport+i)
    servers.append(w)

for i in range(N):
    servers[i].start()


for i in range(N):
    rpcs.append(xmlrpclib.ServerProxy('http://localhost:'+str(baseport+i)))


for i in range(N):
    print rpcs[i].ping()

for i in range(N):
    rpcs[i].stop_server()

#for i in range(N):
#    rpcs[i].join()

print "done"
