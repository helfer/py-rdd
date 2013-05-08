import worker
import xmlrpclib
import marshal
import types
import base64

import scheduler
import rdd

# create N clients, different ports
# create one master, give it ports of clients
# have master take one job, split it into multiple partitions, send it to servers that are not busy. P (number of partitions), N (number of Servers): P>N

baseport = 8500
N = 3
servers = []
rpcs = []



lines = rdd.TextFileRDD("in.pickle")
sched = scheduler.Scheduler("localhost",8112)
sched.add_worker("w1",1)
sched.add_worker("w1",2)
sched.add_worker("w1",3)
sched.add_worker("w1",4)
sched.execute(lines)


exit()






def g(x):
    print x
    res = []
    for l in x:
        for e in l:
            res.append(e)
   
    print res 
    return res

def add(x):
    res = 0
    for l in x:
        for e in l:
            res = res + e
    
    return res

def bump(x):
    print x
    return [e+1 for e in x[0]]

def rtransform(rpcserver,keys_in,key_out,func,data_location = None):
    code_string = marshal.dumps(func.func_code)
    code_string = base64.b64encode(code_string)
    return rpcserver.transform(keys_in,key_out,code_string,data_location)

for i in range(N):
    w = worker.Worker(baseport+i)
    servers.append(w)

for i in range(N):
    servers[i].start()


for i in range(N):
    rpcs.append(xmlrpclib.ServerProxy('http://localhost:'+str(baseport+i),allow_none=True))


for i in range(N):
    print rpcs[i].ping()


dx = [[1,2,3],[4,5,6],[7,8,9]]
for i in range(N):
    rpcs[i].put_data(str(i),dx[i])

for i in range(N):
    print rpcs[i].get_data(str(i))


    print "collapse " + rtransform(rpcs[0],['0','1','2'],'3',g,{'0':("localhost",baseport),'1':("localhost",baseport+1),'2':("localhost",baseport+2)})

    print rpcs[0].get_data('3')
    print "bump " + rtransform(rpcs[0],['3'],'3',bump)
    
    print "add " + rtransform(rpcs[0],['0','1','2','3'],'4',add,{'0':("localhost",baseport),'1':("localhost",baseport+1),'2':("localhost",baseport+2)})

print rpcs[0].get_data('3')
print rpcs[0].get_data('4')

for i in range(N):
    rpcs[i].stop_server()

#for i in range(N):
#    rpcs[i].join()

print "done"
