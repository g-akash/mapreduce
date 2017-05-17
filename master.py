from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import xmlrpclib
import sys
import time
import socket
from threading import Thread
import thread
from mapper import mapper
from reducer import reducer
from os import listdir
from os.path import isfile, join
import marshal
import pickle
import os



class RequestHandler(SimpleXMLRPCRequestHandler):
	rpc_paths = ('/server','/master')

class worker:
	def __init__(self,ipaddr,port):
		self.ipaddr = ipaddr
		self.port = str(port)


workers = []

def register(worker_ip,worker_port):
	client = worker(worker_ip,worker_port)
	if client in workers:
		return False
	workers.append(client)
	return True

def mapper_thread(next_worker,data_folder,temp_folder,file,mapper_code):
	global workers
	print "mapper called"
	addr = "http://"+next_worker.ipaddr+":"+next_worker.port+"/mapper"
	s = xmlrpclib.ServerProxy(addr)
	f = open(data_folder+"/"+file)
	data = f.read().splitlines()
	res = s.do_work(mapper_code,data)

	for k in res:
		f = open(temp_folder+"/"+k+".txt",'a+')
		f.write(str(res[k])+"\n")
		f.close()
	workers.append(next_worker)
	return


def reducer_thread(next_worker,temp_folder,file,reducer_code,result_file):
	global workers
	print "reducer called"
	addr = "http://"+next_worker.ipaddr+":"+next_worker.port+"/reducer"
	s = xmlrpclib.ServerProxy(addr)
	f = open(temp_folder+"/"+file)
	data = f.read().splitlines()
	res = s.do_work(reducer_code,data)
	f = open(result_file,'a+')
	payload = file[:-4]+" "+str(res)
	f.write(payload+"\n")
	f.close()
	workers.append(next_worker)
	return

def schedule(data_folder,temp_folder,result_file):
	global workers
	data_files = [f for f in listdir(data_folder) if isfile(join(data_folder, f))]
	mapper_code = pickle.dumps(mapper)
	reducer_code = pickle.dumps(reducer)
	threads = []
	print workers
	for file in data_files:
		while len(workers)==0:
			time.sleep(2)

		next_worker = workers[0]
		workers=workers[1:]
		t = Thread(target=mapper_thread,args=(next_worker,data_folder,temp_folder,file,mapper_code,))
		t.start()
		threads.append(t)

	for t in threads:
		t.join()
	threads = []
	temp_files = [f for f in listdir(temp_folder) if isfile(join(temp_folder, f))]




	for file in temp_files:
		while len(workers)==0:
			time.sleep(2)

		next_worker = workers[0]
		workers = workers[1:]
		

		t = Thread(target=reducer_thread,args=(next_worker,temp_folder,file,reducer_code,result_file,))
		t.start()
		threads.append(t)

	for t in threads:
		t.join()
	print "done"

	return


if __name__=="__main__":
	if len(sys.argv)!=7:
		print "usage: python master.py <port> <data_folder> <temp_folder> <result_file> mapper.py reducer.py"
		exit()
	#do the checking here to see if mapper and reducer exists
	temp_folder = sys.argv[3]
	temp_files = [f for f in listdir(temp_folder) if isfile(join(temp_folder, f))]
	for file in temp_files:
		os.unlink(temp_folder+"/"+file)

	myport = int(sys.argv[1])
	#myip = socket.gethostbyname(socket.gethostname())
	myip = "127.0.0.1"
	server = SimpleXMLRPCServer((myip,myport),requestHandler=RequestHandler)

	server.register_introspection_functions()
	server.register_function(register)
	thread.start_new_thread(server.serve_forever,())
	time.sleep(3)
	schedule(sys.argv[2],sys.argv[3],sys.argv[4])



