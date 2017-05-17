from SimpleXMLRPCServer import SimpleXMLRPCServer
from SimpleXMLRPCServer import SimpleXMLRPCRequestHandler
import xmlrpclib
import sys
import random
import thread
import time
import socket
import marshal, types
import pickle

class RequestHandler(SimpleXMLRPCRequestHandler):
	rpc_paths = ('/mapper','/reducer')






def register(ipaddr, serverport,myport):
	addr = 'http://'+ipaddr+":"+serverport+"/server"
	s = xmlrpclib.ServerProxy(addr)
	print s
	myip = socket.gethostbyname(socket.gethostname())
	res = False

	while res != True:
		try:
			res = s.register(myip,myport)
		except:
			continue

def do_work(func_code,arg):
	print "called from master to work on"
	#func = types.FunctionType(pickle.loads(func_code),globals(),"some_func_name")
	func = pickle.loads(func_code)
	res = func(arg)
	print res
	return res


if __name__ == "__main__":
	print sys.argv
	if len(sys.argv) != 4:
		print "usage: python worker.py <ipaddr> <port> <myport>"
		exit()
	myport = int(sys.argv[3])
	myip = socket.gethostbyname(socket.gethostname())
	server = SimpleXMLRPCServer((myip,myport),requestHandler=RequestHandler)

	server.register_introspection_functions()
	server.register_function(do_work,'do_work')

	#thread.start_new_thread(server.serve_forever())
	#time.sleep(2)
	if register(sys.argv[1],sys.argv[2],myport) == False:
		print "Could not register worker at server. Please try again. Exiting now."
		exit()
	server.serve_forever()
	# while true:
	# 	time.sleep(300)
