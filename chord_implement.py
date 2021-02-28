import zmq
import zlib
import random
import threading
from collections import namedtuple
import time
import json
node = namedtuple('node',['addr','id'])

max_bits = 10
def repeat_and_sleep(sleep_time):
	def decorator(func):
		def inner(self, *args, **kwargs):
			while 1:
				time.sleep(sleep_time)
				ret = func(self, *args, **kwargs)
				if ret == 0:
					return
		return inner
	return decorator

def do_hash(addr):
	return zlib.crc32(addr.encode()) % (1 << max_bits)

class Chord:
	def __init__(self,identifier,localhost):
		self.identifier = identifier
		self.context = zmq.Context()
		self.req_sock = self.context.socket(zmq.REQ)
		self.answer_sock = self.context.socket(zmq.REP)
		self.answer_sock.bind('tcp://'+localhost)
		self.finger_table = [node(localhost,do_hash(localhost))]*max_bits
		self.successor = node(localhost,do_hash(localhost))
		self.predecessor = None
		self.he_is_death = False
		self.keys = {}
		self.addr = localhost
		self.id = do_hash(localhost)
		self.init_servers()

	def give_me_the_successor(self):
		return json.dumps(str(self.successor.addr))

	def in_range(self,k,a,b):
		if a <= k and b >= k:
			return True
		if b <= a:
			return a <= k or b >= k
		return False

	def find_predecessor(self,k,sock = None):
		if not sock:
			sock = self.req_sock
		if not(self.predecessor is None):
			if(k == self.id):
				return json.dumps(str(self.predecessor.addr))
			
		aux = (self.id,self.addr, self.successor.id)
		while (not self.in_range(int(k),int(aux[0])+1,int(aux[2]))):
			if(int(aux[0]) != self.id):
				last = json.loads(self.conect_to(aux[1],'closest_preceding_finger ' + str(k),sock)).split()
				if(int(last[0]) == int(aux[0])):
					aux = last
					break
				aux = last
			else:
				last = self.closest_preceding_finger(k,sock).split()
				if(int(last[0]) == int(aux[0])):
					aux = last
					break
				aux = last	
		return json.dumps(str(aux[1]))

	def closest_preceding_finger(self,k,socket = None):
		if not socket:
			sock = self.req_sock

		for x in range(0,max_bits):
			if (self.in_range(int(self.finger_table[x].id),int(self.id)+1 ,int(k)-1)):
				addr = self.finger_table[x].addr
				temp = json.loads(self.conect_to(addr,'give_me_the_successor ',socket))
				return str(self.finger_table[x].id) + ' ' + addr + ' ' + str(do_hash(temp))
		return str(self.id) + ' ' +self.addr + ' ' + str(self.id)

	def update_pred(self,addr):
		self.predecessor = node(addr,do_hash(addr))
		return "Ok"

	def update_succ(self,addr):
		self.successor = node(addr,do_hash(addr))
		return "Ok"
	##########################################################################
	@repeat_and_sleep(2)
	def stabilize(self):
		sock = self.context.socket(zmq.REQ)
		pred = json.loads(self.conect_to(self.successor.addr,'find_predecessor '+str(self.successor.id),sock))
		value = do_hash(pred)
		if(self.in_range(int(value), int(self.id)+1, int(self.successor.id)-1) or self.id == self.successor.id):
			self.successor = node(pred,value)
		self.conect_to(self.successor.addr,'notify '+self.addr,sock)
		return 1

	def notify(self,addr_n):
		value = do_hash(addr_n)
		if self.predecessor is None:
			if addr_n != self.addr:
				self.predecessor = node(addr_n,do_hash(addr_n))
		elif (self.in_range(int(value), int(self.predecessor.id)+1, int(self.id)-1)):
			self.predecessor = node(addr_n,do_hash(addr_n))			
		return "Ok"
	
	@repeat_and_sleep(2)
	def fix_fingers(self):
		sock = self.context.socket(zmq.REQ)
		sock1 = self.context.socket(zmq.REQ)
		i = random.randrange(max_bits)
		pred = json.loads(self.find_predecessor(str(self.calculate_pos(self.id,i)),sock1))
		succ = json.loads(self.conect_to(pred,'give_me_the_successor ',sock))
		self.finger_table[i] = node(succ,do_hash(succ))
		return 1

	@repeat_and_sleep(2)
	def replication_keys(self):
		sock = self.context.socket(zmq.REQ)
		for x in self.keys.keys():
			if self.in_range(x,self.predecessor.id,self.id):
				self.conect_to(self.successor.addr,'ok_ok_take_my_keys ' + str(x) + ' ' + json.dumps(self.keys[x]),sock)
		return 1

	@repeat_and_sleep(1)
	def print_values(self):
		print("predecessor ")
		print(self.predecessor)
		print("my id " + str(self.id))
		print("successor ")
		print(self.successor)
		print("keys:")
		print(self.keys)
		return 1

	###recupera las llaves que fueron asignadas al sucesor de n se asignan a n
	def join_node(self,addr = None):
		if addr:
			print("join_node--------------------------------------------------------")
			pred = json.loads(self.conect_to(addr,'find_predecessor ' + str(do_hash(self.addr))))
			succ = json.loads(self.conect_to(pred,'give_me_the_successor '))
			self.successor = node(succ,do_hash(succ))
			list1 = json.loads(self.conect_to(succ,'give_me_the_keys '+str(succ)))
			for x in list1:
				self.keys[x] = list1[x]
			self.predecessor = None

	def give_me_the_keys(self,addr):
		for_remove = {}
		for x in self.keys:
			if(x < do_hash(addr)):
				for_remove[x] = self.keys[x]
			
		return json.dumps(for_remove)

	def ok_ok_take_my_keys(self,k,list1):
		self.keys[int(k)] = list1
		return "Ok"
##########################################sin stabilize
	def update_others(self):
		for x in range(0,max_bits):
			s = json.loads(self.find_predecessor(self.id - 2^x))
			if(self.id != do_hash(s)):
				self.conect_to(s,'update_finger_table '+ str(self.id) + ' ' + str(x) + ' ' + self.addr)
			else:
				self.update_finger_table(self.id,x,self.addr)
		return "Ok"

	def update_finger_table(self,id1,x,addr):
		if int(id1) >= self.id and self.finger_table[x].id > int(id1):
			self.finger_table[x] = node(addr,int(id1))
			s = self.predecessor.addr
			self.conect_to(s,'update_finger_table '+ str(id1) + ' ' + str(x) + ' ' + addr)
			return "Ok"

	def init_finger_table(self,addr):
		aux = json.loads(self.conect_to(addr,'give_me_the_successor '))
		self.successor = node(aux,do_hash(aux))	
		self.finger_table[0] = node(self.successor.addr,self.successor.id)
		aux = do_hash(addr)
		for x in range(1,max_bits-1):
			pos = self.calculate_pos(self.id,x)
			if (pos >= aux and pos < self.finger_table[x].id):
				self.finger_table[x+1] = node(self.finger_table[x].addr,self.finger_table[x].id)
			else:
				temp=json.loads(self.conect_to(addr,'find_predecessor ' + str(pos)))
				temp = json.loads(self.conect_to(temp,'give_me_the_successor '))
				self.finger_table[x+1] = node(temp,do_hash(temp))

	def update_key(self,k,data):
		pred = json.loads(self.find_predecessor(k))
		node_addr = json.loads(self.conect_to(pred,'give_me_the_successor '))
		self.conect_to(node_addr,'remote_update_key ' + str(k) + ' ' + data)
		return "Ok"

	def remote_update_key(self,k,data):
		if k in self.keys:
			self.keys[k] = data
			return "Ok"
		print("Invalid key")
		return "No"

	def calculate_pos(self,n,pos):
		return ((n+2^(pos))%2^max_bits)

	###envia todas las llaves hacia su sucesor para no perderlas
	def leave_network(self):
		self.he_is_death = True
		self.conect_to(self.successor.addr,'ok_ok_take_my_keys ' + json.dumps(self.keys))
		self.conect_to(self.successor.addr,'update_pred ' + self.predecessor.addr)
		self.conect_to(self.predecessor.addr,'update_succ ' + self.successor.addr)
		self.conect_to(self.successor.addr, 'remote_update_others ')

	def add_key(self,k,data):
		pred = json.loads(self.find_predecessor(k))
		node_addr = json.loads(self.conect_to(pred,'give_me_the_successor '))
		if node_addr == self.addr:
			self.keys[k] = data
			return "Ok"
		self.conect_to(node_addr,'add_remote_key ' + str(k) +' '+ data)
		return "Ok"

	def add_remote_key(self,k,data):
		self.keys[k] = data
		return "Ok"

	def ask_for_a_key(self,k):
		pred = json.loads(self.find_predecessor(k))
		node_addr = json.loads(self.conect_to(pred, 'give_me_the_successor '))
		data = json.loads(self.conect_to(node_addr,'remote_ask_for_a_key ' + str(k)))
		return data			

	def remote_ask_for_a_key(self,k):
		if(k in self.keys):
			return self.keys[k]
		return "Invalid key"

	def conect_to(self,addr,data,socket = None):
		if(not socket):
			self.req_sock.connect('tcp://'+addr)
			self.req_sock.send_string(data)
			req_result = self.req_sock.recv_string()
			self.req_sock.disconnect('tcp://'+addr)
		else:
			socket.connect('tcp://'+addr)
			socket.send_string(data)
			req_result = socket.recv_string()
			socket.disconnect('tcp://'+addr)
		return req_result

	def wait_conections(self):
		while 1:
			req = self.answer_sock.recv_string()
			req = req.split()
			if req[0] == 'find_predecessor':
				self.answer_sock.send_string(str(self.find_predecessor(int(req[1]))))
			elif req[0] == 'update_key':
				self.answer_sock.send_string(self.update_keys(json.loads((req[1])).encode()))
			elif req[0] == 'ok_ok_take_my_keys':
				self.answer_sock.send_string(self.ok_ok_take_my_keys( req[1],json.loads((req[2]))))
			elif req[0] == 'update_finger_table':
				self.answer_sock.send_string(str(self.update_finger_table(int(req[1]),
					int(req[2]),req[3])))
			elif req[0] == 'give_me_the_successor':
				self.answer_sock.send_string(self.give_me_the_successor())
			elif req[0] == 'closest_preceding_finger':
				self.answer_sock.send_string(json.dumps(self.closest_preceding_finger(int(req[1]))))
			elif req[0] == 'give_me_the_keys':
				self.answer_sock.send_string(self.give_me_the_keys(req[1]))
			elif req[0] == 'update_pred':
				self.answer_sock.send_string(self.update_pred(req[1]))
			elif req[0] == 'update_succ':
				self.answer_sock.send_string(self.update_succ(req[1]))
			elif req[0] == 'add_remote_key':
				self.answer_sock.send_string(self.add_remote_key(int(req[1]),req[2]))
			elif req[0] == 'remote_ask_for_a_key':
				self.answer_sock.send_string(self.remote_ask_for_a_key(int(req[1])))
			elif req[0] == 'remote_update_key':
				self.answer_sock.send_string(self.remote_update_key(int(req[1]),req[2]))
			elif req[0] == 'remote_update_others':
				self.answer_sock.send_string(self.update_others())
			elif req[0] == 'notify':
				self.answer_sock.send_string(self.notify(req[1]))	


	def init_servers(self):
		a = Daemon(self,'wait_conections')
		b = Daemon(self,'stabilize')
		c = Daemon(self,'fix_fingers')
		d = Daemon(self,'print_values')
		e = Daemon(self,'replication_keys')
		a.start()
		b.start()
		c.start()
		d.start()
		e.start()

class Daemon(threading.Thread):
	def __init__(self, obj, method):
		threading.Thread.__init__(self)
		self.obj_ = obj
		self.method_ = method

	def run(self):
		getattr(self.obj_, self.method_)()


# import sys
# a = Chord('a',sys.argv[1])
# if len(sys.argv) > 2:
# 	a.join_node(sys.argv[2])