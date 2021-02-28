import select
import socket
import sys
import time
from chord_implement import *
import os
import json
#hay q definir el path del server para hacer el upload
musik = "D:\musik\Audio\03 Pi.mp3"
# 53752760

def check_Addr( ip):
	check_1 = ip.split(':')

	#print(len(check_1) != 2)
	if len(check_1) != 2:
		print("ERROR: Dirección de red inválida. Asegurese de escribir el IP y el puerto separados por :")
		return False
	checs = check_1[0].split('.')
	port = check_1[1]
	#print(len(checs))
	if len(checs) < 4:
		print("ERROR: IP incorrecto. Asegurese introducir 4 valores enteros válidos entre 0 y 255 separados por comas")
		time.sleep(0.5)
		print("	  Ejemplo:  127.0.0.1")
		return False

		if int(checs[x]) > 255 or int(checs[x]) < 0:
			print("ERROR: IP incorrecto. Asegurese introducir 4 valores enteros válidos entre 0 y 255 separados por comas")
			time.sleep(0.5)
			print("	  Ejemplo:  127.0.0.1")
			return False
	try:
		rigth = (int(port) >= 0 and int(port) <= 65500)
	except :
		return False
	if not rigth:
		print("ERROR: Puerto incorrecto. Asegurese de introducir un valor entre 0 y 65535")
	return rigth

def check_Path(path):
	try:
		os.path.exists(path)
		return True
	except :
		print("ERROR: Path inexistente. Asegurese de que la ruta existe.")
		return False


class Ser_Spot:
	def __init__(self):
		
		print("Introduzca el nombre del servidor")
		name_ser = input("-->")

		while 1:
			print("Introduzca la dirección del servidor. Con el formato IP:puerto (127.0.0.1:8000)")
			addr = input("-->")
			if check_Addr(addr):
				break

		while 1:
			print("Introduzca el path donde trabajará el servidor. ")
			buf = input("-->")
			if check_Path(buf):
				break

		print("Desea conectarse con otro servidor? y/n")
		choice = input("-->")
		if choice == 'y':
			while 1:
				print("Introduzca la dirección del servidor con el que se desea conectar. Con el formato IP:puerto (127.0.0.1:8000)")
				serv_addr = input("-->")
				if check_Addr(serv_addr):
					break		
		
		self.files = buf
		self.name_ser = name_ser
		self.addr = addr
		self.id = do_hash(addr)
		temp = addr.split(':')
		self.ip = temp[0]
		self.port = temp[1]
		self.chord = Chord(name_ser,self.ip + ':' + str(int(self.port) + 1))
		
		if choice == 'y':
			t = serv_addr.split(':')
			self.other_serv_ip = t[0]
			self.other_serv_port = t[1]
			self.chord.join_node(self.other_serv_ip + ':' + str(int(self.other_serv_port) + 1))
		
		self.listen_sock = socket.socket()
		self.daemon()

	def conections_managment(self):
		download_set = []
		upload_set = []
		rlist = []
		wlist = []
		xlist = []
		rlist.append(self.listen_sock)
		while 1:
			triple = select.select(rlist,wlist,xlist)
			if self.listen_sock in triple[0]:
				sock,_ = self.listen_sock.accept()
				print("Cliente conectado")
				print(sock)
				rlist.append(sock)
			for x in triple[0]:
				if x in triple[1]:
					triple[1].remove(x)							
				elif not self.review_set(x,download_set)and not self.review_set(x,upload_set) and x!=self.listen_sock:
					aux = self.attending_client(x,rlist,wlist)
					try:
						if(aux[1] == '?'):
							download_set.append((x,aux[0]))
							if(not x in wlist):
								wlist.append(x)
						elif aux[1] == '~':
							upload_set.append((x,aux[0]))
							if(not x in rlist):
								rlist.append(x)
					except Exception:
						print("El cliente se desconecto bruscamente")
						if(x in rlist):
							rlist.remove(x)
						if(x in wlist):
							wlist.remove(x)
						x.close()
						
			for_remove=[]
			for x in download_set:
				item = x[1].read(1024)
				if(item):
					x[0].send(item)
				else:
					print("  Descarga terminada----------------")
					for_remove.append(x)				
			for x in upload_set:
				# print("upload package")
				item = x[0].recv(1024)
				if item:
					x[1].write(item)
				else:
					for_remove.append(x)
					print("Upload terminado--------------")
			for x in for_remove:
				if x[0] in rlist:
					rlist.remove(x[0])
					if(x in upload_set):
						upload_set.remove(x)
				if x[0] in wlist:
					wlist.remove(x[0])
					if x in download_set:
						download_set.remove(x)
				x[1].close()
				x[0].close()

	def attending_client(self,client_sock,rlist,wlist):
		data = client_sock.recv(1024)
		if(data is None):
			return(0,'>',0)
		if(data.decode() == '?'):
			client_sock.send("filename".encode())
			data = client_sock.recv(1024)
			item = ""
			item+=str(data.decode())
			try:
				try:
					aux = self.files +item
					print(aux)
					f = open(aux,'rb')
					print("entre"+str(f))
					if(f):
						client_sock.send("Ok".encode())
						return (f,'?',item)
				except :
					print("Bunscando...")
				k=do_hash(item)
				if(k in self.chord.keys):
					tupla = self.chord.keys[k]
				else:
					tupla = self.chord.ask_for_a_key(do_hash(item))
				if(client_sock in rlist):
					rlist.remove(client_sock)
				if(client_sock in wlist):
					wlist.remove(client_sock)		
				print("TUPLA A CONECT")
				print(tupla)
				client_sock.send(tupla.encode())
				client_sock.close()
				print("La solucitud fue enviada al servidor encargado espere mientras es atendido")
				return (0,'>',0)
				
			except Exception:
				print("Error ocurred path or name invalid")
				return (0,'>',0)
		elif data.decode() == '~':			
			client_sock.send("filename".encode())
			data = client_sock.recv(1024)
			item = ""
			item+=str(data.decode())
			try:
				f = open(self.files +item,'wb')
				k = do_hash(item)
				self.chord.add_key(k, self.ip + ':' + str(self.port))
				return (f,'~')
			except Exception:
				print("Error ocurred path or name invalid")
				return (0,'>',0)
		elif data.decode() == 'Leave':
			if(client_sock in rlist):
				rlist.remove(client_sock)
			if(client_sock in wlist):
				wlist.remove(client_sock)
			client_sock.close()

	def daemon(self):
		print(" Servidor levantado correctamente. ")
		time.sleep(0.5)
		print("Esperando por clientes...")
		self.listen_sock.bind((self.ip,int(self.port)))
		self.listen_sock.listen()
		self.conections_managment()

	def review_set(self,sock,set1):
		for x in set1:
			if x[0] == sock:
				return True
		return False

a = Ser_Spot()