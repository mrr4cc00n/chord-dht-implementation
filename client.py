import select
import socket
import sys
import time
import os
import webbrowser
##tambien hay que tener un path pa ver donde almaceno el tema
def check_IP( ip):
	checs = ip.split('.')
	
	if len(checs) < 4:
		print("ERROR: IP incorrecto. Asegurese introducir 4 valores enteros válidos entre 0 y 255 separados por comas")
		time.sleep(0.5)
		print("	  Ejemplo:  127.0.0.1")
		return False
	for x in range(0,4):
		#print("number 1")
		if int(checs[x]) > 255 or int(checs[x]) < 0:
			print("ERROR: IP incorrecto. Asegurese introducir 4 valores enteros válidos entre 0 y 255 separados por comas")
			time.sleep(0.5)
			print("	  Ejemplo:  127.0.0.1")
			return False
	return True
def check_Port( port):
	
	try:
		rigth = (int(port) >= 0 and int(port) <= 65500)
	except :
		return False
	#print (rigth)
	if not rigth:
		print("ERROR: Puerto incorrecto. Asecurese de introducir un valor entre 0 y 65535")
	return rigth

def check_Path(path):        # como verificar si el path esta correcto si un path puede ser cualquier cosa
	try:
		path_ok = path.replace('\\','/')
		#print(path_ok)
		os.path.exists(path_ok)
		#print("OKKKKK")
		return True
	except :
		print("ERROR: Path inexistente. Asegurese de que la ruta existe.")
		return False

class Client_Spot:
	def __init__(self):

		while 1 :
			print("Introduzca el IP del servidor al que se desea conectar. ( 127.0.0.1)")
			server_addr = input("-->")
			if check_IP(server_addr):
				break
		while 1:
			print("Introduzca el puerto. (8000)")
			port = input("-->")
			if check_Port (port):
				break
		while 1:
			print("Introduzca el path donde guardará las canciones descargadas")
			self.path = input("->")
			if check_Path ( self.path):
				break
			
		self.server_addr = server_addr
		self.port = port
		self.conect_sock = socket.socket()
		#print("Me conecte a "+str(server_addr)+':'+str(port))

	def dpwnload_song(self,path,down_path):
		self.conect_sock.send('?'.encode())
		self.conect_sock.recv(1024)
		self.conect_sock.send(path.encode())
		try:
			f = open(down_path + path,'wb')
			#print(down_path + path)
		except :
			print("Path not found!!!!!!!!!!!!!!!!!")
			return
		
		item = self.conect_sock.recv(1024)
		data = "Ok".encode()
		if(item == data):
			item = self.conect_sock.recv(1024)
			print("   Estamos descargando la canción esto tardará algunos segundos. :?")
			while item:
				f.write(item)
				item = self.conect_sock.recv(1024)
			f.close()
			self.conect_sock.close()
			print("   Canción descargada exitosamente. :)")
			return
		self.conect_sock.close()
		a=self.connect(item.decode())
		f.close()
		if(a != "No"):
			self.dpwnload_song(path,down_path)
		return

	def up_load_song(self,path,upload_path):
		self.conect_sock.send('~'.encode())
		print(str(self.conect_sock.recv(1024).decode()))
		self.conect_sock.send(path.encode())
		try:
			f = open(upload_path+path,'rb')

		except :
			print("Path not found!!!!!!!!!!!!!!!!!")
			return
		
		item = f.read(1024)
		self.conect_sock.send(item)
		print("   Estamos compartiendo la canción esto tardará algunos segundos")
		while item:
			self.conect_sock.send(item)
			item = f.read(1024)
		f.close()
		self.conect_sock.close()
		print("   Canción compartida exitosamente. :)")

	def connect(self,addr = None):
		if(not addr):
			# print("WRONGGGGG")
			self.conect_sock = socket.socket()
			self.conect_sock.connect((self.server_addr,int(self.port)))
		else:
			self.conect_sock = socket.socket()
			item = addr.split(':')
			self.server_addr = item[0]
			self.port = item[1]
			#self.conect_sock.connect((item[0],int(item[1])))
			try:
				# print("ADDR")
				# print(addr)

				self.conect_sock.connect((self.server_addr,int(self.port)))				
			except:
				print(" La canción solicitada no fue encontrada lamentablemente. :(")
				return "No" 

	def play(self, song_name):
		try:
			f = open(self.path + song_name)
			f.close()
			webbrowser.open(self.path + song_name)
		except :
			self.dpwnload_song(song_name, self.path)
			webbrowser.open(self.path + song_name)
		
	def path_parser():
		pass
a = Client_Spot()
print("   ================================================================================")
print("  |               Binvenido a tu Buscador de música distribuido                    |")
while 1:
	print("   ================================================================================")
	time.sleep(1)
	print(" 	Introduzca las siguientes palabra:")
	time.sleep(0.5)
	print("   down --> si desea DESCARGAR ")
	time.sleep(0.5)
	print("   play --> si desea REPRODUCIR")
	time.sleep(0.5)
	print("   up --> si desea COMPARTIR")
	time.sleep(0.5)
	print("   help --> si necesita AYUDA")
	time.sleep(0.5)
	print("   addr --> si desea cambiar la DIRECCION de almacenamiento")
	time.sleep(0.5)
	print("   exit --> si desea abandonar")

	a.connect()

	buf = input("-->")
	if(buf == 'down'):
		
		while 1:
			print("Introduzca el nombre de la canción que desea descargar")
			time.sleep(0.5)
			print("!!!!Asegurese de poner la extención de la canción!!!")
			song1 = input("-->")
			song = '\\' + song1
			# if check_Path(path):
			# 	break
			# elif check_Path(path + '/' + song1):
			# 	song = '/'+ song1
			break
		a.dpwnload_song(song,a.path)
		
	elif(buf == 'up'):

		print("Introduzca el nombre de la canción que desea descargar")
		time.sleep(0.5)
		print("!!!!Asegurese de poner la extención de la canción!!!")
		song = '\\' + input("-->")

		a.up_load_song(song,a.path)
		a.conect_sock.close()
	
	elif(buf=='exit'):
		print("Esta seguro que desea abandonar la aplicacion??? y/n")
		resp = input("-->")
		if resp =='y':
			a.conect_sock.send("Leave".encode())
			a.conect_sock.close()
			print("  !!!!!!!!!!Vuelva pronto!!!!!!!!!!!!")
			break
		else:
			continue
	
	elif (buf == 'play'):
		
		print("Introduzca el nombre de la canciónque desea reproducir")
		time.sleep(0.5)
		print("!!!!Asegurese de poner la extención de la canción!!!")
		song = '\\' + input("-->")
		a.play(song)

	elif (buf == 'addr') :
		print("Su path acctual es: -->  " + a.path)
		print("Desea cambiarlo??? y/n")
		choice = input("-->")
		if choice == 'y':
			while 1:
				print("Introduzca el nuevo PATH donde desea almacenar.")		
				path = input("-->")
				if check_Path(path):
					a.path = path
					break
				else:
					print("Path inexistente. Introduzca un path correcto.")

				
			
		
	elif (buf == 'help'):
		print("")
		print("   -----------------------------------------AYUDA-----------------------------------------")
		print("")
		
		print("   Somos una aplicación para reproducir canciones. Trabajamos de forma distribuida entre")
		print("   un conjunto de servidores.")
		print("   Al inicio de la ejecución de la aplicación debiste introducir un path local")
		print("   donde almacenarás las descargas y donde estarán las canciones que desees compartir.")
		print("   Esta direccion puede ser camdiada en cualquier momento, solo debes introducir la ")
		print("   palabra addr.")
		print("	  Esta aplicación consta de tres módulos con tres funcionalidades diferentes.")
		
		time.sleep(15)
		print("")
		print("					-------Modulo No. 1------")
		print("")
		
		print("   En el primer módulo podrás descargar canciones de alguno de los servidores de nuestra red. ")
		print("   Para acceder al mismo deberás introducir la palabra down, luego deberás introducir el ")
		print("   nombre de la canción que deseas reproducir. RECUERDA!!!! poner la extensión de la canción,")
		print("   es decir, .mp3, .wav, .ogg, .mpg, etc.")
		print("   Si no se descargada la canción compruebe que la ruta y el nombre de la canción sean correctos.")
		
		print("")
		time.sleep(15)
		print("					-------Modulo No. 2------")
		print("")
		
		print("   En este módulo podrás reproducir canciones. Las canciones serán reproducidas con el browser")
		print("   que tengas instalado. Para acceder a este módulo deberás introducir la palabra play, luego")
		print("   el path y el nombre de la canción como se indica. RECUERDA!!! que al igual que en el módulo anterior")
		print("   deberás comprobar que pongas correctamente el nombre de la canción así como su extensión.")
			
		print("")
		time.sleep(15)
		print("					-------Modulo No. 3------")
		print("")
		print("   Este es nuestro último módulo y en él podrás subir a nuestros servidores música de tu computadora,")
		print("   para compartir con tus amigos. Para acceder a este módulo deberás escribir la palabra up y luego el")
		print("   nombre de la canción junto con su extensión. RECUERDA!!! verificar que el nombrede la cancion sea correcto")
		print("   y que hayas agregado la extensión de la canción.")
		print("")
		input(" Precione ENTER para volver...")
