

import socket
host = socket.gethostname()  # as both code is running on same pc
port = 15010  # socket server port number


def setup(sendMsg,host, port):
 client_socket = socket.socket()  # instantiate
 client_socket.connect((host, port))  # connect to the server
 client_socket.sendall(sendMsg.encode())
 result = client_socket.recv(1096)
 print(result.decode())
 client_socket.close()

import socket
host = socket.gethostname()  # as both code is running on same pc
port = 15010  # socket server port number
client_socket = socket.socket()  # instantiate
client_socket.connect((host, port))  # connect to the server
client_socket.sendall("insert city boston".encode())
result = client_socket.recv(1096)
print(result.decode())
client_socket.close()

import socket
host = socket.gethostname()  # as both code is running on same pc
port = 15010  # socket server port number
client_socket = socket.socket()  # instantiate
client_socket.connect((host, port))  # connect to the server
client_socket.sendall("insert state MA".encode())
result = client_socket.recv(1096)
print(result.decode())
client_socket.close()

port = 15012  # socket server port number
client_socket = socket.socket()  # instantiate
client_socket.connect((host, port))  # connect to the server
client_socket.sendall("get name".encode())
result = client_socket.recv(1096)
print(result.decode())
client_socket.close()





import socket
host = socket.gethostname()  # as both code is running on same pc
port = 15010  # socket server port number
client_socket = socket.socket()  # instantiate
client_socket.connect((host, port))  # connect to the server
client_socket.sendall("Print all".encode())
result = client_socket.recv(1096)
print(result.decode())
client_socket.close()



import socket
host = socket.gethostname()  # as both code is running on same pc
port = 15011  # socket server port number
client_socket = socket.socket()  # instantiate
client_socket.connect((host, port))  # connect to the server
client_socket.sendall("Print all\n".encode())
result = client_socket.recv(1096)
print(result.decode())
client_socket.close()



import socket
host = socket.gethostname()  # as both code is running on same pc
port = 15012  # socket server port number
client_socket = socket.socket()  # instantiate
client_socket.connect((host, port))  # connect to the server
client_socket.sendall("Print all\n".encode())
result = client_socket.recv(1096)
print(result.decode())
client_socket.close()



import socket
host = socket.gethostname()  # as both code is running on same pc
port = 15013  # socket server port number
client_socket = socket.socket()  # instantiate
client_socket.connect((host, port))  # connect to the server
client_socket.sendall("Print all\n".encode())
result = client_socket.recv(1096)
print(result.decode())
client_socket.close()

import socket
host = socket.gethostname()  # as both code is running on same pc
port = 15014  # socket server port number
client_socket = socket.socket()  # instantiate
client_socket.connect((host, port))  # connect to the server
client_socket.sendall("Print all".encode())
result = client_socket.recv(1096)
print(result.decode())
client_socket.close()


import socket
host = socket.gethostname()  # as both code is running on same pc
port = 15015  # socket server port number
client_socket = socket.socket()  # instantiate
client_socket.connect((host, port))  # connect to the server
client_socket.sendall("Print all".encode())
result = client_socket.recv(1096)
print(result.decode())
client_socket.close()

import socket
host = socket.gethostname()  # as both code is running on same pc
port = 15016  # socket server port number
client_socket = socket.socket()  # instantiate
client_socket.connect((host, port))  # connect to the server
client_socket.sendall("Print all".encode())
result = client_socket.recv(1096)
print(result.decode())
client_socket.close()
