import socket
import time
import random
import string

def getHost():
    return random.choices(["Amiths-MacBook-Pro.local"])[0]

def getStr():
    return ''.join(random.choices(string.ascii_lowercase, k=5))

def setup(sendMsg,host, port):
 sendMsg = sendMsg + "\n"
 client_socket = socket.socket()  # instantiate
 client_socket.connect((host, port))  # connect to the server
 client_socket.settimeout(4)
 client_socket.sendall(sendMsg.encode())
 result = ""
 while("\n" not in result):
    resp = client_socket.recv(10096)
    result = result + resp.decode()
 result = result[:-1]
 print(result)
 client_socket.close()

port = 15010
insert_latency = []
for i in range(200):
    st = time.time()
    setup("insert {} {}".format(getStr(), getStr()), getHost(), port)
    end = time.time()
    insert_latency.append(end-st)
    time.sleep(0.1)

print(sum(insert_latency)/1000)
