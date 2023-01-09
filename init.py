import hashlib
import socket
from _thread import *
import threading
import argparse

class Node:
    def __init__(self, ip, port, successor_ip, successor_port, predecessor_ip, predecessor_port, bit_length):
        self.bit_length = bit_length 
        self.ip = ip
        self.port = port
        if ip is not None and port is not None:
            self.hash = self.getHash(ip+str(port))
        else:
            self.hash = -100
        self.hashTable = {}
        self.successor_ip = successor_ip
        self.successor_port = successor_port
        if successor_ip is not None and successor_port is not None:
            self.successorHash = self.getHash(successor_ip+str(successor_port))
        else:
            self.successorHash = -100
        self.predecessor_ip = predecessor_ip
        self.predecessor_port = predecessor_port
        if predecessor_ip is not None and predecessor_port is not None:
            self.predecessorHash = self.getHash(predecessor_ip+str(predecessor_port))
        else:
            self.predecessorHash = -100
        self.data_lock = threading.Lock()

    def getHash(self, msg):
        if msg is None:
            return -100
        return int(hashlib.sha1(msg.encode()).hexdigest(), 16)% self.bit_length

    def insert(self, key, value):
        self.hashTable[key] = value

    def get(self, key):
        if key in self.hashTable:
            return self.hashTable[key]
        return None

    def setSuccessor(self, successor_ip, successor_port):
        self.successor_ip = successor_ip
        self.successor_port = successor_port
        self.successorHash = self.getHash(successor_ip+str(successor_port))

    def setPredecessor(self, predecessor_ip, predecessor_port):
        self.predecessor_ip = predecessor_ip
        self.predecessor_port = predecessor_port
        self.predecessorHash = self.getHash(predecessor_ip+str(predecessor_port))

    def getInfo(self):
        return {"bit_length ":self.bit_length,
                "ip ":self.ip,
                "port ":self.port,
                "hash ":self.hash,
                "hashTable ":self.hashTable,
                "successor_ip ":self.successor_ip,
                "successor_port ":self.successor_port,
                "successorHash ":self.successorHash,
                "predecessor_ip ":self.predecessor_ip,
                "predecessor_port ":self.predecessor_port,
                "predecessorHash ":self.predecessorHash}


class Chord:
    def __init__(self, ip, bit_length, port, peer_ip, peer_port):
        self.my_node  = Node(ip, port, None, None, None, None, bit_length)
        if peer_ip is not None and peer_port is not None:
            self.find_neighbours(self.my_node.hash,peer_ip, peer_port)
        self.sock = socket.socket()
        self.port = port
        self.setup_server()
        self.start_server()

    def find_neighbours(self, curr_hash, peer_ip, peer_port):
        neighbours = self.send_query("neighbours {}".format(str(curr_hash)), peer_ip, peer_port)
        print("searching neighbours")
        neighbours = neighbours.split(",")
        successor_ip = neighbours[0].strip()
        successor_port = neighbours[1].strip()
        predecessor_ip = neighbours[2].strip()
        predecessor_port = neighbours[3].strip()
        self.my_node.setSuccessor(successor_ip, successor_port)
        self.my_node.setPredecessor(predecessor_ip, predecessor_port)
        self.send_query("setSuccessor {} {}".format(self.my_node.ip, self.my_node.port), predecessor_ip, predecessor_port)
        self.send_query("setPredecessor {} {}".format(self.my_node.ip, self.my_node.port), successor_ip, successor_port)
        #get the responsible keys transferred.

    #add an operation to exit the code.

    def send_query(self, message, host, port):
        client_socket = socket.socket()  # instantiate
        client_socket.connect((host, int(port)))  # connect to the server
        client_socket.sendall(message.encode())
        result = client_socket.recv(1096)
        result = result.decode()
        return result

    def setup_server(self):
        host = socket.gethostname()
        self.sock.bind((host, self.port))
        self.sock.listen(2)

    def start_server(self):
        print("Starting the server")
        while True:
            client, address = self.sock.accept() 
            recv_msg = client.recv(1024).decode()
            parsed_content = self.parse_message(recv_msg)
            result = self.perform_operation(parsed_content)
            client.sendall(result.encode())
            client.close()

    def parse_message(self, message):
        parsed_content = {}
        words = message.split(" ")
        parsed_content["operation"] = words[0]
        parsed_content["val"] = words[1:]
        return parsed_content

    def perform_operation(self, parsed_content):
        if parsed_content["operation"] == "get":
            return self.get_kv(parsed_content["val"][0])
        elif parsed_content["operation"] == "insert":
            self.put_kv(parsed_content["val"][0],parsed_content["val"][1])
            return "Inserted the document"
        elif parsed_content["operation"] == "neighbours":
            return self.get_neighbours(int(parsed_content["val"][0]))
        elif parsed_content["operation"] == "setSuccessor":
            self.my_node.setSuccessor(parsed_content["val"][0], parsed_content["val"][1])
            return "Set the Successor"
        elif parsed_content["operation"] == "setPredecessor":
            self.my_node.setPredecessor(parsed_content["val"][0], parsed_content["val"][1])
            return "Set the Predecessor"
        elif parsed_content["operation"] == "Print":
            return str(self.my_node.getInfo())

    def get_kv(self, key):
        keyHash = self.my_node.getHash(key)
        if self.my_node.successor_ip is None or self.my_node.predecessor_ip is None:
            return self.my_node.get(key)
        elif keyHash < self.my_node.hash and keyHash > self.my_node.predecessorHash:
            return self.my_node.get(key)
        elif (self.my_node.hash <  self.my_node.predecessorHash):
            print("edge of the ring")
            if (keyHash < self.my_node.hash or keyHash > self.my_node.predecessorHash):
                return self.my_node.get(key)
        print("need to check with my Successor")
        return self.send_query("get {}".format(str(key)), self.my_node.successor_ip, self.my_node.successor_port)

    def put_kv(self, key, value):
        keyHash = self.my_node.getHash(key)
        if self.my_node.successor_ip is None or self.my_node.predecessor_ip is None:
            return self.my_node.insert(key, value)
        elif keyHash < self.my_node.hash and keyHash > self.my_node.predecessorHash:
            return self.my_node.insert(key, value)
        elif (self.my_node.hash <  self.my_node.predecessorHash):
            print("edge of the ring")
            if (keyHash < self.my_node.hash or keyHash > self.my_node.predecessorHash):
                return self.my_node.insert(key, value)
        print("need to check with my Successor")
        return self.send_query("insert {} {}".format(str(key), value), self.my_node.successor_ip, self.my_node.successor_port)

    def get_neighbours(self, curr_hash):
        print("new hash - {}".format(curr_hash))
        print("succ hash - {}".format(self.my_node.successorHash))
        print("node hash - {}".format(self.my_node.hash))
        if self.my_node.successor_ip is None or self.my_node.predecessor_ip is None:
            print("found 1st neighbours")
            return "{},{},{},{}".format(self.my_node.ip, self.my_node.port, self.my_node.ip, self.my_node.port)
        elif (curr_hash > self.my_node.hash and curr_hash < self.my_node.successorHash):
            print("actual neighbour")
            return "{},{},{},{}".format(self.my_node.successor_ip, self.my_node.successor_port, self.my_node.ip, self.my_node.port)
        if (self.my_node.successorHash <  self.my_node.hash):
            print("edge of the ring")
            if (curr_hash > self.my_node.hash or curr_hash < self.my_node.successorHash):
                return "{},{},{},{}".format(self.my_node.successor_ip, self.my_node.successor_port, self.my_node.ip, self.my_node.port)
        print("need to check with my Successor")
        return self.send_query("neighbours {}".format(str(curr_hash)), self.my_node.successor_ip, self.my_node.successor_port)


def main():
    parser = argparse.ArgumentParser("Please enter the ip , port, bit_length , peer ip and the peer port of the connecting ring",add_help=True)
    parser.add_argument("--Ip", required=True)
    parser.add_argument("--Port", required=True)
    parser.add_argument("--BitsLength", required=True)
    parser.add_argument("--PeerIp", required=False, default=None)
    parser.add_argument("--PeerPort", required=False,default=None)
    args = parser.parse_args()
    ip = args.Ip
    port = int(args.Port)
    bit_length = int(args.BitsLength)
    peer_ip = None
    if args.PeerIp is not None:
        peer_ip  = args.PeerIp
    peer_port = None
    if args.PeerPort is not None:    
        peer_port = int(args.PeerPort)
    chord = Chord(ip, bit_length , port, peer_ip, peer_port)



if __name__ == "__main__":
    main()