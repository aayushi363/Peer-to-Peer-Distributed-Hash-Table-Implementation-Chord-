import hashlib
import socket
from threading import Thread
import argparse
import sys
import ast
import signal
import os
from node import Node

class Chord:
    def __init__(self, ip, bit_length, port, peer_ip, peer_port, replica_count=2):
        self.replica_count = replica_count
        self.my_node  = Node(ip, port, None, None, None, None, bit_length)
        if peer_ip is not None and peer_port is not None:
            response = self.find_neighbours(self.my_node.hash, peer_ip, peer_port)
            if response == "Already Present":
                self.getupdate_peerList(self.my_node.getHash(peer_ip+str(peer_port)), peer_ip, peer_port, True)            
            else:
                self.getupdate_peerList(self.my_node.successorHash, self.my_node.successor_ip, self.my_node.successor_port, False)            
        self.sock = socket.socket()
        self.port = port
        self.setup_server()
        self.start_server()

    def find_neighbours(self, curr_hash, peer_ip, peer_port):
        neighbours = self.send_query("neighbours {}\n".format(str(curr_hash)), peer_ip, peer_port)
        if neighbours is None:
            print("Could not find neighbours")
            sys.exit(0)
        #print("searching neighbours")
        neighbours = neighbours.split(",")
        successor_ip = neighbours[0].strip()
        successor_port = neighbours[1].strip()
        predecessor_ip = neighbours[2].strip()
        predecessor_port = neighbours[3].strip()
        #print(successor_ip,successor_port, predecessor_ip, predecessor_port)
        #print(self.my_node.ip, self.my_node.port)
        self.my_node.setSuccessor(successor_ip, successor_port)
        self.my_node.setPredecessor(predecessor_ip, predecessor_port)
        if (successor_ip == self.my_node.ip and int(successor_port) == self.my_node.port) or (predecessor_ip == self.my_node.ip and int(predecessor_port) == self.my_node.port):
            return "Already Present"
        self.send_query("setSuccessor {} {}\n".format(self.my_node.ip, self.my_node.port), predecessor_ip, predecessor_port)
        self.send_query("setPredecessor {} {}\n".format(self.my_node.ip, self.my_node.port), successor_ip, successor_port)
        data = self.send_query("getResponsibleData {}\n".format(self.my_node.hash), successor_ip, successor_port)
        if data is not None and len(data) > 1:
            data = data.split(' ')[1:]
            self.set_multi(data)
        self.send_query("delResponsibleData {}\n".format(str(self.my_node.hash)), successor_ip, successor_port)
        return "Done"

    def getupdate_peerList(self, succ_hash, ip, port, repeat):
        # The following function call will return a string containing the peer list of the successor node
        peers = self.send_query("peers \n",ip,port)
        #print(peers)
        # convert the string in list of list and copy it to the peer list of current node
        peerList = ast.literal_eval(peers)
        self.my_node.peerTable = peerList
        #print(self.my_node.peerTable)
        # Now add the successor node as a peer in the above peer list
        peer_info = [ip,port,succ_hash]
        self.my_node.insertInPeerTable(peer_info)
        #print(self.my_node.peerTable)
        # Now for each peer in the peer list tell them that I am the new node
        # add me as your peer
        if not repeat:
            my_info = [self.my_node.ip,self.my_node.port,self.my_node.hash]
            for p in self.my_node.peerTable:
                self.send_query("addMe {}\n".format(my_info),p[0],p[1])
  

    def send_ignore_query(self, message, host, port):
        try:
            client_socket = socket.socket()  # instantiate
            client_socket.connect((host, int(port)))  # connect to the server
            client_socket.sendall(message.encode())
            client_socket.settimeout(2)
            client_socket.close()
        except Exception as e:
            print(e)
            print("cant not run the query - {}".format(message))

    def send_query(self, message, host, port):
        try:
            client_socket = socket.socket()  # instantiate
            client_socket.connect((host, int(port)))  # connect to the server
            client_socket.sendall(message.encode())
            client_socket.settimeout(4)
            result = ""
            while("\n" not in result):
                resp = client_socket.recv(10096)
                result = result + resp.decode()
            result = result[:-1]
            client_socket.close()
            return result
        except Exception as e:
            print(e)
            print("cant not run the query - {}".format(message))
            return None

    def setup_server(self):
        try:
            host = socket.gethostname()
            self.sock.bind((host, self.port))
            self.sock.listen(200)
        except Exception as e:
            print(e)
            print("Cannot bind the port")
            sys.exit(0)

    def start_server(self):
        print("Starting the server")
        threads = []
        # start the chord server and accept connection and perform operations.
        while True:
            try:
                client, address = self.sock.accept() 
                recv_msg = ""
                while("\n" not in recv_msg):
                    resp = client.recv(10024)
                    recv_msg = recv_msg + resp.decode()
                recv_msg = recv_msg[:-1]
                t = Thread(target = self.handle_request, args = (client, recv_msg))
                threads.append(t)
                t.start()
            except Exception as e:
                print(e)
                print("encountered an error")
                continue
                
        for i in threads:
            t.join()

    def handle_request(self, client, recv_msg):
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
        if parsed_content["operation"] == "replicate":
            self.replicate_doc(parsed_content["val"][0], parsed_content["val"][1],parsed_content["val"][2])
            return "Replicated the document\n"
        elif parsed_content["operation"] == "finalGet":
            val = self.my_node.get(parsed_content["val"][0])
            if val is None:
                return "No such key present\n"
            return val + "\n"
        elif parsed_content["operation"] == "get":
            val = self.get_kv(parsed_content["val"][0])
            if val is None:
                return "No such key present\n"
            return val + "\n"
        elif parsed_content["operation"] == "insert":
            self.put_kv(parsed_content["val"][0],parsed_content["val"][1])
            return "Inserted the document\n"
        elif parsed_content["operation"] == "finalInsert":
            self.my_node.insert(parsed_content["val"][0],parsed_content["val"][1])
            return "Inserted the document\n"
        elif parsed_content["operation"] == "neighbours":
            return self.get_neighbours(int(parsed_content["val"][0])) + "\n"
        elif parsed_content["operation"] == "setSuccessor":
            self.my_node.setSuccessor(parsed_content["val"][0], parsed_content["val"][1])
            return "Set the Successor\n"
        elif parsed_content["operation"] == "setPredecessor":
            self.my_node.setPredecessor(parsed_content["val"][0], parsed_content["val"][1])
            return "Set the Predecessor\n"
        elif parsed_content["operation"] == "getResponsibleData":
            return self.get_responsible(parsed_content["val"][0]) + "\n"
        elif parsed_content["operation"] == "delResponsibleData":
            self.del_responsible(parsed_content["val"][0])
            return "deleted unwanted data\n"
        elif parsed_content["operation"] == "setMulti":
            self.set_multi(parsed_content["val"])
            return "Inserted all the new docs\n"
        elif parsed_content["operation"] == "exit":
            try:
                self.removeNode()
                os.kill(os.getpid(), signal.SIGINT)
                return "killed the node\n"
            except Exception as e:
                print(e)
                print("could not safe exit. quiting now")
                os.kill(os.getpid(), signal.SIGINT)
        elif parsed_content["operation"] == "Print":
            return str(self.my_node.getInfo()) + "\n"
        elif parsed_content["operation"] == "peers":
            return str(self.my_node.getPeerTable()) + "\n"
        elif parsed_content["operation"] == "addMe":
            new_peer = ast.literal_eval(''.join(parsed_content["val"]))
            self.my_node.insertInPeerTable(new_peer)
            return "added the peer\n"
        

    def replicate_doc(self, rem, key, value):
        self.my_node.insert(key, value)
        if int(rem) != 0:
            self.send_query("replicate {} {} {}\n".format(str(int(rem)-1),key,value), self.my_node.successor_ip,  self.my_node.successor_port)

    def del_responsible(self, new_hash):
        del_keys = []
        for i in self.my_node.hashTable:
            if self.my_node.getHash(i) <= int(new_hash):
                del_keys.append(i)
        for i in del_keys:
            self.my_node.hashTable.pop(i)
    
    def removeNode(self):
        print(self.my_node.getInfo())
        keys = "setMulti"
        for i in self.my_node.hashTable:
            keys = keys + " " + i + " " + self.my_node.hashTable[i]
        if len(keys) > 9:
            keys = keys+"\n"
            self.send_query(keys, self.my_node.successor_ip, self.my_node.successor_port)
        if not self.my_node.successor_ip is None or self.my_node.predecessor_ip is None or (self.my_node.successor_ip == self.my_node.ip and self.my_node.successor_port == self.my_node.port):
            self.send_query("setSuccessor {} {}\n".format(self.my_node.successor_ip, self.my_node.successor_port), self.my_node.predecessor_ip, self.my_node.predecessor_port)
            self.send_query("setPredecessor {} {}\n".format(self.my_node.predecessor_ip, self.my_node.predecessor_port),  self.my_node.successor_ip,  self.my_node.successor_port)

    def set_multi(self, allKeys):
        #print("all keys - {}".format(str(allKeys)))
        for i in range(0,len(allKeys),2):
            self.my_node.insert(allKeys[i], allKeys[i+1])

    def get_responsible(self, new_hash):
        #print("new Hash - {}".format(new_hash))
        keys = "setMulti"
        for i in self.my_node.hashTable:
            #print("curr hashes - {} - {}".format(i,self.my_node.getHash(i)))
            if self.my_node.getHash(i) <= int(new_hash):
                keys = keys + " " + i + " " + self.my_node.hashTable[i]
        #print("new sending keys - {}".format(keys))
        if len(keys) > 9:
            return keys
        return ""

    def get_kv(self, key):
        keyHash = self.my_node.getHash(key)
        if self.my_node.successor_ip is None or self.my_node.predecessor_ip is None or (self.my_node.successor_ip == self.my_node.ip and self.my_node.successor_port == self.my_node.port):
            #print("found the key - {}".format(key))
            return self.my_node.get(key)
        elif keyHash <= self.my_node.hash and keyHash >= self.my_node.predecessorHash:
            #print("found the key - {}".format(key))
            return self.my_node.get(key)
        elif (self.my_node.hash <=  self.my_node.predecessorHash):
            #print("edge of the ring")
            if (keyHash <= self.my_node.hash or keyHash >= self.my_node.predecessorHash):
                print("found the key - {}".format(key))
                return self.my_node.get(key)
        index = self.get_responsible_node(keyHash)
        peer_entry = self.my_node.peerTable[index]
        count = index
        for i in range(self.replica_count):
            ans =  self.send_query("finalGet {}\n".format(str(key)), self.my_node.peerTable[count][0], int(self.my_node.peerTable[count][1]))
            if ans is None:
                count = count + 1
                conitnue
            return ans


    def put_kv(self, key, value):
        keyHash = self.my_node.getHash(key)
        if self.my_node.successor_ip is None or self.my_node.predecessor_ip is None or (self.my_node.successor_ip == self.my_node.ip and self.my_node.successor_port == self.my_node.port):
            return self.my_node.insert(key, value)
        elif keyHash <= self.my_node.hash and keyHash >= self.my_node.predecessorHash:
            self.send_query("replicate {} {} {}\n".format(str(self.replica_count-1),key,value), self.my_node.successor_ip,  self.my_node.successor_port)
            return self.my_node.insert(key, value)
        elif (self.my_node.hash <=  self.my_node.predecessorHash):
            #print("edge of the ring")
            if (keyHash <= self.my_node.hash or keyHash >= self.my_node.predecessorHash):
                self.send_query("replicate {} {} {}\n".format(str(self.replica_count-1),key,value), self.my_node.successor_ip,  self.my_node.successor_port)
                return self.my_node.insert(key, value)
        index = self.get_responsible_node(keyHash)
        peer_entry = self.my_node.peerTable[index]
        return self.send_query("insert {} {}\n".format(str(key), value), peer_entry[0], int(peer_entry[1]))

    def get_neighbours(self, curr_hash):
        if self.my_node.successor_ip is None or self.my_node.predecessor_ip is None or (self.my_node.successor_ip == self.my_node.ip and self.my_node.successor_port == self.my_node.port):
            #print("found 1st neighbours")
            return "{},{},{},{}".format(self.my_node.ip, self.my_node.port, self.my_node.ip, self.my_node.port)
        elif (curr_hash >= self.my_node.hash and curr_hash <= self.my_node.successorHash):
            #print("actual neighbour")
            return "{},{},{},{}".format(self.my_node.successor_ip, self.my_node.successor_port, self.my_node.ip, self.my_node.port)
        if (self.my_node.successorHash <=  self.my_node.hash):
            #print("edge of the ring")
            if (curr_hash >= self.my_node.hash or curr_hash <= self.my_node.successorHash):
                return "{},{},{},{}".format(self.my_node.successor_ip, self.my_node.successor_port, self.my_node.ip, self.my_node.port)
        #print("need to check with my Successor")
        return self.send_query("neighbours {}\n".format(str(curr_hash)), self.my_node.successor_ip, self.my_node.successor_port)
    
    def get_responsible_node(self, curr_hash):
        all_hashes = self.get_column(self.my_node.peerTable,2)
        #print("all_hashes - {}".format(all_hashes))
        node_index = self.find_index(all_hashes, len(all_hashes), curr_hash)
        #print("peertable  - {}".format(self.my_node.peerTable))
        #print("nodeIndex - {}".format(str(node_index)))
        if node_index >= len(all_hashes):
            node_index = 0
        return node_index

    def get_column(self, matrix, i):
        return [int(row[i]) for row in matrix]


    def find_index(self, arr, n, K):
        # Lower and upper bounds
        start = 0
        end = n - 1
        # Traverse the search space
        while start<= end:
            mid =(start + end)//2
            if arr[mid] == K:
                return mid
            elif arr[mid] < K:
                start = mid + 1
            else:
                end = mid-1     
        # Return the insert position
        return end + 1


