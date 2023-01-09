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
        self.hash = self.getIPHash(ip, port)
        self.hashTable = {}
        self.successor_ip = successor_ip
        self.successor_port = successor_port
        self.successorHash = self.getIPHash(successor_ip, successor_port)
        self.predecessor_ip = predecessor_ip
        self.predecessor_port = predecessor_port
        self.predecessorHash = self.getIPHash(predecessor_ip, predecessor_port)
        self.data_lock = threading.Lock()
        self.peerTable = []

    def getIPHash(self, ip, port):
        if ip is not None and port is not None:
            return self.getHash(ip+str(port))
        else:
            return -100

    def getHash(self, msg):
        if msg is None:
            return -100
        return int(hashlib.sha1(msg.encode()).hexdigest(), 16)% self.bit_length

    def insert(self, key, value):
        self.data_lock.acquire()
        self.hashTable[key] = value
        self.data_lock.release()
    
    def insertInPeerTable(self,list):
        self.peerTable.append(list)
        self.peerTable = sorted(self.peerTable,key=lambda x: x[2])
    
    def getPeerTable(self):
        return self.peerTable

    def get(self, key):
        ans = None
        self.data_lock.acquire()
        if key in self.hashTable:
            ans = self.hashTable[key]
        self.data_lock.release()
        return ans

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
                "peerTable":self.peerTable,
                "successor_ip ":self.successor_ip,
                "successor_port ":self.successor_port,
                "successorHash ":self.successorHash,
                "predecessor_ip ":self.predecessor_ip,
                "predecessor_port ":self.predecessor_port,
                "predecessorHash ":self.predecessorHash}