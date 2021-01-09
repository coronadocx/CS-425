#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr  5 20:27:39 2020

@author: lishenhe
"""

import threading
import socket
import time
import sys

SERV_IP = "172.22.156.169"
ALL_IP          = ["172.22.156.169", 
                   "172.22.158.169",
                   "172.22.94.168",
                   "172.22.156.170",
                   "172.22.158.170",
                   "172.22.94.169",
                   "172.22.156.171",
                   "172.22.158.171"]

SERV_PORT = 4000
THREADS = []

def create_node(id):
    
    port = int(id) + 4000
    print("create_node: my id is " + str(id) + " | port: " + str(port))
    # build connection with the server
    serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serv.connect(((SERV_IP, SERV_PORT)))
    print("Successfully connected to SERVER")
    serv.send( ("CONNECT nodexxx 172.22.158.169 " + str(port)).encode() )
    serv.send( "a wrong message" )
    print("Sent connection message to server.")
    while True:
        msg = serv.recv(1024)
        print("received msg")
        if not msg:
            print("wrong message from server")
            break
        print('Received', repr(msg))
    serv.close()
    
def main(argv):
    # get number
    if len(argv) == 0:
        N = 1
    elif len(argv) == 1:
        N = int(argv[0])
    else:
        print("wrong number of input argument")
        sys.exit()

    
    global THREADS
    # create Daemon node threads
    id = 0
    for i in range(N):
        t = threading.Thread(target = create_node, args = (id,))
        t.daemon = True
        THREADS.append(t)
        t.start()
        id = id + 1
        
    
    for t in THREADS:
        t.join()
    
    


if __name__ == "__main__":
    main(sys.argv[1:])