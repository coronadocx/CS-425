#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  2 21:13:41 2020

@author: lishenhe
"""
import socket
import time
#import sys

ALL_IP          = ["172.22.156.169", 
                   "172.22.158.169",
                   "172.22.94.168",
                   "172.22.156.170",
                   "172.22.158.170",
                   "172.22.94.169",
                   "172.22.156.171",
                   "172.22.158.171"]


def main():
    while True:
        try:
            coord = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            coord.connect((ALL_IP[1], 1234))
            break
        except:
            time.sleep(1.0)
            print("Connection refused. Re-connection in 1sec.\n")
    
    print("Connection ready. Please input your commands.\n")
    while True:
        # accept from user input and send it to coordinator
        # at VM2
        try:
            msg = input()
            #print("Ready to send message:"+msg)
            coord.send(msg.encode())
            #print("Finished sending message:"+msg)
            rsp = coord.recv(1024)
            print(rsp.decode())
        except:
            print("Coordinator died.\n")  
            break

    coord.close()
        
        
    
if __name__ == "__main__":
    main()