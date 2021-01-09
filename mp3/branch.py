#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  2 21:16:49 2020

@author: lishenhe
"""

# The information coming into branch.py is
# Pattern: Command + Account + transaction timestamp
# e.g. DEPOSIT xyz 5 2777.64
#      BALANCE xyz 2777.64
#      WITHDRAW xyz 5 2777.64
#      COMMIT 2777.64
#      DoCOMMIT 2777.64
#      ABORT 2777.64

# NOTE: All the messages are "PREPARATION" until a DoCOMMIT comes

# Commit messages asks for votes
# this is time to check negative account in VIEWs
# e.g. COMMIT 2777.64
# reply with : OK or ABORT

# Message going back to coordinator
# Pattern: status +[value] + [dependency]; dependency = None or 2777.64 2778.43
# e.g. 1. ABORT TX
# e.g. 2. OK TX val
# e.g. 3. OK TX val tx_1 tx_2 <- dependency(ies)

# timestamp is the ID of a transaction, also can "linked" to a specific client
# does not distinguish different clients, only transactions
import socket
import threading
from threading import Lock
lock = Lock()
import copy
ALL_IP          = ["172.22.156.169", 
                   "172.22.158.169",
                   "172.22.94.168",
                   "172.22.156.170",
                   "172.22.158.170",
                   "172.22.94.169",
                   "172.22.156.171",
                   "172.22.158.171"]
ACCOUNTs = dict()  # list of available account names
                   # "xyz" -> class account("xyz")
VIEWs  = dict()    # tentative updates of transaction
                   # tx ID -> dict( "xyz" -> class("xyz"))
                   # tx2 ID -> dict( "xyzzz" -> class("xyzzz"))

class account:
    def __init__(self,N="",V=0,rts="", wts=""):
        self.name = N
        self.value = V
        self.rts = ""
        self.wts = ""
        
    def print(self):
        print(self.name + " " +str(self.value) + " " + self.rts + " " + self.wts)
   
def isNewerOrSame(obj1, obj2, flag):
    # check if obj1 is newer than obj2
    if (flag == "read"):
        if (obj1.wts >= obj2.wts):
            return True
        else:
            return False
    elif (flag == "write"):
        if (obj1.wts >= obj2.wts) and (obj1.rts >= obj2.rts):
            return True
        else:
            return False
    else:
        print("isNewer: something is wrong.")
    
def getObj(act, tx, dp, flag ):
    # Goal: 1. find newest object in VIEWs, search most recent txs. If found, return.
    #       otherwise, search ACCOUNTs
    
    # flag = "read" or "write"
    
    # Logic:
        # 1. the most recent update may be in ACCOUNTs. Just committed.
        # 2. or in VIEWs[tx_i]: transaction i attemps to modify act
        # 3. serach newest tx. If found, then this one has all the dependency infomation saved in rts and wts
        # 4. we may return an obj not found in both data structures. It does not matter.

 
    obj = account(act)
    
    # check most recent transaction, then older ones; return on first found
    for t in sorted(VIEWs.keys(), reverse = True):
        print("getObj: Searching tx: " + tx)
        for k, v in VIEWs[t].items():
            print("getObj: Searching account: " + act + " , current account is: " + k)
            if k == act:
                print("getObj: found an account in view")
                if isNewerOrSame(v, obj, flag):
                    obj = copy.copy(v)
                    print("getObj: found account in VIEWs is newer.")
                    return obj
                else:
                    print("getObj: obj is older. Continue.")
                    
    # if           
    for k, v in ACCOUNTs.items():
        if k == act:
            if isNewerOrSame(v , obj, flag):
                obj = copy.copy(v)
                print("getObj: found account in ACCOUNTs.")
                return obj
    print("getObj: account not found in ACCOUNTs or VIEWs.")
    return obj
    
        
def deposit(name, value, tx, socket):
    print("Deposite " + name + " of " + str(value) + " by " + tx)
    dp = list() # dependency
    global VIEWs
    # get obj: a copy of most recent tentative or committed update
    obj = getObj(name, tx, dp, "write")
    print("Deposit: obj val = " + str(obj.value))
    if obj.wts != tx:
        dp.append(obj.wts)
    if obj.rts != tx:
        dp.append(obj.rts)
            
    # make changes and save into views
    if (obj.wts > tx) or (obj.rts > tx):
        # reject by timestamp
        print("Deposit: Conflict found by Timestamp Ordering.")
        socket.send( ("ABORT "+ tx) .encode() )
    else:
        # accept
        print("Deposit: Timestemp Ordering fine")
        # update obj
        print("Deposit: Val =   "+str(obj.value) )
        obj.value = obj.value + value
        obj.wts = tx
        
        lock.acquire()
        if tx not in VIEWs.keys():
            print("Deposit: Adding tx's view." )
            VIEWs[tx] = dict()
            VIEWs[tx][name] = obj
            print("Deposit: put obj in View: (tx,name,val)=" + tx + " "+ obj.name + " " + str(obj.value))
        else:
            print("Deposit: Update tx's view. Val =   "+str(obj.value) )

            VIEWs[tx][name] = obj
        lock.release()
        
        socket.send( ("OK " + tx +  " "+ "0" + " "  + " ".join(dp) ).encode() ) # 0 is dummy
            
    return

def balance( name, tx, socket ):
    print("Balance of " + name + " for " + tx)
    global VIEWs
    dp = list()
    
    obj = getObj(name, tx, dp, "read")
    # the object is not found anywhere
    # vprint("balance: val,rts,wts=" + str(obj.value) + ","+obj.wts+","+obj.rts)
    if (obj.value ==0) and (obj.rts == "") and (obj.wts == ""):
        print("balance: obj not found. ")
        socket.send( "NOT FOUND".encode() )
        return
    # if object exists
    if obj.wts != tx:
        dp.append(obj.wts)
    
    if (obj.wts > tx):
        print("Balance: Conflict found by Timestamp Ordering.")
        socket.send( ("ABORT " + tx).encode() )
    else:
        print("Balance: Timestemp Ordering fine")
        # update obj
        obj.rts = tx
        
        lock.acquire()
        if tx not in VIEWs.keys():
            VIEWs[tx] = dict()
            VIEWs[tx][name] = obj
        else:
            VIEWs[tx][name] = obj
        lock.release()
        socket.send( ("OK " + tx +  " " + str(obj.value) + " " + " ".join(dp) ).encode() ) # 0 is dummy
        
    print("Balanced: finished.")
    
def withdraw( name, value, tx, socket):
    print("Withdraw " + name + " of " + str(value) + " by " + tx)
    dp = list() # dependency
    global VIEWs
    # get obj: a copy of most recent tentative or committed update
    obj = getObj(name, tx, dp, "write")
   
    # the object is not found anywhere
    if (obj.value ==0) and (obj.rts == "") and (obj.wts == ""):
        socket.send( "NOT FOUND".encode() )
        return
    print("Withdraw: obj val = " + str(obj.value))
    if obj.wts != tx:
        dp.append(obj.wts)
    if obj.rts != tx:
        dp.append(obj.rts)
            
    # make changes and save into views
    if (obj.wts > tx) or (obj.rts > tx):
        # reject by timestamp
        print("Deposit: Conflict found by Timestamp Ordering.")
        socket.send( ("ABORT "+ tx) .encode() )
    else:
        # accept
        print("Withdraw: Timestemp Ordering fine")
        # update obj
        print("Withdraw: Val =   "+str(obj.value) )
        obj.value = obj.value - value
        obj.wts = tx
        
        lock.acquire()
        if tx not in VIEWs.keys():
            print("Withdraw: Adding tx's view." )
            VIEWs[tx] = dict()
            VIEWs[tx][name] = obj
            print("Withdraw: put obj in View: (tx,name,val)=" + tx + " "+ obj.name + " " + str(obj.value))
        else:
            print("Withdraw: Update tx's view. Val =   "+str(obj.value) )

            VIEWs[tx][name] = obj
        lock.release()
        
        socket.send( ("OK " + tx +  " "+ "0" + " "  + " ".join(dp) ).encode() ) # 0 is dummy
            
    return
    
    
def commit(tx,socket):
    # phase 1 of commit
    # check if account balance is valid(>0)
    if (tx in VIEWs.keys() ):
        for k, v in VIEWs[tx].items():
            if v.value < 0:
                print("Commit: found invalid account balance")
                socket.send( ("ABORT " + tx).encode() )
                return
    socket.send( ("OK" + " " + tx).encode() )

def doCommit(tx, socket):
    global ACCOUNTs
    global VIEWs
    lock.acquire()
    for name,val in VIEWs[tx].items():
        # all changed accounts "xyz"->class("xyz")
        ACCOUNTs[name] = val
    # delete view for the transaction
    del VIEWs[tx]
    lock.release()
    print('DoCOMMIT: have made changes to transaction: ' + tx)
    socket.send( ("OK" + " " + tx).encode() )
        
def abort(tx):
    del VIEWs[tx]

def msg_handler(msg, socket):
    # parse
    print("msg_handler: msg to process is:" + msg)
    action = msg.split()[0]  # DEPOSIT...
    tx = msg.split()[-1]
    if action in ["DEPOSIT", "WITHDRAW"] :
        name = (msg.split()[1]).split(".")[1]
        value = int(msg.split()[2])
        tx = msg.split()[3]
        if action == "DEPOSIT":
            deposit(name, value, tx, socket)
        elif action == "WITHDRAW":
            withdraw(name, value, tx, socket)
        else:
            print("something is wrong")
    elif action == "BALANCE":
        name = (msg.split()[1]).split(".")[1]
        tx = msg.split()[2]
        balance(name, tx, socket)
    elif action in ["COMMIT", "ABORT", "DoCOMMIT"]:
        tx = msg.split()[1]
        if action == "COMMIT":
            commit(tx, socket)
        elif action == "ABORT":
            abort(tx)
        elif action == "DoCOMMIT":
            doCommit(tx, socket)
        else:
            print("something is wrong")
    else:
        print("msg_handler: something is wrong. action= " + action)
    print("Finished handeling transaction: " + tx)

def create_socket():
    coord = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    coord.connect(("172.22.158.169", 1234))
    print("connected to coordinator")
    while True:
        msg = coord.recv(1024)
        if not msg:
            break
            
        msg = msg.decode()
        t = threading.Thread(target = msg_handler, args = (msg,coord))
        t.setDaemon(True)
        t.start()
    coord.close()
def main():
    # coordinator must be created first
    create_socket()
    
     

if __name__ == "__main__":
    main()