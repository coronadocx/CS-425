#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May  2 21:23:51 2020

@author: lishenhe
"""
# MESSAGES
# To servers
# From servers
    # rsp = 
    # 1. ABORT
    # 2. OK
    # 3. OK tx1 tx2 ... txn <- these are the dependencies
    # rational: since each server knows which client/tx it's communicating with
    #           bcz each client has unique thread, there is NO need to append tx ID
# From client
# To client
import socket
import threading
import time
from threading import Lock

lock = Lock()
ALL_IP          = ["172.22.156.169", 
                   "172.22.158.169",
                   "172.22.94.168",
                   "172.22.156.170",
                   "172.22.158.170",
                   "172.22.94.169",
                   "172.22.156.171",
                   "172.22.158.171"]
CLIENT = dict()  #"IP -> socket()"
BRANCHES = dict()  #"A or B or... E -> socket()"
TX_OK = list()     # commited transactions
TX_ABT = list()    # aborted transactions

def create_socket():
    serv = socket.socket()
    host = ""
    serv.bind( (host, 1234))
    serv.listen(16)
    while True:
        try:
            conn, address = serv.accept()
            if address[0] in ALL_IP[0:2] or address[0] in ALL_IP[7:]:
                t = threading.Thread(target=on_new_client, args=(conn,address[0]))
            elif address[0] in ALL_IP[2:7]:
                t = threading.Thread(target=on_new_branch, args=(conn, address[0]))
            t.setDaemon(True)
            t.start()
        except:
            time.sleep(1)
            print("Error accepting connections. Should never reach here.\n")

def on_new_client(socket, addr):
    # one threaded function of this handles a different client
    # maintain the life cycle of a transaction
    # socket is client socket
    needBegin = True
    dp = list() # dependency list
    
    while True:
        # one while loop handles one item in a transaction
        print("on_new_client: ready to take a new tx item.")
        msg = socket.recv(1024)
        print("im here")
        if not msg:
            break
        msg = msg.decode()
        print("on_new_client: received: " + msg)
        msg = msg.rstrip()
        # check if throw away wrong message
        if (needBegin):
            if (msg!="BEGIN"):
                print("Please enter BEGIN to start\n")
                socket.send("Please enter BEGTIN to start...".encode())
                continue
            else: # msg == "BEGIN"
                # create tx ID
                print("BEGIN: creating ID for tx.")
                tx = str(time.time())
                needBegin = False
                socket.send("OK".encode())
                print("BEGIN: responded to client.")
        
        action = msg.split()[0]
        if (action == "COMMIT"):
            # phase 0: check dependency
            print("COMMIT: checking dependencies.")
            for d in dp:
                if d in TX_OK:
                    print("COMMIT: this dependency is OK.")
                    continue
                elif d in TX_ABT:
                    print("COMMIT: this dependency is aborted. " + d)
                    abort(socket, tx)
                    dp = list()
                    needBegin = True
                    continue
                else:
                    # dependency is not committed
                    print("COMMIT: this dependency is pending" + d)
                    while True:
                        time.sleep(1.0)
                        if d in TX_OK:
                            break
                        else:
                            continue
            # phase 1: info gathering
            abt = False
            for b , s in BRANCHES.items():
                s.send( ("COMMIT" + " " + tx).encode() )
                rsp = s.recv(1024)
                
                # msg:
                    # OK tx
                    # ABORT tx
                rsp = rsp.decode()
                print("COMMIT: receive from branch:" + rsp)
                rsp = rsp.split()
                if (rsp[0] == "ABORT"):
                    abort(socket, tx)
                    abt = True
                    print("COMMIT: ABORT transaction:" + tx)
                    break
                else:
                    print("on_new_client: COMMIT: this branch reports consistent transactions.")
                    #continue
            if (abt):
                print("on_new_client: ABORT finished. Next loop for a new transaction.")
                needBegin = True
                continue
            
            print("on_new_client: COMMIT: All branch's tx are consistent. Ready to make changes to database.")
            # reaching here means no abortion happend
            
            # phase 2: making changes
            for b, s in BRANCHES.items():
                s.send( ("DoCOMMIT" + " " + tx).encode() )
                rsp = s.recv(1024)
                rsp = rsp.decode()
                if ( rsp.split()[0] != "OK" ):
                    print("on_new_client: DoCOMMIT: something is wrong.rsp=" + rsp)
                
            print("on_new_client: COMMIT: Finished making changes to database.")
            
            # put transaction into TX_OK
            lock.acquire()
            TX_OK.append(tx)
            lock.release()
            needBegin = True
            dp = list()
            socket.send( "COMMIT OK".encode() )
            
        elif (action == "ABORT"):
            abort(socket, tx)
            print("ABORT: aborted")
            needBegin = True
            dp = list()
            
        elif (action in ["DEPOSIT" , "WITHDRAW", "BALANCE"] ): # DEPOSIT, WITHDRAW, BALANCE
            dest = ((msg.split()[1]).split("."))[0]
            s = BRANCHES[dest]
            m = msg + " " + tx
            s.send(m.encode())
            rsp = s.recv(1024)
            rsp = rsp.decode()
            print("on_new_client: receiving msg: " + rsp)
            # rsp = 
            # 1. ABORT TX
            # 2. NOT FOUND
            # 3. OK TX Val
            # 4. OK TX Val tx1 tx2 ... txn <- these are the dependencies
            rsp = rsp.split()
            
            if (rsp[0] == "ABORT"):
                # timestamp is too old!
                abort(socket, tx)
                needBegin = True
                dp = list()
            elif (rsp[0] == "NOT"):
                # not found in Balance
                socket.send( "NOT FOUND".encode() )
                needBegin = True
            elif (rsp[0] == "OK"):
                if len(rsp) > 2:
                    for item in rsp[3:]:
                        print("on_new_client: adding depency: " + item)
                        dp.append(item)
                # 
                if (action == "DEPOSIT"):
                    socket.send( "OK".encode() )
                elif (action == "BALANCE"):
                    socket.send( rsp[2].encode() )
                elif (action == "WITHDRAW"):
                    socket.send( "OK".encode() )
                else:
                    print("sth is wrong...")
            else:
                print("on_new_client, last part: rsp is wrong:" + " ".join(rsp))
        else:
            if (msg=="BEGIN"):
                print("on_new_client: wierd, why is begin here?")
                continue
            else:
                print("on_new_client: wrong message: " + msg)
                print("action: " + action)
        print("on_new_client: finished tx item:" + msg)
    
    print("Client disconnected: " + addr )
    socket.close()

def on_new_branch(conn, address):
    global BRANCHES
    if (address == "172.22.94.168"):
        BRANCHES['A'] = conn
    elif (address == "172.22.156.170"):
        BRANCHES['B'] = conn
    elif (address == "172.22.158.170"):
        BRANCHES['C'] = conn
    elif (address == "172.22.94.169"):
        BRANCHES['D'] = conn
    elif (address == "172.22.156.171"):
        BRANCHES['E'] = conn
    else:
        print("wrong VM")
    

def abort(client, tx):
    global TX_ABT
    # send abort message of tx to all branches
    for k, v in BRANCHES.items():
        m = "ABORT" + " " + tx
        v.send(m.encode())
    
    # send "ABORTED" to THE client
    client.send( "ABORTED".encode() )
    
    # delete entry in TX
    lock.acquire()
    TX_ABT.append(tx)
    lock.release()
"""
def commit(socket, tx, dp):
    global TX_OK
    # dp: dependency list
    for item in dp:
        if item in TX_ABT:
            abort(socket, tx)
            return
    for item in dp:
        if item not in TX_OK:
            # some dependency is pending
            # let's just abort for now
            abort(socket, tx)
            return
    
    # phase 1: get votes
    abt = False
    for k, v in BRANCHES.items():
        m = "COMMIT" + " " + tx
        v.send(m.encode())
        rsp = v.recv(4)
        if (rsp == "ABORT"):
            abt = True
            break

    # phase 2: decision
    if (abt):
       abort(socket, tx)
       return
    else:
        m = "DoCOMMIT" + " " + tx
        for k, v in BRANCHES.items():
            v.send(m.encode())
        
        lock.acquire()
        TX_OK.append(tx)
        lock.release()
        socket.send( "OK".encode() )
"""
    
def main():
    create_socket()

    
if __name__ == "__main__":
    main()
