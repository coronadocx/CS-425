# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 23:56:36 2020

@author: Lishen He
"""
import sys
import struct
import threading
from queue import Queue
from queue import PriorityQueue
import socket
import time

MESSAGE_TIME = {}

JOB_QUEUE = Queue()
# ALL_MSG_HASH = set()  # Save the hash of all messages. 
                      # hash = Content +  sender IP + sender priority
                      # delete hash if message delivered
# ALL_MSG_RESPONSES = dict() # key = Hash; val = list(IP1, IP2, ...IP8)
# OUTGOING_QUEUE = Queue()
TRANS_QUEUE = Queue()
ACCOUNTS = {}
DEBUG = False
DEBUG_TEST_SEND = False
NUMBER_OF_THREADS = 10  
JOB_NUMBER = [1,2,3,4,5,6,8,9,10,11]
ALL_IP          = ["172.22.156.169", 
                   "172.22.158.169",
                   "172.22.94.168",
                   "172.22.156.170",
                   "172.22.158.170",
                   "172.22.94.169",
                   "172.22.156.171",
                   "172.22.158.171"]
ALL_CONNECTIONS = dict()
#ALL_ADDRESS = []
TO_CONNECT = []
PORT = 9999

SEEN_MSGS  = set()
SEEN_QUEUE = Queue()
SEEN_MAX   = 51200
R_QUEUE    = Queue()
I_QUEUE    = Queue()
I_RESP     = {}
SELF_PRIO  = 0

BW_DATA = Queue()


hostname = socket.gethostname()    
SELF_IP = socket.gethostbyname(hostname)
lock = threading.Lock()
CONNECTION_FINISHED = False





def create_socket():
    global serv
    serv = socket.socket()
    
def bind_socket():
    host = ""
    serv.bind((host, PORT))
    serv.listen(8)
    
def on_new_client(clientsocket, clientAddress):
    global R_QUEUE
    global MESSAGE_TIME
    while True:
        raw_msglen = clientsocket.recv(4)
        if not raw_msglen:
            break
        msglen = struct.unpack('>I', raw_msglen)[0]
        #print(msglen) 
        # if not msglen:
            # break
        time.sleep(50/1000000.0)
        data = clientsocket.recv(msglen)
        # print("Data from " + clientAddress + " is " + data.decode())
        if not data:
            break
        data = data.decode()
        data_fields = data.split(',')
        data_hash = ','.join(data_fields[0:3])
        #print(msglen, data)
        lock.acquire()
        if (len(data) < 40):
            print("Client: ", msglen, data)
        R_QUEUE.put(data)
        MESSAGE_TIME[data_hash] = time.time()
        BW_DATA.put(sys.getsizeof(raw_msglen))
        BW_DATA.put(sys.getsizeof(data))
        lock.release()
        #TODO: append meta-data here before queue the message
        
        """
        lock.acquire()
        OUTGOING_QUEUE.put(data)
        lock.release()
        """
        
        # note that the following can be given a thread to improve
        # performance
        # process_incoming_data(data)
        # if DEBUG:
            # print("Received data :" + data)
        # TODO: Queue received messages
        
        
        """
        l = len(data)
        data = data.decode()
        data = data.split()
        
        # calculate delay
        delay = time.time() - float(data[0])
        f.write(data[0] + " " + str(delay) + " " + str(l) + '\n')
        data = data[0] + ' ' +data[2] + ' '+data[1]
        print(data)
        data_back = "I am SERVER!"
        clientsocket.send(data_back.encode())
        """
    clientsocket.close()
    
    lock.acquire()
    ALL_CONNECTIONS.pop(clientAddress) # Remove client from all connections
    lock.release()
    print(clientAddress + "disconnected")
    if DEBUG:
        print_avaiable_connections()

def accept_new_client():
    while True:
        try:
            conn, address = serv.accept()
            #serv.setblocking(1) # prevents timeout
            # print("Connection from: " + str(address))
            #start_new_thread(on_new_client, (conn,) )
            t = threading.Thread(target=on_new_client, args=(conn,address[0]))
            #t.daemon = True
            t.start()
                
        except:
            print("Error accepting connections. Should never reach here.")
            
def connect_to_all():
    # Try to connect to all other nodes
    # Only single thread runs this functions: thread is safe
    # Because we do not need to handle reconnection!
    # After all connections are made, return function and join main thread
    # 
    global TO_CONNECT
    global ALL_CONNECTIONS
    while TO_CONNECT:
        target = TO_CONNECT[0]
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            print(str(threading.current_thread()) + " is trying to connect : " + target)
            conn.connect((target, PORT))
            if DEBUG:
                print("Successfully connected to : " + target + ". Saving IP to ALL_CONNECTIONS...")
            lock.acquire()
            ALL_CONNECTIONS[target] =  conn
            lock.release()
            # The following does not need lock?
            TO_CONNECT = [x for x in TO_CONNECT if x!=target]
            print("Remaining IPs to connect is :" + str(TO_CONNECT))
        except:
            # Reconnection..
            time.sleep(1.0) # avoid reach deepest recursive call
            connect_to_all() # try again'
        

# Local Transaction and Balance Handling
def transactions():
    global TRANS_QUEUE
    global ACCOUNTS

    while True:
        lock.acquire()
        if TRANS_QUEUE.empty():
            lock.release()
            time.sleep(0.0001)  # Sleep to stop greedy thred
            continue
        trans = TRANS_QUEUE.get().split()
        lock.release()

        if trans[0] == "DEPOSIT":
            if trans[1] not in ACCOUNTS:
                ACCOUNTS[trans[1]] = 0
            ACCOUNTS[trans[1]]+= int(trans[2]) 

        elif trans[0] == "TRANSFER":
            if trans[1] not in ACCOUNTS:
                continue
            if ACCOUNTS[trans[1]] < int(trans[4]):
                continue
            if trans[3] not in ACCOUNTS:
                ACCOUNTS[trans[3]] = 0

            ACCOUNTS[trans[1]] -= int(trans[4])
            ACCOUNTS[trans[3]] += int(trans[4])

        else: 
            print("TRANSACTION ERROR : ",trans[0])


def print_balances():
    global ACCOUNTS

    while True:
        bal_str = "BALANCES " 
        lock.acquire()
        for act in ACCOUNTS:
            if ACCOUNTS[act] != 0:
                bal_str += "%s:%d " % (act,ACCOUNTS[act])
        lock.release()
        print(bal_str)
        time.sleep(5)

def get_stdin():
    global R_QUEUE
    global SELF_IP
    global SELF_PRIO

    for line in sys.stdin:
        line = ' '.join(line.split())
        lock.acquire()
        line += ",%s,%s,%s,%s" % (SELF_IP, SELF_PRIO, SELF_IP, SELF_PRIO)
        SELF_PRIO += 1
        if len(line) < 40:
            print("getstd: ", line)
        R_QUEUE.put(line)
        lock.release()
        r_multicast(line)

def isis_deliver():
    global I_RESP
    global TRANS_QUEUE

    # Just go through the dictionary and deliver when possible
    while True: 
        least = (99999.0,0,False)
        lock.acquire()
        for k,val in I_RESP.items():
            if val < least:
                least = val
                key = k

        if least[2] == True:
            #print("Delivering to Transactions")
            #print(key)
            key_fields = key.split(',')
            #print(key_fields[0])
            TRANS_QUEUE.put(key_fields[0])
            #print(MESSAGE_TIME[key], (time.time()-MESSAGE_TIME[key]))
            MESSAGE_TIME[key] = time.time() - MESSAGE_TIME[key]

            del I_RESP[key]
            
        #print(I_RESP)
        lock.release()
        time.sleep(0.0001)
        #time.sleep(1)

def check_trans():
    global TRANS_QUEUE

    while True:
        lock.acquire()
        print(TRANS_QUEUE.queue)
        lock.release()
        time.sleep(1)

def clear_set():
    global SEEN_MSGS
    global SEEN_MAX
    global SEEN_QUEUE
    while True:
        lock.acquire()
        if SEEN_QUEUE.qsize() > SEEN_MAX:
            SEEN_MSGS.remove(SEEN_QUEUE.get())
        lock.release()
        time.sleep(0.001)

        

    
def isis_recv():
    global I_RESP
    global I_PQUEUE
    global I_QUEUE
    global SEEN_MSGS
    global SELF_PRIO
    global SELF_IP
    global ALL_CONNECTIONS

    while True:
        lock.acquire()
        if I_QUEUE.empty():
            lock.release()
            time.sleep(0.0001)
            continue
        msg = I_QUEUE.get()
        if len(msg) < 40:
            print("ISIS: " + msg)
        msg_fields = msg.split(',')
        send_ip    = msg_fields[1]
        resp_ip    = msg_fields[3]
        resp_prio  = float(msg_fields[4])
        msg_key    = ','.join(msg_fields[0:3])

        if send_ip == resp_ip and send_ip != SELF_IP:   # Request for priority
            msg_fields[3] = str(SELF_IP)
            msg_fields[4] = str(SELF_PRIO) 
            SELF_PRIO += 1
            #print(msg, type(msg))
            msg = ','.join(msg_fields)
            if (len(msg) < 40):
                print("ISIS2: ", msg)
            R_QUEUE.put(msg)
        # Response
        if msg_key in I_RESP:
            old = I_RESP[msg_key]
            new_prio  = max([resp_prio, old[0]])
            new_count = old[1]+1
            new_deliverable = True if new_count == (len(ALL_CONNECTIONS)+1) else False
            I_RESP[msg_key] = (new_prio, new_count, new_deliverable)
        else:
            I_RESP[msg_key] = (resp_prio, 1, False)
        lock.release()   
        
def print_avaiable_connections():
    #global ALL_CONNECTIONS
    print("Printing all available outoigng connections...")
    lock.acquire()
    for key in ALL_CONNECTIONS.keys():
        print(key)
    lock.release() 
    
def r_recv():
    # msg format : <CONTENT>,<SEND_IP>,<SEND_PRIO>,<RESP_IP>,<RESP_PRIO>
    global R_QUEUE
    global I_QUEUE
    global SEEN_MSGS
    global SELF_IP
    global SELF_PRIO
    global SEEN_QUEUE

    while True:
        lock.acquire()
        if R_QUEUE.empty():
            lock.release()
            time.sleep(0.0001)
            continue
        msg = R_QUEUE.get()
        if DEBUG:
            print("(r_recv) :: From R_QUEUE: " + msg)
        if len(msg) < 40:
            print("r_recv: " + msg)
        msg_fields = msg.split(',')
        # content    = msg_fields[0]
        send_ip    = msg_fields[1]
        # send_prio  = msg_fields[2]
        # reply_ip   = msg_fields[3] 
        # reply_prio = msg_fields[4]

        # print("Seen Messages :")
        # print(SEEN_MSGS)
        if msg not in SEEN_MSGS:
            if DEBUG:
                print("(r_recv) :: Message not seen")
                print(send_ip, SELF_IP)
            SEEN_MSGS.add(msg)
            SEEN_QUEUE.put(msg)
            I_QUEUE.put(msg)
            #print("GOT: " + msg)
            lock.release()
            if send_ip != SELF_IP:  
                if DEBUG:
                    print("IPs aren't equal, multicasting")
                r_multicast(msg)
        else:
            lock.release()

def r_multicast(msg):
    global ALL_CONNECTIONS
    global R_QUEUE

    lock.acquire()
    if (len(msg) < 40):
        print("RCAST: ", msg) 
    R_QUEUE.put(msg)
    msg = struct.pack('>I', len(msg)) + msg.encode()
    for ip,recv_socket in ALL_CONNECTIONS.items():
        try:
            recv_socket.sendall(msg)
        except: 
            print("Could not send message")
    lock.release()

def track_bandwidth():
    global BW_DATA
    global MESSAGE_TIME

    while True:
        size = 0
        lock.acquire()
        keys = []
        with open("./time_data.csv", "a") as f:
            for key,val in MESSAGE_TIME.items():
                if (val < 15):
                    f.write(str(val)+',')
                    keys.append(key)
            f.write('\n')

        for key in keys:
            del MESSAGE_TIME[key]
        lock.release()
        time.sleep(1)


def test_send_to_all():
    n = 0
    while True:
        n = n+1
        time.sleep(0.5)
        lock.acquire()
        for key, val in ALL_CONNECTIONS.items():
            if DEBUG:
                print("There are " + str(len(ALL_CONNECTIONS.keys())) + " available connections.")
            msg = str(n)+"'th test send-to-all from: " + SELF_IP + "\n"
            try:
                val.send(msg.encode())
            except:
                # remove connections from available 
                ALL_CONNECTIONS.pop(key)    # FIXME This breaks on_new_client()
        lock.release()
   
def create_workers():
    threads = []
    for i_thread in range(NUMBER_OF_THREADS):
        t = threading.Thread(target = work, args = (threads,))
        threads.append(t)
        # t.daemon = True
        t.start()

def work(threads):
    global CONNECTION_FINISHED
    if len(threads) > 2:
        # Let's wait for connection to complete
        if DEBUG:
            print("I'm waiting for connection to complete...")
        while not CONNECTION_FINISHED:
            # wait for connection to finish
            time.sleep(0.5)
            pass
    while True:
        job = JOB_QUEUE.get()
        if job == 1:
            create_socket()
            bind_socket()
            accept_new_client()
        elif job == 2:
            connect_to_all()
            
            CONNECTION_FINISHED = True
            # single thread: it moves on..
            #if DEBUG:
            #    print("TEST: Print all available connections...")
            #    print_avaiable_connections()
            #if DEBUG or DEBUG_TEST_SEND:
            #    # the following never exits
            #    print("TEST: About to send to all...")
            #    test_send_to_all()
            print("Finished all connections...")
        elif job == 3:
            r_recv()
        elif job == 4:
            get_stdin()
        elif job == 5:
            isis_recv()
        elif job == 6:
            isis_deliver()
        elif job == 7:
            check_trans()
        elif job == 8: 
            transactions()
        elif job == 9: 
            print_balances()
        elif job == 10:
            clear_set()
        elif job == 11:
            track_bandwidth()
        if DEBUG:
            # get thread ID. Thread should never finish their work
            pass
        JOB_QUEUE.task_done()
       
def create_jobs():
    # create jobs for threads to execute
    # JOB_QUEUE is thread-safe queue
    for x in JOB_NUMBER:
        JOB_QUEUE.put(x)
    
    # The following prevents the main thread from moving forward and exit
    JOB_QUEUE.join()
        

def main(argv):
    global N
    global TO_CONNECT
    global PORT
    global SELF_PRIO
    N = int(argv[0])

    if len(argv) > 1:
        PORT        = int(argv[1])
        SELF_PRIO   = int(argv[2])/10
    
    TO_CONNECT = ALL_IP[:N]
    TO_CONNECT = [x for x in TO_CONNECT if x!=SELF_IP]
    if DEBUG:
        print("IPs to connect is :" + str(TO_CONNECT))
    
    
    create_workers()
    create_jobs()


if __name__ == "__main__":
    main(sys.argv[1:])


        
#def r_recv():
    # Goal: process single incomeing data FROM other nodes
    #       and r-multicast if necessary
    
    # if seen the message before, do nothing 
    
    # if the message is from self, then give a priority and send to all
    # this also measn that 
    
    # if the message is a response, modifty the priority of the message
    """
    Message design:
        msg = content + Org. Sender IP + Org. Sender Priority + Responder IP +\
              Responder's Priority
        (All parts separated by ",")
        Because the receiver needs to know:
            * Identify of the message
                content does not uniquely identify a message
                but 'content + original sender IP + original sender's priority' can
            * responder's priority
            * content
            
        Example:
            DEPOSIT a 10, "172.22.156.169", 3, "172.22.156.170", 5
    """
    
    """
    * If the meessage is never seen[in terms of hash]
        - calculate own priority
        - compare with the prio in the data and calcualte max_prio
        - create local entries (save.. datastructures not fully determined)
          and save message/hash with max_prio
        - append meta-data to message and send to all

    * If the message has been seen[in terms of hash]
      - If not seen the response b4
        -- calculate max_prio and update if necessary
        -- save this new response to a list/dict to a specific message(hash)
      - If seen the responseb4
        -- Disgard the message. This means we have seen the exact message b4.
    """

   
