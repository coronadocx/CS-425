#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 05:17:44 2020

@author: lishenhe
"""

"""
read all files in current folder:
    self_name_1.out
    self_name_2.out
    ...
    self_name_n.out
    
within each file, a list of transaction messages:
    TRANSACTION 1587083646.139143 cc2780f7cf0b5f84ccee521bd8a94472 0 5 227 100747.488736
    TRANSACTION 1587083647.078524 33c85f7beabea8af224383e687502051 6 2 5 100748.427803
    TRANSACTION 1587083647.443357 d45ef368ff30e3951a25592aa4b2d943 4 3 4 100748.794415
    TRANSACTION 1587083648.309645 b14816bfdb25b7edf98ce71db9fa8ce7 8 1 132 100749.660432
    TRANSACTION 1587083648.791658 8e4655fae97e3742c598d6eb7869ccb0 8 6 2 100750.141879

"""
import os
import numpy as np
import matplotlib.pyplot as plt


msg = dict() #key = content; value = [received time stamps]
count = 0
for filename in os.listdir(os.getcwd()+"/data_blocked_trans_first_seen"):
    if filename[0] == ".":
        continue
    with open(os.path.join(os.getcwd() + "/data_blocked_trans_first_seen", filename), 'r') as f:
        while True:
            count = count + 1
            line = f.readline().rstrip()
            if not line:
                break
            # if not empty
            t = line.split()[-1]
            t = float(t)
            line = line.split()
            line = line[:-1]
            s = " "
            line = s.join(line)
            if line not in msg:
                msg[line] = np.array(t)
            else:
                msg[line] = np.append(msg[line], t )
            
        f.close()

MSG = dict   
for filename in os.listdir(os.getcwd()+"/data_blocked_trans"):
    if filename[0] == ".":
        continue
    with open(os.path.join(os.getcwd() + "/data_blocked_trans", filename), 'r') as f:
        while True:
            count = count + 1
            line = f.readline().rstrip()
            if not line:
                break
            # if not empty
            t = line.split()[-1]
            t = float(t)
            line = line.split()
            line = line[:-1]
            s = " "
            line = s.join(line)
            if line not in MSG:
                MSG[line] = np.array(t)
            else:
                MSG[line] = np.append(MSG[line], t )
        f.close()
        


msg_t     = np.empty(0)
msg_T     = np.empty(0)

for k, v in msg.items():
    t = float(k.split()[1])
    if k not in MSG.keys():
        continue
    msg_t = np.append(msg_t, t)
    T = float(MSG[k].split()[-1])
    msg_T = np.append(msg_t, T)

diff = msg_T - msg_t


# sorg
ind = np.argsort(msg_t)
fig = plt.figure()
ax = fig.axes
plt.plot(msg_t[ind], diff[ind], 'b')
plt.xlabel("Timestamp in second")
plt.ylabel("Time Needed for a Transaction to Appear in a Block(sec)")
plt.title("case: 20 nodes, 0.1 tps")
plt.show()


    
    
        
        
        
        