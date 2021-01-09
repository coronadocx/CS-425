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
for filename in os.listdir(os.getcwd()+"/data_trans"):
    if filename[0] == ".":
        continue
    with open(os.path.join(os.getcwd() + "/data_trans", filename), 'r') as f:
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

msg_t     = np.empty(0)
msg_delay_min = np.empty(0)
msg_delay_max = np.empty(0)
msg_delay_med = np.empty(0)
msg_reach     = np.empty(0)

for k, v in msg.items():
    print(v)
    msg_reach = np.append(msg_reach, v.size)
    t = float(k.split()[1])
    msg_t = np.append(msg_t, t)
    if (v.size > 1):
        sorted_arr = np.sort(v)
    else:
        sorted_arr = v
    if (sorted_arr.size > 1):
        msg_delay_min = np.append(msg_delay_min , sorted_arr[1] - sorted_arr[0])
        msg_delay_max = np.append(msg_delay_max , sorted_arr[-1] - sorted_arr[0])
        msg_delay_med = np.append(msg_delay_med , np.median(sorted_arr - sorted_arr[0]))
    else:
        msg_delay_min = np.append(msg_delay_min, 0)
        msg_delay_max = np.append(msg_delay_max, 0)
        msg_delay_med = np.append(msg_delay_med, 0)

# sorg
ind = np.argsort(msg_t)
fig = plt.figure()
ax = fig.axes
plt.plot(msg_t[ind], msg_delay_max[ind], 'b', msg_t[ind], msg_delay_med[ind], 'r', msg_t[ind], msg_delay_min[ind], 'g')
plt.legend(["max delay", "median delay", "min delay"])
plt.xlabel("Timestamp in second")
plt.ylabel("Delayed Time in second")
plt.title("Delay for 100 Nodes")
plt.show()

plt.figure()
plt.plot(msg_t[ind], msg_reach[ind])
plt.xlabel("Timestamp in second")
plt.ylabel("Number of Reached Nodes")
plt.title("Reachability for 100 Nodes")
plt.show()

    

    
    
        
        
        
        