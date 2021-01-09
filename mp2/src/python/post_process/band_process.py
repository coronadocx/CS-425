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
    109350.189008 71
    109350.189593 71
    109350.189616 71
    109350.190451 71
    109350.191159 71

"""
import os
import numpy as np
import matplotlib.pyplot as plt


msg = dict() #key = content; value = [received time stamps]
count = 0
for filename in os.listdir(os.getcwd()+"/data_band"):
    if filename[0] == ".":
        continue
    with open(os.path.join(os.getcwd() + "/data_band", filename), 'r') as f:
        while True:
            count = count + 1
            line = f.readline().rstrip()
            if not line:
                break
            # if not empty
            t = line.split()[0]
            t = float(t)
            t = np.floor(t)
            nb = int(line.split()[1])

            if t not in msg:
                msg[t] = nb
            else:
                msg[t] = msg[t] + nb
            
        f.close()
        

x = list(msg.keys())
y = list(msg.values())
plt.figure()
plt.bar(x,y, width=1.0, color = 'orange')
plt.xlabel("Timestamp in second")
plt.ylabel("Bandwidth in Bytes/sec")
plt.title("Bandwidth for 100 Nodes")

    

"""
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
plt.title("Delay for 20 Nodes")
plt.show()

plt.figure()
plt.plot(msg_t[ind], msg_reach[ind])
plt.xlabel("Timestamp in second")
plt.ylabel("Number of Reached Nodes")
plt.title("Reachability for 20 Nodes")
plt.show()
"""

    

    
    
        
        
        
        