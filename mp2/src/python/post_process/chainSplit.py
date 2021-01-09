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
    100747.488736
    100748.427803
    100748.794415
    100749.660432
    100750.141879

"""
import os
import numpy as np
import matplotlib.pyplot as plt


split = np.empty() #key = content; value = [received time stamps]
count = 0
for filename in os.listdir(os.getcwd()+"/chainSplit"):
    if filename[0] == ".":
        continue
    with open(os.path.join(os.getcwd() + "/chainSplit", filename), 'r') as f:
        while True:
            count = count + 1
            line = f.readline().rstrip()
            if not line:
                break
            # if not empty
            t = float(line)

            split = np.append(s, t)
            
        f.close()

# sorg
ind = np.argsort(msg_t)
split = split[ind]
fig, ax = plt.figure()
axs.hist(split, bins=(floor(split[-1] - split[0])))
plt.xlabel("Timestamp(sec)")
plt.ylabel("Total Chain Split per Second")
plt.title("case: 20 nodes, 1.0 m/s, 0.1 tps")
plt.show()


    
    
        
        
        
        