#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr  5 17:14:09 2020

@author: lishenhe
"""

# Python program killing 
# thread using daemon 
  
import threading 
import time 
import sys 
  
def func(): 
    while True: 
        time.sleep(0.5) 
        print('Thread alive, but it will die on program termination') 
  
t1 = threading.Thread(target=func) 
t1.daemon = True
t1.start() 
time.sleep(2) 
sys.exit() 