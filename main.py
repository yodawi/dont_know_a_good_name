import time
import os
import random
import uuid
import numpy as np
import matplotlib.pyplot as plt

times = []

def uuid(chars, length):
  return ''.join(random.choice(chars) for _ in range(length))


os.system("gcc main.c -o main  && ./main create test")
# qrstuvwxyz
#67
for i in range(500):
  email = uuid('abcdefghijklmnop', 10)
  a = os.system("./main add test "+email+" Password1!")