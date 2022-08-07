import time
import os
import random
import uuid
import numpy as np
import matplotlib.pyplot as plt

times = []

def uuid(chars, length):
  return ''.join(random.choice(chars) for _ in range(length))


#os.system("./main create test")

for i in range(1000):
  email = uuid('abcdefghijklmnopqrstuvwxyz', 49)
  start = time.time()
  os.system("./main add test "+email+" Password1!")
  times = times + [time.time() - start]
  #

#print(times)
print(np.array(times).mean()*1000)

plt.plot(np.array(times)*1000)
plt.show()
