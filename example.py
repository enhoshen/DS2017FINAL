from dataloader import *
from datacollector import *
c=CYShare()
c.dataload(True) #must restore 
coll=datacollector(c)
coll.getcomb_info() # must reload 

