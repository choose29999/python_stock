# coding=utf-8
'''
This program is used to initialize stock database,
and need an input file which contains sotck number list

e.q.:
0050	两j帆W50	TW0000050004	20030630	䗥뉎ULL	EUOXSR

arguement list:
    argv[1]: data group option
        0:  number_0.txt single_stock00
        1:  number_1.txt single_stock01
        2:  number_2.txt single_stock02
        3:  number_3.txt single_stock03
'''

def log(msg):
	log=open('log/STOCK_DAYMAIN.log','a')
	log.writelines(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' '+msg+'\n')
	log.close()
 
import sys,os
from datetime import datetime
import time
today=datetime.today().date()

fileName=''
user=''
if int(sys.argv[1])== 0:
    fileName='number1.txt'
    user='single_stock00'
elif int(sys.argv[1])== 1:
    fileName='number_1.txt'
    user='single_stock01'
elif int(sys.argv[1])== 2:
    fileName='number_2.txt'
    user='single_stock02'
elif int(sys.argv[1])== 3:
    fileName='number_3.txt'
    user='single_stock03'
else:
    print "Wrong group option!!"
    exit()
cmd = 'python.exe init\init_STOCK_DAYMAIN.py %s %s'
f=open(fileName,"r")
log("***Start initializing %s***"%(fileName))
print "Start initializing %s"%(fileName)
for line in f.readlines():
	data = line.split('\t')
	if len(data[0]) != 4:
		continue
	os.system(cmd%(data[0],user))
log("***Finish initializing %s***"%(fileName))
f.close()
