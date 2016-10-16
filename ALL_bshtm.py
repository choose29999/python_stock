# coding=utf-8
'''
argv[1]: database user option
		0:	number_0.txt single_point0 point0 single_stock00
		1:	number_1.txt single_point1 point1 single_stock01
		2:	number_2.txt single_point2 point2 single_stock02
		3:	number_3.txt single_point3 point3 single_stock03
'''
import time
import sys,os
def log(msg):
	log=open('log/bshtm.log','a')
	log.writelines(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' '+msg+'\n')
	log.close()
	
'''
This program is used to initialize stock database,
and need an input file which contains sotck number list

e.q.:
0050	ĸŪƸǗ50	TW0000050004	20030630	ŗū	NULL	EUOXSR

arguement list:
	argv[1]: file name
'''
fileNmae=''
user1=''
user2=''
user3=''
if int(sys.argv[1]) == 0:
	fileNmae='number_0.txt'
	user1='single_point0'
	user2='point0'
	user3='single_ana0'
	user4='single_stock00'	
elif int(sys.argv[1]) == 1:
	fileNmae='number_1.txt'
	user1='single_point1'
	user2='point1'
	user3='single_ana1'
	user4='single_stock01'	
elif int(sys.argv[1]) == 2:
	fileNmae='number_2.txt'
	user1='single_point2'
	user2='point2'
	user3='single_ana2'
	user4='single_stock02'		
elif int(sys.argv[1]) == 3:
	fileNmae='number_3.txt'
	user1='single_point3'
	user2='point3'
	user3='single_ana3'
	user4='single_stock03'	
else:
	print "Wrong user option!!"
	exit()
cmd = 'python.exe bshtm_1.py %s %s %s %s'
f=open(fileNmae,"r")
print "Start ALL_bshtm file: "+fileNmae+" at: "+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
log("Start ALL_bshtm file: "+fileNmae)
for line in f.readlines():
	data = line.split('\t')
	if len(data[0]) != 4:
		continue
	print data[0]
	os.system(cmd%(data[0],user1,user2,user3))
f.close()
print "Finish ALL_bshtm file: "+fileNmae+" at: "+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
log("Finish ALL_bshtm file: "+fileNmae)