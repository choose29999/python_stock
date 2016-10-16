# coding=utf-8
'''
This program is used to initialize 
-------------------------------------------
database: 
	single_stock
table:
	*
column: 
	public_stock
	mi_qf_stock
	mi_qf_ratio
-------------------------------------------
default start date: 104/01/01
default end date: datetime.today().date()
'''
from __future__ import division
import os,sys
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
today=datetime.today().date()
password='44449999'
serverIP='127.0.0.1'

if __debug__:
	import MySQLdb
 
def log(msg):
	f=open('log/MI_QFIIS.log','a+')
	f.writelines(time.strftime("%Y/%m/%d %H:%M:%S  ", time.localtime()))
	f.writelines(msg+'\n')
	f.close()

url = 'http://www.twse.com.tw/ch/trading/fund/MI_QFIIS/MI_QFIIS.php'    

form_data={'download':'','qdate':'','sorting':'by_stkno','select2':'all'}
'''
single_stock_insert_header = 'INSERT INTO s%s '
single_stock_insert_body ='(\
trade_date,public_stock,mi_qf_stock,mi_qf_ratio,transfer_ratio) \
VALUES (%s,%s,%s,%s) \
ON DUPLICATE KEY UPDATE \
public_stock=%s,\
mi_qf_stock=%s,\
mi_qf_ratio=%s,\
transfer_ratio=%s)'
'''
single_stock_update_header = 'UPDATE s%s SET '
single_stock_update_body ='\
public_stock=%s,\
mi_qf_stock=%s,\
mi_qf_ratio=%s,\
transfer_ratio=%s \
WHERE trade_date=%s'

single_stock_select_header = 'SELECT trade_stock FROM s%s '
single_stock_select_body ='WHERE trade_date=%s'


if __debug__:
	#connect to database
	single_stock_conn=MySQLdb.connect(host=serverIP,user='single_stock1',passwd=password,db='single_stock',port=3306,use_unicode=True,charset='utf8')
	single_stock_cur=single_stock_conn.cursor()
log('init_MI_QFIIS: Start!!!!')
print 'init_MI_QFIIS: start  date 104/01/01'
print 'init_MI_QFIIS: finish date %d/%02d/%02d'%(today.year-1911,today.month,today.day)
log('init_MI_QFIIS: start  date 104/01/01')
log('init_MI_QFIIS: finish date %d/%02d/%02d'%(today.year-1911,today.month,today.day))
#endYear = int(sys.argv[1])
endYear=today.year
if endYear > 1000:
	endYear -=1911
endMonth= 12
endDay=31
for year in range(104,endYear+1):
	if year==endYear:
		endMonth=today.month
	for month in range(1,endMonth+1):
		print year,month
		if year==endYear and month==today.month:
			endDay=today.day
		for day in range(1,endDay+1):
			form_data['qdate']="%d/%02d/%02d"%(year,month,day)
			while True:
				r = requests.post(url,data=form_data)
				if r.status_code != requests.codes.ok:
					continue
				soup = BeautifulSoup(r.content,'html.parser')
				table=soup.find_all('table')
				try:
					date_info=table[0].find_all('tr')[0].find_all('td')[0].get_text()
				except:
					log("Can't get date_info "+form_data['qdate'])
					continue
				if ( str(year) and str(month) and str(day) ) in date_info:
					break
				else:
					continue
			#start parsing
			tr = table[0].find_all('tr')
			#if tr[2].find_all('td')<=8 , means that day maybe holiday or something 
			#if len(tr[2].find_all('td'))<=8:
			#	continue
			data_list=[]
			trade_date=str(year+1911)+'/'+str(month)+'/'+str(day)
			for data in tr[2:len(tr)]:
				td=data.find_all('td')
				if len(td) < 8:
					continue
				stock_number = td[0].get_text().replace(' ','')
				if len(stock_number)>4:
					continue
				public_stock = td[3].get_text().replace(',','')
				mi_qf_stock = td[5].get_text().replace(',','')
				mi_qf_ratio = td[7].get_text()
				#single_stock_insert_cmd=single_stock_insert_header%(stock_number)+single_stock_insert_body
				single_stock_update_cmd=single_stock_update_header%(stock_number)+single_stock_update_body 
				if __debug__:
					single_stock_select_cmd=single_stock_select_header%(stock_number)+single_stock_select_body
					single_stock_cur.execut(single_stock_select_cmd,(trade_date))
					transfer_ratio=float(float(single_stock_cur[0].get('trade_stock'))/float(public_stock))
					#single_stock_cur.execut(single_stock_insert_cmd,(trade_date,public_stock,mi_qf_stock,mi_qf_ratio,public_stock,mi_qf_stock,mi_qf_ratio,transfer_ratio))
					single_stock_cur.execut(single_stock_update_cmd,(public_stock,mi_qf_stock,mi_qf_ratio,transfer_ratio,trade_date))
					single_stock_conn.commit()
				else:
					#print single_stock_insert_cmd%(trade_date,public_stock,mi_qf_stock,mi_qf_ratio,public_stock,mi_qf_stock,mi_qf_ratio,0)
					print single_stock_update_cmd%(public_stock,mi_qf_stock,mi_qf_ratio,transfer_ratio,0)


'''release resource'''
if __debug__:
	single_stock_cur.close()
	single_stock_conn.close()
log('init_MI_QFIIS: Finish!!!!')
