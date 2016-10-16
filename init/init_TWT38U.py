# coding=utf-8
'''
This program is used to initialize
-------------------------------------------
database: 
	three_major
table: 
	TWT38U
column:	
	trade_date
	sotck_number
	twt38u_buy_quality
	twt38u_sell_quality
	twt38u_total_quality
-------------------------------------------
database: 
	single_stock
table:	
	*
column: 
	twt38u_buy_quality
	twt38u_sell_quality
	twt38u_total_quality
-------------------------------------------
default start date: 104/01/01
default end date: datetime.today().date()

after runing this script,
we must run another script (XXXXXXXX) to get total_money
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

def isInt(value):
  try:
    int(value)
    return True
  except:
    return False

def log(msg):
	f=open('log/TWT38U.log','a+')
	f.writelines(time.strftime("%Y/%m/%d %H:%M:%S  ", time.localtime()))
	f.writelines(msg+'\n')
	f.close()
 
url = 'http://www.twse.com.tw/ch/trading/fund/TWT38U/TWT38U.php'    

form_data={'download':'','qdate':'','sorting':'by_issue'}

insert_cmd = '\
INSERT INTO TWT38U \
(idx,trade_date,stock_number,twt38u_buy_quality,twt38u_sell_quality,twt38u_total_quality)\
VALUES \
(%s,%s,%s,%s,%s,%s) \
ON DUPLICATE KEY UPDATE \
trade_date=%s,\
stock_number=%s,\
twt38u_buy_quality=%s,\
twt38u_sell_quality=%s,\
twt38u_total_quality=%s'

drop_table_cmd = 'DROP TABLE TWT38U'	
		
create_table_cmd='\
CREATE TABLE TWT38U (\
idx VARCHAR(20) UNIQUE,\
trade_date DATE NOT NULL,\
stock_number VARCHAR(8) NOT NULL,\
twt38u_buy_quality INT NOT NULL,\
twt38u_sell_quality INT NOT NULL,\
twt38u_total_quality INT NOT NULL,\
twt38u_total_money INT DEFAULT 0,\
PRIMARY KEY (idx))'

'''
single_point_insert_header = 'INSERT INTO s%s'
single_point_insert_body =' \
(trade_date,twt38u_buy_quality,twt38u_sell_quality,twt38u_total_quality) \
VALUES (%s,%s,%s,%s) \
ON DUPLICATE KEY UPDATE \
twt38u_buy_quality=%s,\
twt38u_sell_quality=%s,\
twt38u_total_quality=%s)'
'''
single_point_update_header = 'UPDATE s%s SET '
single_point_update_body =' \
twt38u_buy_quality=%s,\
twt38u_sell_quality=%s,\
twt38u_total_quality=%s \
WHERE trade_date=%s'

if __debug__:
	#connect to database
	conn=MySQLdb.connect(host=serverIP,user='three_major1',passwd=password,db='three_major',port=3306,use_unicode=True,charset='utf8')
	cur=conn.cursor()
	single_stock_conn=MySQLdb.connect(host=serverIP,user='single_stock4',passwd=password,db='single_stock',port=3306,use_unicode=True,charset='utf8')
	single_stock_cur=conn.cursor()
	#drop table
	try:
		cur.execute(drop_table_cmd)
		log(drop_table_cmd)
	except:
		pass

	#ceate table
	cur.execute(create_table_cmd)
	log('create table TWT38U')
log('init_TWT38U: Start!!!!')	
print 'init_TWT38U: start  date 104/01/01'
print 'init_TWT38U: finish date %d/%02d/%02d'%(today.year-1911,today.month,today.day)
log('init_TWT38U: start  date 104/01/01')
log('init_TWT38U: finish date %d/%02d/%02d'%(today.year-1911,today.month,today.day))
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
			#if tr<=2, means that day maybe holiday or something 
			if len(tr)<=2:
				continue
			data_list=[]
			trade_date=str(year+1911)+'/'+str(month)+'/'+str(day)
			for data in tr[2:len(tr)]:
				td=data.find_all('td')
				stock_number = td[1].get_text().replace(' ','')
				
				idx=trade_date+'-'+stock_number
				buy_quality = td[3].get_text().replace(',','')
				sell_quality= td[4].get_text().replace(',','')
				total_quality= td[5].get_text().replace(',','')
				insertData=(idx,trade_date,stock_number,buy_quality,sell_quality,total_quality,trade_date,stock_number,buy_quality,sell_quality,total_quality)
				data_list.append(insertData)
				if len(stock_number)>4:
					continue
				#single_stock_insert_cmd=single_point_insert_header%(stock_number)+single_point_insert_body
				single_stock_update_cmd=single_point_update_header%(stock_number)+single_point_update_body
				if __debug__:
					#single_stock_cur.execut(single_stock_insert_cmd,(trade_date,buy_quality,sell_quality,total_quality,buy_quality,sell_quality,total_quality))
					single_stock_cur.execut(single_stock_update_cmd,(buy_quality,sell_quality,total_quality,trade_date))
					single_stock_conn.commit()
				else:
					#print single_stock_insert_cmd%(trade_date,buy_quality,sell_quality,total_quality,buy_quality,sell_quality,total_quality)
					print single_stock_update_cmd%(buy_quality,sell_quality,total_quality,trade_date)
			if __debug__:
				cur.executemany(insert_cmd,data_list)
				conn.commit()
			else:
				print data_list

'''release resource'''
if __debug__:
	cur.close()
	conn.close()
	single_stock_cur.close()
	single_stock_conn.close()
log('init_TWT38U: Finish!!!!')	
