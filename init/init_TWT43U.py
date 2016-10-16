# coding=utf-8
'''
This program is used to initialize 
-------------------------------------------
database: 
	three_major
table: 
	TWT43UA
	TWT43UB
	TWT43UC
column:	
	trade_date
	sotck_number
	twt43ua_buy_quality
	twt43ua_sell_quality
	twt43ua_total_quality

	trade_date
	sotck_number
	twt43ub_buy_quality
	twt43ub_sell_quality
	twt43ub_total_quality

	trade_date
	sotck_number
	twt43uc_buy_quality
	twt43uc_sell_quality
	twt43uc_total_quality
-------------------------------------------
database: 
	single_stock
table:	
	*
column: 
	twt43ua_buy_quality
	twt43ua_sell_quality
	twt43ua_total_quality
	twt43ub_buy_quality
	twt43ub_sell_quality
	twt43ub_total_quality
	twt43uc_buy_quality
	twt43uc_sell_quality
	twt43uc_total_quality
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
	f=open('log/TWT43U.log','a+')
	f.writelines(time.strftime("%Y/%m/%d %H:%M:%S  ", time.localtime()))
	f.writelines(msg+'\n')
	f.close()
 
url = 'http://www.twse.com.tw/ch/trading/fund/TWT43U/TWT43U.php'    

form_data={'download':'','qdate':'','sorting':'by_issue'}

insert_cmd_a ='\
INSERT INTO TWT43UA \
(idx,trade_date,stock_number,twt43ua_buy_quality,twt43ua_sell_quality,twt43ua_total_quality)\
VALUES \
(%s,%s,%s,%s,%s,%s)\
ON DUPLICATE KEY UPDATE \
trade_date=%s,\
stock_number=%s,\
twt43ua_buy_quality=%s,\
twt43ua_sell_quality=%s,\
twt43ua_total_quality=%s'

insert_cmd_b ='\
INSERT INTO TWT43UB \
(idx,trade_date,stock_number,twt43ub_buy_quality,twt43ub_sell_quality,twt43ub_total_quality)\
VALUES \
(%s,%s,%s,%s,%s,%s)\
ON DUPLICATE KEY UPDATE \
trade_date=%s,\
stock_number=%s,\
twt43ub_buy_quality=%s,\
twt43ub_sell_quality=%s,\
twt43ub_total_quality=%s'

insert_cmd_c ='\
INSERT INTO TWT43UC \
(idx,trade_date,stock_number,twt43uc_buy_quality,twt43uc_sell_quality,twt43uc_total_quality)\
VALUES \
(%s,%s,%s,%s,%s,%s)\
ON DUPLICATE KEY UPDATE \
trade_date=%s,\
stock_number=%s,\
twt43uc_buy_quality=%s,\
twt43uc_sell_quality=%s,\
twt43uc_total_quality=%s'

drop_table_cmd_a= 'DROP TABLE TWT43UA'	
drop_table_cmd_a= 'DROP TABLE TWT43UB'
drop_table_cmd_a= 'DROP TABLE TWT43UC'
			
create_table_cmd_a= '\
CREATE TABLE TWT43UA (\
idx VARCHAR(20) UNIQUE,\
trade_date DATE NOT NULL,\
stock_number VARCHAR(8) NOT NULL,\
twt43ua_buy_quality INT NOT NULL,\
twt43ua_sell_quality INT NOT NULL,\
twt43ua_total_quality INT NOT NULL,\
twt43ua_total_money INT DEFAULT 0,\
PRIMARY KEY (idx))'

create_table_cmd_b= '\
CREATE TABLE TWT43UB (\
idx VARCHAR(20) UNIQUE,\
trade_date DATE NOT NULL,\
stock_number VARCHAR(8) NOT NULL,\
twt43ub_buy_quality INT NOT NULL,\
twt43ub_sell_quality INT NOT NULL,\
twt43ub_total_quality INT NOT NULL,\
twt43ub_total_money INT DEFAULT 0,\
PRIMARY KEY (idx))'

create_table_cmd_c= '\
CREATE TABLE TWT43UC (\
idx VARCHAR(20) UNIQUE,\
trade_date DATE NOT NULL,\
stock_number VARCHAR(8) NOT NULL,\
twt43uc_buy_quality INT NOT NULL,\
twt43uc_sell_quality INT NOT NULL,\
twt43uc_total_quality INT NOT NULL,\
twt43uc_total_money INT DEFAULT 0,\
PRIMARY KEY (idx))'

'''
single_point_insert_header = 'INSERT INTO s%s'
single_point_insert_body =' \
(trade_date,twt43ua_buy_quality,twt43ua_sell_quality,twt43ua_total_quality,twt43ub_buy_quality,twt43ub_sell_quality,twt43ub_total_quality,twt43uc_buy_quality,twt43uc_sell_quality,twt43uc_total_quality) \
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) \
ON DUPLICATE KEY UPDATE \
twt43ua_buy_quality=%s,\
twt43ua_sell_quality=%s,\
twt43ua_total_quality=%s,\
twt43ub_buy_quality=%s,\
twt43ub_sell_quality=%s,\
twt43ub_total_quality=%s,\
twt43uc_buy_quality=%s,\
twt43uc_sell_quality=%s,\
twt43uc_total_quality=%s)'
'''
single_point_update_header = 'UPDATE s%s SET '
single_point_update_body ='\
twt43ua_buy_quality=%s,\
twt43ua_sell_quality=%s,\
twt43ua_total_quality=%s,\
twt43ub_buy_quality=%s,\
twt43ub_sell_quality=%s,\
twt43ub_total_quality=%s,\
twt43uc_buy_quality=%s,\
twt43uc_sell_quality=%s,\
twt43uc_total_quality=%s \
WHERE trade_date'
if __debug__:
	#connect to database
	conn=MySQLdb.connect(host=serverIP,user='three_major3',passwd=password,db='three_major',port=3306,use_unicode=True,charset='utf8')
	cur=conn.cursor()
	single_stock_conn=MySQLdb.connect(host=serverIP,user='single_stock2',passwd=password,db='single_stock',port=3306,use_unicode=True,charset='utf8')
	single_stock_cur=conn.cursor()
	#drop table A
	try:
		cur.execute(drop_table_cmd_a)
		log(drop_table_cmd_a)
	except:
		pass
	#ceate table A
	cur.execute(create_table_cmd_a)
	log('create table TWT43UA')

	#drop table B
	try:
		cur.execute(drop_table_cmd_b)
		log(drop_table_cmd_b)
	except:
		pass
	#ceate table B
	cur.execute(create_table_cmd_b)
	log('create table TWT43UB')

	#drop table C
	try:
		cur.execute(drop_table_cmd_c)
		log(drop_table_cmd_c)
	except:
		pass
	#ceate table C
	cur.execute(create_table_cmd_c)
	log('create table TWT43UC')

log('init_TWT43U: Start!!!!')	
print 'init_TWT43U: start  date 104/01/01'
print 'init_TWT43U: finish date %d/%02d/%02d'%(today.year-1911,today.month,today.day)
log('init_TWT43U: start  date 104/01/01')
log('init_TWT43U: finish date %d/%02d/%02d'%(today.year-1911,today.month,today.day))
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
			#if tr<=3, means that day maybe holiday or something 
			if len(tr)<=3:
				continue
			data_list_a=[]
			data_list_b=[]
			data_list_c=[]
			trade_date=str(year+1911)+'/'+str(month)+'/'+str(day)
			for data in tr[3:len(tr)]:
				td=data.find_all('td')
				stock_number = td[0].get_text().replace(' ','')
				idx=trade_date+'-'+stock_number
				buy_quality_a = td[2].get_text().replace(',','')
				sell_quality_a= td[3].get_text().replace(',','')
				total_quality_a= td[4].get_text().replace(',','')
				insertData=(idx,trade_date,stock_number,buy_quality_a,sell_quality_a,total_quality_a,trade_date,stock_number,buy_quality_a,sell_quality_a,total_quality_a)
				data_list_a.append(insertData)
				buy_quality_b = td[5].get_text().replace(',','')
				sell_quality_b= td[6].get_text().replace(',','')
				total_quality_b= td[7].get_text().replace(',','')
				insertData=(idx,trade_date,stock_number,buy_quality_b,sell_quality_b,total_quality_b,trade_date,stock_number,buy_quality_b,sell_quality_b,total_quality_b)
				data_list_b.append(insertData)
				buy_quality_c = td[8].get_text().replace(',','')
				sell_quality_c= td[9].get_text().replace(',','')
				total_quality_c= td[10].get_text().replace(',','')
				insertData=(idx,trade_date,stock_number,buy_quality_c,sell_quality_c,total_quality_c,trade_date,stock_number,buy_quality_c,sell_quality_c,total_quality_c)
				data_list_c.append(insertData)
				if len(stock_number)>4:
					continue
				#single_stock_insert_cmd=single_point_insert_header%(stock_number)+single_point_insert_body
				single_stock_insert_cmd=single_point_update_header%(stock_number)+single_point_update_body
				if __debug__:
					#single_stock_cur.execut(single_stock_insert_cmd,(trade_date,buy_quality_a,sell_quality_a,total_quality_a,buy_quality_b,sell_quality_b,total_quality_b,buy_quality_c,sell_quality_c,total_quality_c,buy_quality_a,sell_quality_a,total_quality_a,buy_quality_b,sell_quality_b,total_quality_b,buy_quality_c,sell_quality_c,total_quality_c))
					single_stock_cur.execut(single_stock_update_cmd,(buy_quality_a,sell_quality_a,total_quality_a,buy_quality_b,sell_quality_b,total_quality_b,buy_quality_c,sell_quality_c,total_quality_c,trade_date))
					single_stock_conn.commit()
				else:
					#print single_stock_insert_cmd%(trade_date,buy_quality_a,sell_quality_a,total_quality_a,buy_quality_b,sell_quality_b,total_quality_b,buy_quality_c,sell_quality_c,total_quality_c,buy_quality_a,sell_quality_a,total_quality_a,buy_quality_b,sell_quality_b,total_quality_b,buy_quality_c,sell_quality_c,total_quality_c)
					print single_stock_update_cmd%(buy_quality_a,sell_quality_a,total_quality_a,buy_quality_b,sell_quality_b,total_quality_b,buy_quality_c,sell_quality_c,total_quality_c,trade_date)			
			if __debug__:
				cur.executemany(insert_cmd_a,data_list_a)
				conn.commit()
				cur.executemany(insert_cmd_b,data_list_b)
				conn.commit()
				cur.executemany(insert_cmd_c,data_list_c)
				conn.commit()
			else:
				print data_list_a,data_list_b,data_list_c

'''release resource'''
if __debug__:
	cur.close()
	conn.close()
	single_stock_cur.close()
	single_stock_conn.close()
log('init_TWT43U: Finish!!!!')
