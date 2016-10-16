# coding=utf-8
'''
This program is used to update 
-------------------------------------------
database: 
	three_major
table: 
	TWT44U
column:	
	trade_date
	sotck_number
	twt44u_buy_quality
	twt44u_sell_quality
	twt44u_total_quality
-------------------------------------------
database: 
	single_stock
table:	
	*
column: 
	twt44u_buy_quality
	twt44u_sell_quality
	twt44u_total_quality
-------------------------------------------

search table for start day
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
import MySQLdb

today=datetime.today().date()
password='44449999'
serverIP='127.0.0.1'


def isInt(value):
  try:
    int(value)
    return True
  except:
    return False
 
def log(msg):
	f=open('log/TWT44U.log','a+')
	f.writelines(time.strftime("%Y/%m/%d %H:%M:%S  ", time.localtime()))
	f.writelines(msg+'\n')
	f.close()

url = 'http://www.twse.com.tw/ch/trading/fund/TWT44U/TWT44U.php'    

form_data={'download':'','qdate':'','sorting':'by_issue'}

insert_cmd ='\
INSERT INTO TWT44U \
(idx,trade_date,stock_number,twt44u_buy_quality,twt44u_sell_quality,twt44u_total_quality)\
VALUES \
(%s,%s,%s,%s,%s,%s)\
ON DUPLICATE KEY UPDATE \
trade_date=%s,\
stock_number=%s,\
twt44u_buy_quality=%s,\
twt44u_sell_quality=%s,\
twt44u_total_quality=%s'

'''
single_point_insert_header = 'INSERT INTO s%s'
single_point_insert_body =' \
(trade_date,twt44u_buy_quality,twt44u_sell_quality,twt44u_total_quality) \
VALUES (%s,%s,%s,%s) \
ON DUPLICATE KEY UPDATE \
twt44u_buy_quality=%s,\
twt44u_sell_quality=%s,\
twt44u_total_quality=%s)'
'''
single_point_update_header = 'UPDATE s%s SET '
single_point_update_body =' \
twt44u_buy_quality=%s,\
twt44u_sell_quality=%s,\
twt44u_total_quality=%s \
WHERE trade_date=%s'

#connect to database
conn=MySQLdb.connect(host=serverIP,user='three_major2',passwd=password,db='three_major',port=3306,use_unicode=True,charset='utf8')
cur=conn.cursor()
single_stock_conn=MySQLdb.connect(host=serverIP,user='single_stock3',passwd=password,db='single_stock',port=3306,use_unicode=True,charset='utf8')
single_stock_cur=single_stock_conn.cursor()

log('init_TWT44U: Start!!!!')

#endYear = int(sys.argv[1])
endYear=today.year
if endYear > 1000:
	endYear -=1911
#get lastest day in DB
single_stock_cur.execute('SELECT trade_date, price_close FROM s0050 ORDER BY trade_date DESC LIMIT 1')
lastest_day =''
last_day_close='0'
for date,price in cur:
	lastest_day=date
	last_day_close=price
startYear=lastest_day.year

print 'init_TWT44U: start  date %d/%02d/%02d'%(lastest_day.year-1911,lastest_day.month,lastest_day.day)
print 'init_TWT44U: finish date %d/%02d/%02d'%(today.year-1911,today.month,today.day)
log('init_TWT44U: start  date %d/%02d/%02d'%(lastest_day.year-1911,lastest_day.month,lastest_day.day))
log('init_TWT44U: finish date %d/%02d/%02d'%(today.year-1911,today.month,today.day))

for year in range(startYear,endYear+1):
	if year==endYear:
		endMonth=today.month
	else:
		endMonth=12
	if year==startYear:
		startMonth=lastest_day.month
	else:
		startMonth=1
	for month in range(startMonth,endMonth+1):
		if year==endYear and month==today.month:
			endDay=today.day
		else:
			endDay=31
		if year==startYear and month==lastest_day.month:
			startDay=lastest_day.day+1
		else:
			startDay=1
		for day in range(startDay,endDay+1):
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
				single_stock_insert_cmd=single_point_update_header%(stock_number)+single_point_update_body
				#single_stock_cur.execut(single_stock_insert_cmd,(trade_date,buy_quality,sell_quality,total_quality,buy_quality,sell_quality,total_quality))
				single_stock_cur.execut(single_stock_update_cmd,(buy_quality,sell_quality,total_quality,trade_date))
				single_stock_conn.commit()
				#print single_stock_insert_cmd%(trade_date,buy_quality,sell_quality,total_quality,buy_quality,sell_quality,total_quality)
				#print single_stock_update_cmd%(buy_quality,sell_quality,total_quality,trade_date)
			cur.executemany(insert_cmd,data_list)
			conn.commit()
			#print data_list

'''release resource'''
cur.close()
conn.close()
single_stock_cur.close()
single_stock_conn.close()

log('init_TWT44U: Finish!!!!')
