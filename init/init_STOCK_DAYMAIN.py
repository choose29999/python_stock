# coding=utf-8
'''
This program is used to initialize 
-------------------------------------------
database: 
    single_stock
talbe: 
    *
column: 
	trade_date
	price_high
	price_low
	price_open
	price_close
	trade_money
	trade_stock
	trade_times
	price_avg
	ma5
	ma10
	ma20
	ma60
	stock_ratio5
	stock_ratio10
	raise_ratio
-------------------------------------------	
argv[1]: stock number
argv[2]: single stock DB account
default start date: 104/01/01
default end date: datetime.today().date()
'''
from __future__ import division
import MySQLdb
import os,sys
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
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
	log=open('../log/STOCK_DAYMAIN.log','a')
	log.writelines(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+' '+msg+'\n')
	log.close()
 
url = 'http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY/STOCK_DAYMAIN.php'    

form_data={'download':'','query_year':'','query_month':'','CO_ID':'','query-button':'%E6%9F%A5%E8%A9%A2'}

insert_cmd = '\
INSERT INTO s'+sys.argv[1]+'(\
trade_date,\
price_high,\
price_low,\
price_open,\
price_close,\
trade_money,\
trade_stock,\
trade_times,\
price_avg,\
ma5,\
ma10,\
ma20,\
ma60,\
quality_ratio5,\
quality_ratio10,\
raise_ratio) \
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

drop_table_cmd	= 	"DROP TABLE s"+sys.argv[1]	

create_table_cmd= '\
CREATE TABLE s'+sys.argv[1]+' (\
trade_date DATE,\
price_high FLOAT(10,2),\
price_low FLOAT(10,2),\
price_open FLOAT(10,2),\
price_close FLOAT(10,2),\
trade_money BIGINT,\
trade_stock INT,\
trade_times INT,\
price_avg FLOAT(10,2),\
ma5 FLOAT(10,2),\
ma10 FLOAT(10,2),\
ma20 FLOAT(10,2),\
ma60 FLOAT(10,2),\
quality_ratio5 FLOAT(10,4),\
quality_ratio10 FLOAT(10,4),\
raise_ratio FLOAT(10,4),\
transfer_ratio FLOAT(10,4) DEFAULT 0.0000,\
chip1_price FLOAT(10,2) DEFAULT 0.00,\
chip1_quality INT DEFAULT 0,\
chip2_price FLOAT(10,2) DEFAULT 0.00,\
chip2_quality INT DEFAULT 0,\
public_stock INT DEFAULT 0,\
mi_qf_stock INT DEFAULT 0,\
mi_qf_ratio FLOAT(10,4) DEFAULT 0.0000,\
twt43ua_buy_quality INT DEFAULT 0,\
twt43ua_sell_quality INT DEFAULT 0,\
twt43ua_total_quality INT DEFAULT 0,\
twt43ub_buy_quality INT DEFAULT 0,\
twt43ub_sell_quality INT DEFAULT 0,\
twt43ub_total_quality INT DEFAULT 0,\
twt43uc_buy_quality INT DEFAULT 0,\
twt43uc_sell_quality INT DEFAULT 0,\
twt43uc_total_quality INT DEFAULT 0,\
twt44u_buy_quality INT DEFAULT 0,\
twt44u_sell_quality INT DEFAULT 0,\
twt44u_total_quality INT DEFAULT 0,\
twt38u_buy_quality INT DEFAULT 0,\
twt38u_sell_quality INT DEFAULT 0,\
twt38u_total_quality INT DEFAULT 0,\
bft41u_quality INT DEFAULT 0,\
bft41u_times INT DEFAULT 0,\
bft41u_money INT DEFAULT 0,\
bft41u_last_buy INT DEFAULT 0,\
bft41u_last_sell INT DEFAULT 0,\
twt93u_buy INT DEFAULT 0,\
twt93u_sell INT DEFAULT 0,\
twt93u_remnant INT DEFAULT 0,\
mi_buy INT DEFAULT 0,\
mi_sell INT DEFAULT 0,\
mi_remnant INT DEFAULT 0,\
ma_buy INT DEFAULT 0,\
ma_sell INT DEFAULT 0,\
ma_remnant INT DEFAULT 0,\
mi_ma INT DEFAULT 0,\
mi_ma_ratio FLOAT(10,4) DEFAULT 0.0000,\
PRIMARY KEY (trade_date))'

#connect to database
conn=MySQLdb.connect(host=serverIP,user=sys.argv[2],passwd=password,db='single_stock',port=3306,use_unicode=True,charset='utf8')
cur=conn.cursor()

#drop table
try:
	cur.execute(drop_table_cmd)
	print drop_table_cmd
except:
	pass

#ceate table
cur.execute(create_table_cmd)
	
#insert data
form_data['CO_ID']=sys.argv[1]
MA_money=[0]*60
MA_stock=[0]*60
last_day_close='0'
year=2015
month=1
endYear = today.year
endMonth = 12

for year in range(2015,endYear+1):
	print sys.argv[1]+":"+str(year)
	if year==endYear:
		endMonth=today.month
	for month in range(1,endMonth+1):
		data_list=[]
		form_data['query_year']=year
		form_data['query_month']=month
		while True:
			try:
				r = requests.post(url,data=form_data)
			except:
				continue
			#TODO how to check if the return page is available
			if r.status_code != requests.codes.ok:
				continue
			break
		soup = BeautifulSoup(r.content,'html.parser')
		table=soup.find_all('table')
		tr=table[0].find_all('tr')
		#break, if no more data
		if len(tr[2].find_all('td')) != 9:
			continue
		for txt in tr[2:len(tr)]:
			td=txt.find_all('td')
			for idx in range(0,59):
				MA_money[idx]=MA_money[idx+1]
				MA_stock[idx]=MA_stock[idx+1]
			date       =td[0].get_text().split('/')
			date[0]=int(date[0])+1911
			trade_date=str(date[0])+'/'+date[1]+'/'+date[2]
			price_high  =td[4].get_text().replace(',', '')
			if float(price_high) == 0:
				#log(sys.argv[1]+':'+trade_date+':price_high is 0')
				price_high = last_day_close
			price_low   =td[5].get_text().replace(',', '')
			if float(price_low) == 0:
				price_low = last_day_close
			price_open  =td[3].get_text().replace(',', '')
			if float(price_open) == 0:
				price_open = last_day_close
			price_close =td[6].get_text().replace(',', '')
			if float(price_close) == 0:
				price_close = last_day_close
			trade_money =td[2].get_text().replace(',', '')
			trade_stock =td[1].get_text().replace(',', '')
			trade_times =td[8].get_text().replace(',', '')
			try:
				price_avg   =float(trade_money)/float(trade_stock)
			except ZeroDivisionError:
				price_avg	=float(last_day_close)
			MA_money[59]=int(trade_money)
			MA_stock[59]=int(trade_stock)
			ma_money = 0
			ma_stock = 0
			ma5=0.00
			ma10=0.00
			ma20=0.00
			ma60=0.00
			'''MA5'''
			for day in range(55,60):
				ma_money+=MA_money[day]
				ma_stock+=MA_stock[day]
			try:
				ma5=ma_money/ma_stock
			except ZeroDivisionError:
				ma5=0.00
			'''MA10'''
			ma_money = 0
			ma_stock = 0
			for day in range(50,60):
				ma_money+=MA_money[day]
				ma_stock+=MA_stock[day]
			try:
				ma10=ma_money/ma_stock
			except ZeroDivisionError:
				ma10=0.00
			'''MA20'''
			ma_money = 0
			ma_stock = 0
			for day in range(40,60):
				ma_money+=MA_money[day]
				ma_stock+=MA_stock[day]
			try :
				ma20=ma_money/ma_stock
			except ZeroDivisionError:
				ma20=0.00
			'''MA60'''
			ma_money = 0
			ma_stock = 0
			for day in range(0,60):
				ma_money+=MA_money[day]
				ma_stock+=MA_stock[day]
			try:
				ma60=ma_money/ma_stock
			except ZeroDivisionError:
				ma60=0.00
				
			'''stock_ratio5'''
			ma_stock=0
			for day in range(54,59):
				ma_stock+=MA_stock[day]
			try:
				quality_ratio5=(5*MA_stock[59])/ma_stock
			except ZeroDivisionError:
				quality_ratio5=2.00
			'''stock_ratio10'''
			for day in range(49,54):
				ma_stock+=MA_stock[day]
			try:
				quality_ratio10=(10*MA_stock[59])/ma_stock
			except ZeroDivisionError:
				quality_ratio10=2.00
			'''raise_ratio'''
			try:
				raise_ratio = (float(price_close)-float(price_open))/float(price_open)
			except:
				raise_ratio =0
			insertData=(trade_date,price_high,price_low,price_open,price_close,trade_money,trade_stock,trade_times,price_avg,ma5,ma10,ma20,ma60,quality_ratio5,quality_ratio10,raise_ratio)
			data_list.append(insertData)
			last_day_close = price_close
			#print insertData
			#cur.execute(insert_cmd,insertData)
		cur.executemany(insert_cmd,data_list)
		conn.commit()
'''release resource'''
cur.close()
conn.close()
