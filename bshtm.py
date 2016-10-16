# coding=utf-8
'''http://dba.stackexchange.com/questions/39278/update-column-based-on-the-sort-order-of-another-query
This program is used to download stock point data 
-------------------------------------------
database: 
	single_point
table:
	*
column: 
	idx(UNIQUE)
	trade_date
	trade_idx
	trade_point
	trade_price
	buy_quality
	sell_quality
-------------------------------------------
database: 
	point
table:
	*
column: 
	idx(UNIQUE)
	trade_date
	stock_number
	trade_idx
	trade_price
	buy_quality
	sell_quality
-------------------------------------------
database: 
	single_point_ana
table:
	*
column: 
	idx
	trade_date
	trade_point
	point_buy_quality
	point_sell_quality
	point_total_quality
	point_total_money
	point_buy_money
	point_sell_money
	point_buy_number
	point_sell_number
-------------------------------------------
database: 
	single_stock
table:
	*
column: 
    chip1_price
    chip1_quality
    chip2_price
    chip2_quality
-------------------------------------------
argv[1]: stock number
argv[2]: single point DB account
argv[3]: point DB account
argv[4]: single_point_ana DB account
argv[5]: single_stock DB account
'''

import requests,sys
from bs4 import BeautifulSoup
from getMagicNumber import getMagicNumberFromFile
from getMagicNumber import getMagicNumberFromArray
import MySQLdb
import time, os
from PIL import Image
import numpy

def log(msg):
	log=open('log/bshym.log','a')
	log.writelines(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())+' '+msg)
	log.close()
	
password='44449999'
serverIP='192.168.0.7'
#single_point----------------------------------------------------------------------------------
single_point_insert_cmd='\
INSERT INTO s'+sys.argv[1]+'s \
( idx, trade_date, trade_idx, trade_point, trade_price, buy_quality, sell_quality) \
VALUES (%s,%s,%s,%s,%s,%s,%s) \
ON DUPLICATE KEY UPDATE \
trade_date=%s,\
trade_idx=%s,\
trade_point=%s,\
trade_price=%s,\
buy_quality=%s,\
sell_quality=%s'

check_single_point_table_cmd='SELECT 1 FROM s'+sys.argv[1]+'s LIMIT 1'

create_single_point_table_cmd='\
CREATE TABLE s'+sys.argv[1]+'s (\
idx VARCHAR(16),\
trade_date DATE,\
trade_idx INT,\
trade_point VARCHAR(8),\
trade_price FLOAT(10,2),\
buy_quality INT,\
sell_quality INT,\
PRIMARY KEY (idx))'

#point----------------------------------------------------------------------------------
create_point_table_format='\
CREATE TABLE p%s (\
idx VARCHAR(24) UNIQUE,\
trade_date DATE,\
stock_number VARCHAR(10),\
trade_idx INT,\
trade_price FLOAT(10,2),\
buy_quality INT,\
sell_quality INT,\
PRIMARY KEY (idx))'

point_insert_header='INSERT INTO p%s'
point_insert_body=' \
( idx, trade_date, stock_number, trade_idx, trade_price, buy_quality, sell_quality) \
VALUES (%s,%s,%s,%s,%s,%s,%s) \
ON DUPLICATE KEY UPDATE \
trade_date=%s,\
stock_number=%s,\
trade_idx=%s,\
trade_price=%s,\
buy_quality=%s,\
sell_quality=%s'

#single point ana----------------------------------------------------------------------------------
single_point_ana_insert_cmd='\
INSERT INTO a'+sys.argv[1]+' \
( idx, trade_date, trade_point, point_buy_quality, point_sell_quality, point_total_quality, point_total_money,point_buy_money,point_sell_money) \
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) \
ON DUPLICATE KEY UPDATE \
trade_date=%s,\
trade_point=%s,\
point_buy_quality=%s,\
point_sell_quality=%s,\
point_total_quality=%s,\
point_total_money=%s,\
point_buy_money=%s,\
point_sell_money=%s'

check_ana_table_cmd='SELECT 1 FROM a'+sys.argv[1]+' LIMIT 1'

create_single_point_ana_table_cmd='\
CREATE TABLE a'+sys.argv[1]+' (\
idx VARCHAR(24) UNIQUE,\
trade_date DATE,\
trade_point VARCHAR(8),\
point_buy_quality INT,\
point_sell_quality INT,\
point_total_quality INT,\
point_total_money INT,\
point_buy_money INT,\
point_sell_money INT,\
point_buy_number INT DEFAULT 0,\
point_sell_number INT DEFAULT 0,\
PRIMARY KEY (idx))'
#----------------------------------------------------------------------------------

MAGIC_ERROR=u"驗證碼"
NO_DATA=u"查無資料"
#todo check if connection time out
#header
headers={'Host': 'bsr.twse.com.tw', 'Referer': 'http://bsr.twse.com.tw/bshtm/', 'user-agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36'}
url='http://bsr.twse.com.tw/bshtm/bsMenu.aspx'
while True:
	#get cookie
	while True:
		try:
			r=requests.get(url,headers=headers)
			break
		except:
			time.sleep(2)
			continue
		if r.status_code != requests.codes.ok:
			print "status code error 0"
		time.sleep(0.3)
		continue
	try:
		cookie= r.headers['set-cookie'].split(';')[0]
	except:
		#log(sys.argv[1]+": can't find set-cookie\n")
		continue
	#get image
	soup = BeautifulSoup(r.content,'html.parser')
	try:
		src=soup.find_all('img')[1]['src']
	except:
		log(sys.argv[1]+": can't find img src\n")
		continue
	try:
		VIEWSTATE=soup.find_all(attrs={"id" : "__VIEWSTATE"})[0]['value']
	except:
		log(sys.argv[1]+": can't find VIEWSTATE\n")
		continue
	try:
		EVENTVALIDATION=soup.find_all(attrs={"id" : "__EVENTVALIDATION"})[0]['value']
	except:
		log(sys.argv[1]+": can't find VIEWSTATE\n")
		continue
	#set request header with cookie
	rHeader={'Host': 'bsr.twse.com.tw', 'Referer': 'http://bsr.twse.com.tw/bshtm/', 'user-agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36','Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8','Cookie':cookie}
	#save image
	try:
		imagefile=requests.get("http://bsr.twse.com.tw/bshtm/"+src,headers=rHeader,stream=True)
	except:
		time.sleep(1)
		continue
	try:
		f=open(sys.argv[1]+'.jpeg','wb')
	except:
		log(sys.argv[1]+": can't open number.jpeg\n")
		continue
	f.write(imagefile.content)
	f.close()
	#identify code
	magicNumber=getMagicNumberFromFile(sys.argv[1]+'.jpeg')
	
	if magicNumber == -1:
		#log(sys.argv[1]+": magicNumber -1\n")
		continue
	
	'''try:
		im=Image.open(StringIO(r.content)).convert('L')
	except:
		continue
	magicNumber=getMagicNumberFromImageObject(im)
	'''
	#set request header with cookie
	rHeader={'Host': 'bsr.twse.com.tw', 'Referer': 'http://bsr.twse.com.tw/bshtm/', 'user-agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36','Content-Type':'application/x-www-form-urlencoded','Origin':'http://bsr.twse.com.tw','Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8','Cookie':cookie}
	#set form data
	formData={'__EVENTTARGET':'','__EVENTARGUMENT':'','__LASTFOCUS':'','__VIEWSTATE':VIEWSTATE,'__EVENTVALIDATION':EVENTVALIDATION,'RadioButton_Normal':'RadioButton_Normal','TextBox_Stkno':'','CaptchaControl1':'','btnOK':'查詢'.decode('utf8')}

	'''try:
		im = Image.open('number.jpeg').convert('L')
	except:
		continue
	(width, height) = im.size
	im = Image.open('number.jpeg').convert('L')	
	while True:
		#magicNumber=getMagicNumberFromFile('number.jpeg')
		#im = Image.open('number.jpeg').convert('L')

		#dump to array
		imgArray = list(im.getdata())
		imgArray = numpy.array(imgArray)
		imgArray = imgArray.reshape((height, width))
		magicNumber=getMagicNumberFromArray(imgArray,width,height)
		if magicNumber == -1:
			log(sys.argv[1]+": magicNumber -1\n")
			try:
				f.close()
			except:
				pass
			time.sleep(1)
			continue
		else:
			break
	'''
	#fill magic number
	formData['CaptchaControl1']=magicNumber
	formData['TextBox_Stkno']=sys.argv[1]
	try:
		rr=requests.post(url,headers=rHeader,data=formData)
	except:
		time.sleep(0.3)
		continue
	if rr.status_code != requests.codes.ok:
		#print "status code error 1"
		time.sleep(0.3)
		continue
	try:
		ErrorMsg=BeautifulSoup(rr.content,'html.parser').find_all('span',attrs={"id" : "Label_ErrorMsg"})[0].get_text()
	except:
		continue
	if MAGIC_ERROR in ErrorMsg:
		#print "Magic error"
		time.sleep(0.3)
		continue
	if NO_DATA in ErrorMsg:
		#print "NODATA"
		log(sys.argv[1]+": no single point data\n")
		exit(2)
	rrHeader={'Host': 'bsr.twse.com.tw', 'Referer': 'http://bsr.twse.com.tw/bshtm/bsMenu.aspx', 'user-agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36','Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8','Cookie':cookie}
	#get final data 
	data={'v':'t'}
	try:
		rr=requests.get('http://bsr.twse.com.tw/bshtm/bsContent.aspx?',headers=rrHeader,data=data)
	except:
		time.sleep(0.3)
		continue
	if rr.status_code != requests.codes.ok:
		#print "status code error 2"
		time.sleep(0.3)
		continue
	if int(rr.headers['Content-Length'])<50:
		#print "Content-Length<50"
		#log(sys.argv[1]+": Content-Length<50\n")
		time.sleep(0.3)
		continue
	else:
		break
	
	'''if len(table)<3:
		print 'len(table)<3'
		time.sleep(1)
		continue'''

os.remove(sys.argv[1]+'.jpeg')
#parse content
soup = BeautifulSoup(rr.content,'html.parser')
table=soup.find_all('table',attrs={"border" : "1","cellpadding":"2","cellspacing":"0"})
if len(table)<3:		
	f=open('log/'+sys.argv[1]+'s2.html','w')
	f.write(rr.text.encode('utf8'))
	f.close()
	exit(5)

#data we need, store in table[1],table[2],table[4],table[5],table[7],table[8],.......
totalPage=int(len(table)/3)	

#todo add check connect or not
#connect to single_point database
single_point_conn=MySQLdb.connect(host=serverIP,user=sys.argv[2],passwd=password,db='single_point',port=3306,use_unicode=True,charset='utf8')
single_point_cur=single_point_conn.cursor()

#connect to point database
point_conn=MySQLdb.connect(host=serverIP,user=sys.argv[3],passwd=password,db='point',port=3306,use_unicode=True,charset='utf8')
point_cur=point_conn.cursor()

#connect to single point ana database
single_point_ana_conn=MySQLdb.connect(host=serverIP,user=sys.argv[4],passwd=password,db='single_point_ana',port=3306,use_unicode=True,charset='utf8')
single_point_ana_cur=single_point_ana_conn.cursor()

#check if single point table exist or not, create if not exist
try:
	single_point_cur.execute(check_single_point_table_cmd)
except:
	single_point_cur.execute(create_single_point_table_cmd)

#check if single point ana table exist or not, create if not exist
try:
	single_point_ana_cur.execute(check_ana_table_cmd)
except:
	single_point_ana_cur.execute(create_single_point_ana_table_cmd)

#get date from table[0]
date = table[0].find_all('tr')[0].find_all('td')[1].get_text().split('/')
year=date[0][-4]+date[0][-3]+date[0][-2]+date[0][-1]
idxHead=year+date[1]+date[2]
trade_date=year+'/'+date[1]+'/'+date[2]
last_point='zzzz'
point_cnt=0
point_buy=0
point_sell=0
point_total_quality=0
point_total_money=0
point_buy_quality=0
point_sell_quality=0
point_buy_money=0
point_sell_money=0
for page in range(0,totalPage):
	leftPage=table[3*page+1].find_all('tr')
	rightPage=table[3*page+2].find_all('tr')
	data_list=[]
	data_list2=[]
	idx=0
	for row in range(1,len(leftPage)):
		#handle left page
		data = leftPage[row].find_all('td')
		trade_idx = int(data[0].get_text())
		point = data[1].get_text().replace(' ','')
		trade_point = point[2]+point[3]+point[4]+point[5]
		trade_price = float(data[2].get_text())
		buy_quality = int(data[3].get_text().replace(',', ''))
		sell_quality = int(data[4].get_text().replace(',', ''))
		idx="%s-%05d"%(trade_date,trade_idx)
		insertData=(idx,trade_date,trade_idx,trade_point,trade_price,buy_quality,sell_quality,trade_date,trade_idx,trade_point,trade_price,buy_quality,sell_quality)
		data_list.append(insertData)
		#calculate for point & single_point_ana
		if cmp(last_point,trade_point) == 0:
			point_cnt += 1
		else:
			if last_point != 'zzzz':
				idx="%s-%s"%(trade_date,last_point)
				point_total_quality = point_buy_quality-point_sell_quality
				point_total_money = point_buy_money-point_sell_money
				insertData=(idx, trade_date, last_point, point_buy_quality, point_sell_quality, point_total_quality, point_total_money,point_buy_money,point_sell_money,trade_date, last_point, point_buy_quality, point_sell_quality, point_total_quality, point_total_money,point_buy_money,point_sell_money)
				data_list2.append(insertData)
			#single_point_ana_insert_cmd = single_point_ana_insert_header%(trade_point)+single_point_ana_insert_body
			last_point = trade_point
			point_cnt=0
			point_buy_quality=0
			point_sell_quality=0
			point_buy_money=0
			point_sell_money=0
		point_buy_quality +=buy_quality
		point_sell_quality+=sell_quality
		point_buy_money+=buy_quality*trade_price
		point_sell_money+=sell_quality*trade_price
		idx = "%s-%s-%04d"%(trade_date,sys.argv[1],point_cnt)
		#insert data to point
		point_insert_cmd = point_insert_header%(trade_point)+point_insert_body
		
		try:
			point_cur.execute(point_insert_cmd,(idx,trade_date,sys.argv[1],trade_idx,trade_price,buy_quality,sell_quality,trade_date,sys.argv[1],trade_idx,trade_price,buy_quality,sell_quality))
		except MySQLdb.Error, e:
			if int(e.args[0]) == 1146:
				print "create table: p"+trade_point
				point_cur.execute(create_point_table_format%(trade_point))
				point_cur.execute(point_insert_cmd,(idx,trade_date,sys.argv[1],trade_idx,trade_price,buy_quality,sell_quality,trade_date,sys.argv[1],trade_idx,trade_price,buy_quality,sell_quality))
			else:
				print "mysql error"
				exit()
		point_conn.commit()
		#handle right page
		data = rightPage[row].find_all('td')
		try:
			trade_idx = int(data[0].get_text())
		except ValueError:
			continue
		point = data[1].get_text().replace(' ','')
		trade_point = point[2]+point[3]+point[4]+point[5]
		trade_price = float(data[2].get_text())
		buy_quality =  int(data[3].get_text().replace(',', ''))
		sell_quality =  int(data[4].get_text().replace(',', ''))
		idx="%s-%05d"%(trade_date,trade_idx)
		insertData=(idx,trade_date,trade_idx,trade_point,trade_price,buy_quality,sell_quality,trade_date,trade_idx,trade_point,trade_price,buy_quality,sell_quality)
		data_list.append(insertData)
		#calculate for point & single_point_ana
		if cmp(last_point,trade_point) == 0:
			point_cnt += 1
		else:
			if last_point != 'zzzz':
				idx="%s-%s"%(trade_date,last_point)
				point_total_quality = point_buy_quality-point_sell_quality
				point_total_money = point_buy_money-point_sell_money
				insertData=(idx, trade_date, last_point, point_buy_quality, point_sell_quality, point_total_quality, point_total_money,point_buy_money,point_sell_money,trade_date, last_point, point_buy_quality, point_sell_quality, point_total_quality, point_total_money,point_buy_money,point_sell_money)
				data_list2.append(insertData)
			#single_point_ana_insert_cmd = single_point_ana_insert_header%(trade_point)+single_point_ana_insert_body
			last_point = trade_point
			point_cnt=0
			point_buy_quality=0
			point_sell_quality=0
			point_buy_money=0
			point_sell_money=0
		point_buy_quality +=buy_quality
		point_sell_quality+=sell_quality
		point_buy_money+=buy_quality*trade_price
		point_sell_money+=sell_quality*trade_price
		idx = "%s-%s-%04d"%(trade_date,sys.argv[1],point_cnt)
		#insert data to point
		point_insert_cmd = point_insert_header%(trade_point)+point_insert_body
		'''point_insert_cmd = point_insert_format %(trade_point,trade_date,sys.argv[1],idx,trade_idx,trade_price,buy_quality,sell_quality,trade_date,sys.argv[1],trade_idx,trade_price,buy_quality,sell_quality)'''
		
		try:
			point_cur.execute(point_insert_cmd,(idx,trade_date,sys.argv[1],trade_idx,trade_price,buy_quality,sell_quality,trade_date,sys.argv[1],trade_idx,trade_price,buy_quality,sell_quality))
		except MySQLdb.Error, e:
			if int(e.args[0]) == 1146:
				print "create table: p"+trade_point
				point_cur.execute(create_point_table_format%(trade_point))
				point_cur.execute(point_insert_cmd,(idx,trade_date,sys.argv[1],trade_idx,trade_price,buy_quality,sell_quality,trade_date,sys.argv[1],trade_idx,trade_price,buy_quality,sell_quality))
			else:
				print "mysql error"
				exit()
		point_conn.commit()
	single_point_cur.executemany(single_point_insert_cmd,data_list)
	single_point_conn.commit()
	single_point_ana_cur.executemany(single_point_ana_insert_cmd,data_list2)
	single_point_ana_conn.commit()

#insert last data in single_point_ana
idx="%s-%s"%(trade_date,trade_point)
point_total_quality = point_buy_quality-point_sell_quality
point_total_money = point_buy_money-point_sell_money
single_point_ana_cur.execute(single_point_ana_insert_cmd,(idx, trade_date, trade_point, point_buy_quality, point_sell_quality, point_total_quality, point_total_money,point_buy_money,point_sell_money,trade_date, trade_point, point_buy_quality, point_sell_quality, point_total_quality, point_total_money,point_buy_money,point_sell_money))
single_point_ana_conn.commit()


#sort single_point_ana
single_point_ana_sort_buy_cmd = '\
UPDATE a'+sys.argv[1]+' SET point_buy_number=(@x:=@x+1) WHERE trade_date=%s ORDER BY point_buy_quality DESC'
single_point_ana_sort_sell_cmd = '\
UPDATE a'+sys.argv[1]+' SET point_sell_number=(@x:=@x+1) WHERE trade_date=%s ORDER BY point_sell_quality DESC'

#sort buyer
single_point_ana_cur.execute("SET @x = 0")
single_point_ana_cur.execute(single_point_ana_sort_buy_cmd,(trade_date))
single_point_ana_conn.commit()
#sort seller
single_point_ana_cur.execute("SET @x = 0")
single_point_ana_cur.execute(single_point_ana_sort_sell_cmd,(trade_date))
single_point_ana_conn.commit()

'''
# calculate CHIP
#connect to single_stock database
single_stock_conn=MySQLdb.connect(host=serverIP,user=sys.argv[5],passwd=password,db='single_stock',port=3306,use_unicode=True,charset='utf8')
single_stock_cur=single_stock_conn.cursor()

select_chip_cmd='\
SELECT trade_price,SUM(buy_quality) AS quality from s'+sys.argv[1]+'s WHERE trade_date=%s GROUP BY trade_price ORDER BY quality DESC LIMIT 2'
update_chip_cmd='\
UPDATE s'+sys.argv[1]+' SET chip1_price=%s,chip1_quality=%s,chip2_price=%s,chip2_quality=%s WHERE trade_date=%s'
single_point_cur.execute(select_chip_cmd,(trade_date))
count= 0
data=[0]*4
for (trade_price,quality) in single_point_cur:
	data[count]=trade_price
	count+=1
	data[count]=quality
	count+=1
print update_chip_cmd%(data[0],data[1],data[2],data[3],trade_date)
'''


'''release resource'''
single_point_conn.close()
single_point_cur.close()
point_conn.close()
point_cur.close()
single_point_ana_conn.close()
single_point_ana_cur.close()
#single_stock_conn.close()
#single_stock_cur.close()
