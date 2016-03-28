'''
    Author: Alejandro Luperon
    Date: March 27th, 2016
'''
'''
This script gathered data from kat.cr, a website that provides .torrent files for people to 
feed to their bittorrent clients so that they can obtain files. The data scraped was title, 
info_hash, and magnet. Both the info hash and magnet can be used to download files.
'''
'''
CREATE TABLE scrape_data(
	title VARCHAR(8000),
	info_hash VARCHAR(40),
	magnet VARCHAR(500),
	scrape_time DATETIME,
	source INT
)

'''
import os
import time
import httplib2
from html import HTML
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
import re
import mysql.connector

local_host = 'localhost';
local_username = os.environ.get('LOCAL_DB_USERNAME')
local_password = os.environ.get('LOCAL_DB_PWORD')
local_database = 'scraper'
local_port = '3306'

local_mysql_connection = mysql.connector.connect(host=local_host,user=local_username,database=local_database,password=local_password,port=local_port)
local_mysql_cursor = local_mysql_connection.cursor(buffered=True)

http = httplib2.Http(".cache", disable_ssl_certificate_validation=True)
urlHead = 'https://kat.cr/tv/'

for page in range(1,401):
	scrape_time = datetime.now().date()	
	url = urlHead + str(page) + '/'

	try:
		status, response = http.request(url)
		soup = BeautifulSoup(response,"html.parser")
		result = []
		for tag in soup.findAll(True,{'id':True}):

			if 'torrent_tv_torrents' == tag['id'][:len('torrent_tv_torrents')]:
				tr_torrent_tv_torrents = soup.find(id=tag['id'])
				torrent_name_grandparent = tr_torrent_tv_torrents.find_all('div', attrs={'class':'torrentname'})
				torrent_name_parent = torrent_name_grandparent[0].find_all('div')
				torrent_name_container = torrent_name_parent[0].find_all('a', attrs={'class':'cellMainLink'})
				torrent_name = torrent_name_container[0].contents
				torrent_name = torrent_name[0].encode('utf-8')

				magnet_parent = tr_torrent_tv_torrents.find_all('a', attrs={'class':'icon16'})
				magnet_container = magnet_parent[1]
				magnet =  magnet_container['href']
				info_hash_search = re.search(r'\burn:btih:([a-zA-Z0-9]{32,40})\b',magnet)
				info_hash = info_hash_search.group(0)[9:]

				insert_sql = ("INSERT INTO scrape_data(title,info_hash,magnet,scrape_time,source) VALUES(%s,%s,%s,%s,%s)")
				insert_data = (torrent_name,info_hash,magnet,scrape_time,1)
				local_mysql_cursor.execute(insert_sql,insert_data)
				local_mysql_connection.commit()
	except:
		print 'URL failed'

	time.sleep(1)