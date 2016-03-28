'''
        Author: Alejandro Luperon
        Date: March 27th, 2016
'''
'''
This script maps the torrent scrape data to IMDb data.

CREATE TABLE tv_mappings(
        torrent_id INT,
    torrent_title VARCHAR(1000),
    imdb_show_name VARCHAR(1000),
    imdb_episode_name VARCHAR(1000),
    imdb_episode_number VARCHAR(10),
    imdb_season_number VARCHAR(10),
    imdb_show_year VARCHAR(10),
    imdb_title_id INT
);

CREATE TABLE tv_mappings_failed(
        torrent_id INT,
    torrent_title VARCHAR(1000),
    imdb_show_name VARCHAR(1000),
    imdb_episode_name VARCHAR(1000),
    imdb_episode_number VARCHAR(10),
    imdb_season_number VARCHAR(10),
    imdb_show_year VARCHAR(10),
    imdb_title_id INT
);
'''
from elasticsearch import Elasticsearch
from subprocess import call
import json
import requests
import mysql.connector
import math
import os
import re

es_connection = Elasticsearch()

#####Torrent Library
tl_local_host = 'localhost'
tl_local_username = os.environ.get('LOCAL_DB_USERNAME')
tl_local_password = os.environ.get('LOCAL_DB_PWORD')
tl_local_database = 'scraper'
tl_local_port = '3306'

tl_mysql_connection = mysql.connector.connect(host=tl_local_host,user=tl_local_username,database=tl_local_database,password=tl_local_password,port=tl_local_port)
tl_mysql_cursor = tl_mysql_connection.cursor(buffered=True)

#####Mappings
m_host = 'localhost';
m_username = os.environ.get('LOCAL_DB_USERNAME')
m_password = os.environ.get('LOCAL_DB_PWORD')
m_database = 'mappings'
m_port = '3306'

m_mysql_connection = mysql.connector.connect(host=m_host,user=m_username,database=m_database,password=m_password,port=m_port)
m_mysql_cursor = m_mysql_connection.cursor(buffered=True)

sql = "SELECT title,id FROM scrape_data WHERE category = 'TV'"
tl_mysql_cursor.execute(sql)
result_set = tl_mysql_cursor.fetchall()

for result in result_set:
        torrent_id = result[1]
        torrent_title = result[0].encode('ascii', 'ignore')

        es_searching_torrent_title = torrent_title

        '''
        The following two lines of code standardize the torrent title. A torrent tv title often is presented as the two-most common formats: 
        Game of Thrones S05E08 HDTV x264-KILLERS[ettv] OR Game of Thrones Season 5 Episode 8 HDTV x264-KILLERS[ettv]
        I would process strings like "S05E08" and split it to "S05 E08" to increase likelyhood of match.
        '''
        split_me = re.search(r'[S^0-9A-Z]{6}',torrent_title)

        if split_me != None:
                if split_me.group(0).upper() != 'SEASON':
                        im_split = split_me.group(0)[:3] + ' ' + split_me.group(0)[3:]
                        es_searching_torrent_title = re.sub(r'[S^0-9A-Z]{6}',im_split, es_searching_torrent_title)

        all_integers = re.findall(r'\b\d\b',es_searching_torrent_title)
        if len(all_integers) > 0:
                for i in all_integers:
                        regex = r'\b' + i +r'\b'              
                        es_searching_torrent_title = re.sub(regex,'0' + i, es_searching_torrent_title)


        es_searching_torrent_title = re.sub(r'\b[s|S]eason \b','S',es_searching_torrent_title)
        es_searching_torrent_title = re.sub(r'\b[e|E]pisode \b','E',es_searching_torrent_title)
        torrent_title_unfiltered = torrent_title

        #replace html-encoded ampersand
        torrent_title = str.replace(torrent_title,'&amp;','and')

        #replace html-encoded apostrophe
        torrent_title = str.replace(torrent_title,'&#39;','and')

        #replace ampersand
        torrent_title = str.replace(torrent_title,'&','and')

        #remove all non alphanumeric characters
        torrent_title = re.sub(r'[^a-zA-Z0-9]','', torrent_title)

        if ("SEASON" in torrent_title.upper() and "COMPLETE" in torrent_title.upper()) or ("SERIES" in torrent_title.upper() and "COMPLETE" in torrent_title.upper()) :
                insert_sql = ("INSERT INTO tv_mappings_failed(torrent_id,torrent_title) VALUES(%s,%s)")
                insert_data = (torrent_id,torrent_title_unfiltered)
                m_mysql_cursor.execute(insert_sql,insert_data)
                m_mysql_connection.commit()
                
        else:        
                res_television = es_connection.search(index="imdb", doc_type="tv", body={"query": {"multi_match": {"query": str.replace(es_searching_torrent_title,'.',' '),"type":"cross_fields","fields":["series_title","episode_number","season_number"]}}})

                if (len(res_television['hits']['hits']) != 0):

                        res_imdb_show_name = res_television['hits']['hits'][0]["_source"]["series_title"].encode('ascii', 'ignore')
                        res_imdb_show_name_unfiltered = res_imdb_show_name

                        #replace html-encoded ampersand
                        res_imdb_show_name = str.replace(res_imdb_show_name,'&amp;','and')

                        #replace html-encoded apostrophe
                        res_imdb_show_name = str.replace(res_imdb_show_name,'&#39;','and')

                        #replace ampersand
                        res_imdb_show_name = str.replace(res_imdb_show_name,'&','and')

                        #remove all non alphanumeric characters
                        res_imdb_show_name = re.sub(r'[^a-zA-Z0-9]','', res_imdb_show_name)

                        res_imdb_episode_number = res_television['hits']['hits'][0]["_source"]["episode_number"]
                        res_imdb_season_number = res_television['hits']['hits'][0]["_source"]["season_number"]
                        res_imdb_show_year = res_television['hits']['hits'][0]["_source"]["production_year"]
                        res_imdb_title_id = res_television['hits']['hits'][0]["_source"]["id"]
                        res_imdb_episode_name = res_television['hits']['hits'][0]["_source"]["episode_title"]
                        
                        #If statement validates the guess from elasticsearch to see
                        if res_imdb_episode_number != None and res_imdb_season_number != None and (res_imdb_show_name.upper() == torrent_title[:len(res_imdb_show_name)].upper()) and (res_imdb_episode_number.upper() in torrent_title.upper()) and (res_imdb_season_number.upper() in torrent_title.upper()):

                                insert_sql = ("INSERT INTO tv_mappings(torrent_id,torrent_title,imdb_show_name,imdb_episode_name,imdb_episode_number,imdb_season_number,imdb_show_year,imdb_title_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)")
                                insert_data = (torrent_id,torrent_title_unfiltered,res_imdb_show_name_unfiltered,res_imdb_episode_name,res_imdb_episode_number,res_imdb_season_number,res_imdb_show_year,res_imdb_title_id)
                                m_mysql_cursor.execute(insert_sql,insert_data)
                                m_mysql_connection.commit()
                                
                        else:
                                insert_sql = ("INSERT INTO tv_mappings_failed(torrent_id,torrent_title,imdb_show_name,imdb_episode_name,imdb_episode_number,imdb_season_number,imdb_show_year,imdb_title_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)")
                                insert_data = (torrent_id,torrent_title_unfiltered,res_imdb_show_name_unfiltered,res_imdb_episode_name,res_imdb_episode_number,res_imdb_season_number,res_imdb_show_year,res_imdb_title_id)
                                m_mysql_cursor.execute(insert_sql,insert_data)
                                m_mysql_connection.commit()
                                       
                   
                else:
                        insert_sql = ("INSERT INTO tv_mappings_failed(torrent_id,torrent_title) VALUES(%s,%s)")
                        insert_data = (torrent_id,torrent_title_unfiltered)
                        m_mysql_cursor.execute(insert_sql,insert_data)
                        m_mysql_connection.commit()
                                
                        
tl_mysql_cursor.close()
tl_mysql_connection.close()

m_mysql_cursor.close()
m_mysql_connection.close()


