'''
    Author: Alejandro Luperon
    Date: March 27th, 2016
'''

'''
This script takes data from the title table in imdb and loads it into Elasticsearch.
I cast the season_nr and episode_nr field in IMDb and add an "S" or "E" in the beginning 
to make it easier to match. Many people use the alphanumeric convention of "S06 E09" 
which represents season 6 episode 9 of some title Torrent tv
title to classify.
'''
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from subprocess import call
import mysql.connector
import os
import sys

###Establish ES connection
es_connection = Elasticsearch()

local_host = 'localhost';
local_username = os.environ.get('LOCAL_DB_USERNAME')
local_password = os.environ.get('LOCAL_DB_PWORD')
local_database = 'imdb'
local_port = '3306'

local_mysql_connection = mysql.connector.connect(host=local_host,user=local_username,database=local_database,password=local_password,port=local_port)
local_mysql_cursor = local_mysql_connection.cursor(buffered=True)

es_max_id = 1974140
SELECT episode.id, series.title, episode.title, episode.production_year,episode.season_nr,episode.episode_nr FROM title AS episode INNER JOIN title AS series ON series.id = episode.episode_of_id WHERE episode.kind_id IN(2,7) AND episode.id > 0
while es_max_id < 3693956:

	sql = "SELECT episode.id, series.title, episode.title, episode.production_year,episode.season_nr,episode.episode_nr FROM title AS episode INNER JOIN title AS series ON series.id = episode.episode_of_id WHERE episode.kind_id IN(2,7) AND episode.id > " + str(es_max_id) + " LIMIT 10000"
	print sql	
	local_mysql_cursor.execute(sql)
	result_set = local_mysql_cursor.fetchall()

	documents = []

	for result in result_set:

		result = [result[0],result[1],result[2],result[3],result[4],result[5]]

		if result[4] < 10:
			result[4] = "S0" + str(result[4])
		elif result[4] >= 10:
			result[4] = "S" + str(result[4])

		if result[5] < 10:
			result[5] = "E0" + str(result[5])
		elif result[5] >= 10:
			result[5] = "E" + str(result[5])

		source = {
			"id":result[0],
			"series_title":result[1],
			"episode_title":result[2],
			"production_year":str(result[3]),
			"season_number":result[4],
			"episode_number":result[5]
		}

		doc = {
			"_index":"imdb",
			"_type":"tv",
			"_source":source
		}

		documents.append(doc)
	bulk(es_connection,documents)
	doc = {
	  "filter" : {
	    "match_all" : { }
	  },
	  "sort": [
	    {
	      "id": {
	        "order": "desc"
	      }
	    }
	  ],
	  "size": 1
	}

	res = es_connection.search(index="imdb",doc_type="tv",body=doc)
	es_max_id = res['hits']['hits'][0]["sort"][0]
	es_max_id = int(es_max_id)

local_mysql_cursor.close()
local_mysql_connection.close()