import csv
from elasticsearch import Elasticsearch
from csvops import extractHeader
from facebook_pull import getPageDetails
from facebook_pull import indexPosts
import facebook
import os
import sys

print("Welcome to The Oracle by DigitalJedi")
fb_token = input("Enter Facebook Access Token: ")
graph = facebook.GraphAPI(access_token=fb_token, version='2.3')
es = Elasticsearch()

with open('company_master.csv', 'r') as in_file:
	
	out_file = open('company_master_out.csv', 'wt')

	reader = csv.reader(in_file)
	writer = csv.writer(out_file)
	colnums = extractHeader(reader,writer)
	header_check = 0
	

	for row in reader:

		brand = (row[colnums['brand_colnum']]).lower()
		model = (row[colnums['model_colnum']]).lower()
		location = (row[colnums['location_colnum']]).lower()
		facebook_handle = row[colnums['facebook_colnum']]
		twitter_handle = row[colnums['twitter_colnum']]
		instagram_handle = row[colnums['instagram_colnum']]
		fblastpostid = row[colnums['fblastpostid_colnum']]

		general_info = {
		'brand':brand,
		'model':model,
		'location':location,
		'facebook_handle':facebook_handle,
		'instagram_handle':instagram_handle,
		'twitter_handle':twitter_handle,
		'fblastpostid':fblastpostid
		}

		print (brand.upper())
		es.index(index = 'info', doc_type = brand, id = 'general_info', body = general_info)
		print("Indexed General Info")
		fb_info = getPageDetails(facebook_handle,graph)
		es.index(index = 'info', doc_type = brand, id = 'fb_info', body = fb_info)
		print("Indexed Page Info")
		print("Indexing Posts....")
		fblastpostid = indexPosts(facebook_handle,graph,fblastpostid,es,brand)
		row[colnums['fblastpostid_colnum']] = fblastpostid

		writer.writerow(row)

	out_file.close()
		
os.remove('company_master.csv')
os.rename('company_master_out.csv','company_master.csv')

	

		


