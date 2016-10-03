# -*- coding: utf-8 -*-
"""
Load data into elastic search

@author: Standalone
"""
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Mapping, String, Index
from elasticsearch_dsl.connections import connections
import pandas as pd

#es_domain = 'localhost'
es_domain = 'https://search-dft-hansard-rhlukwzq2jxu4g4ggcy2h47df4.eu-west-1.es.amazonaws.com'
connections.create_connection(hosts=[es_domain], timeout=20)



index_name = 'hansard01'

Index(index_name).create()
#Index(index_name).delete()

#mapping for elastic search
#we want tabled members not to be analysed as free text
#https://elasticsearch-dsl.readthedocs.org/en/latest/persistence.html#mappings
#https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html

# name your type
m = Mapping('Written question')

# you can use multi-fields easily
m.field('tabling member', String(index='not_analyzed'))
# save the mapping into index 'my-index'
m.save(index_name)

#Load the csv 
data = pd.read_csv("mattsData.csv")
                        
#Make the dates a datetime format
data['answer date'] =  pd.to_datetime(data['answer date'],
                         format='%d/%m/%Y')
                         
data['date tabled'] = pd.to_datetime(data['date tabled'],
                         format='%d/%m/%Y')                  
                        
#Create a empty list to store each of the dictionaries
mylist = []
for i in range(len(data)):
    x = {}
    for key in data.columns.values:
        y = data[key][i]
        if key== 'answer date' or key == 'date tabled':
            #convert to datetime
            x[key] = datetime(y.year, y.month, y.day)
        else:
            x[key] = str(y)
    mylist.append(x)
        
#Add the elastic search parameters
for i in mylist:
    i['_index'] = index_name
    i['_type'] = 'Written question'










#load
#es = Elasticsearch({'host': 'localhost'})
es = Elasticsearch(hosts = {'host': es_domain, 'port': 443})
helpers.bulk(es, mylist)