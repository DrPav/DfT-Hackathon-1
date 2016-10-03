# -*- coding: utf-8 -*-
"""
Created on Sat Apr 23 01:40:47 2016

@author: David
"""

from os import listdir
from hansard_es_functions import upload_QandA_xml, QandA #, Connect
from elasticsearch_dsl import Index

#List all files in directory
#http://stackoverflow.com/questions/3207219/how-to-list-all-files-of-a-directory-in-python
folder = 'Download\\all 2016-04-20'
files = listdir(folder)
            
#First file is desktop.ini, we don't want that
files.pop(0)

#Connect to elastic search
#Connect() #Use for aws

#localhost only
from elasticsearch_dsl.connections import connections
connections.create_connection(hosts=['localhost'], timeout=20)



#Choose a name for the index to upload to
index_name = 'hansard'

#Index(index_name).delete(ignore=404) #Only use if you want to delete it


class mappings(QandA):
    class Meta:
        index = index_name

mappings.init()



documents_uploaded = 0
for file in files:
    filepath = folder+ '\\' + file
    upload_QandA_xml(filepath, index_name)
    documents_uploaded += 100
    print(documents_uploaded)
    
    