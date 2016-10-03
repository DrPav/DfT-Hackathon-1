# -*- coding: utf-8 -*-
"""
Script to put parlimentary Q&A's from data.parliament.uk into Elastic Search

Run this script once the xml files have been downloaded



@author: David Pavitt
DFT Hack CLub
"""
from os import listdir
from bs4 import BeautifulSoup
from datetime import datetime
from urllib import request
from time import sleep
from elasticsearch_dsl import DocType, String, Date, Boolean, Index
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Index

#==============================================================================

#PARAMETERS
#Elastic search location. Currently the local machine but could be a amazon or microsfot instance in the future
es_domain = 'localhost'

# 
index_name = 'hansard'



#==============================================================================




#FUNCTION TO CONVERT RELEVANT XML TO PYTHON DICTIONARY
#
#The data is stored between the <item> tages in <items>
#Looking at the tags in Item I have converted the relevant columns
#Turn xml into a dictionairy that can be imported to elasticsearch
def xml_item_to_dict(item):
    mydict = {}
    #Need to use all these try statments beacuse we must skip if the variable
    #doesn't exist, which is true sometimes in this messy data set
    #There is a probably a better way to do this
    try:
        mydict["item url"] = item.attrs['href']
    except AttributeError:
        mydict["item url"] = None
    try:
        mydict["answering body"] =  item.AnsweringBody.item.string
    except AttributeError:
        mydict["answering body"] = None
    try:
        mydict["answer"] =  item.answerText.string
    except AttributeError:
        mydict["answer"] = None
    try:
        mydict["answering members constituency"] =  item.answeringMemberConstituency.string
    except AttributeError:
        mydict["answering members constituency"] = None
    try:
        mydict["answering member"] =  item.answeringMemberPrinted.string
    except AttributeError:
        mydict["answering member"] = None
    try:
        mydict["answer date"] =  item.dateOfAnswer.string #convert to datetime later
    except AttributeError:
        mydict["answer date"] = None
    try:
        mydict["is it a ministerial correction"] =  item.isMinisterialCorrection.string
    except AttributeError:
        mydict["is it a ministerial correction"] = None
    try:
        mydict["first answer date"] =  item.questionFirstAnswered.item.string
    except AttributeError:
        mydict["first answer date"] = None
    try:
        mydict["answering department"] =  item.answeringDeptShortName.string
    except AttributeError:
        mydict["answering department"] = None
    try:
        mydict["asked date"] =  item.date.string
    except AttributeError:
        mydict["asked date"] = None
    try:
        mydict["house"] =  item.legislature.prefLabel.string
    except AttributeError:
        mydict["house"] = None
    try:
        mydict["question"] =  item.questionText.string
    except AttributeError:
        mydict["question"] = None
    try:
        mydict["tabling member"] =  item.tablingMemberPrinted.item.string
    except AttributeError:
        mydict["tabling member"] = None
    try:
        mydict["tabling member constituency"] =  item.tablingMemberConstituency.string
    except AttributeError:
        mydict["tabling member constituency"] = None
    try:
        mydict["uin"] =  item.uin.string
    except AttributeError:
        mydict["uin"] = None
	

    #Fix the datatypes of 'true' and 'false' to be python boolean
    if mydict['is it a ministerial correction'] == 'true':
        mydict['is it a ministerial correction'] = True
    else:
        mydict['is it a ministerial correction'] = False
    
    #Convert to DateTime
    #Added error handling for NoneType
    def to_datetime(string):
        if string != None:
            year = int(string[0:4])
            month = int(string[5:7])
            day = int(string[8:10])
            return datetime(year, month, day)
        else:
            return None
    mydict['answer date'] = to_datetime(mydict['answer date'])
    mydict["first answer date"] = to_datetime(mydict["first answer date"])
    mydict["asked date"] = to_datetime(mydict["asked date"])
    
    return mydict

#==============================================================================
#SET UP THE DATA TO GO INTO ELASTIC SEARCH USING DocType
#https://elasticsearch-dsl.readthedocs.org/en/latest/persistence.html#doctype


class QandA(DocType):
    answer = String()
    answering_member = String(index = 'not_analyzed')
    answer_date = Date()
    ministerial_correction = Boolean()
    first_answer_date = Date()
    asked_date = Date()
    house = String(index='not_analyzed')
    question = String()
    tabling_member = String(index='not_analyzed')
    
    
    # you can use multi-fields easily
    #This creates a extra field that won't be analysed. Useful when aggregating on
    #terms. i.e. Doing a histogram on the analysed categoriy will give:
    #"ministry", "defence", "justice" , "of"...
    #where raw will give
    #"ministry of justice", "minstry of defence"
    answering_body = String(
        fields={'raw': String(index='not_analyzed')})
    answering_members_constituency = String(
        fields={'raw': String(
            index='not_analyzed')}) 
    answering_department = String(
        fields={'raw': String(index='not_analyzed')})
    tabling_members_constituency = String(
        fields={'raw': String(
            index='not_analyzed')}) 

    class Meta:
        index = index_name


    def save(self, ** kwargs):
        self.created_at = datetime.now()
        return super().save(** kwargs)
#==============================================================================

#SETUP ELASTIC SEARCH SERVER
#Connect to elastic search -insecure, no authentication
connections.create_connection(hosts=[es_domain], timeout=20)



  
#delete the index, ignore if it doesn't exist
#WARNING THIS DELETES THE INDEX - useful if you want to delete the curernt one and start again
Index(index_name).delete(ignore=404)

#If its the first document then you need to set up a new index and define the mappings
#Otherwise comment out this line
#Create the index and mappings - initiliase the above class
QandA.init()


#==============================================================================        
#Function to load xml file to elastic search
def uploadXML(xml_file):
    #Use Beautiful sout to read the xml file
    #https://www.crummy.com/software/BeautifulSoup/bs4/doc/#
    soup = BeautifulSoup(xml_file, 'xml', from_encoding="utf-8")
    #Use beautiful soup and next siblings to iterate throug the <item> tabs
    #There is an issue where using next sibling returns the string '\n' for every
    #other sibling, e.e. the records are separeted by new lines. 
    #Also the next_siblings iterator misses teh first item so thats added outside
    #the loop 
    #Start with the first item and add to a list
    first_item = soup.result.items.item
    my_items = [xml_item_to_dict(first_item)]
    #Loop over the rest of the items and put in the list
    #(A list of dictionaries)
    for i in first_item.next_siblings:
        if i != '\n': #ignore the blank new line rows
            my_items.append(xml_item_to_dict(i))


    #Now loop over this list and load each to elastic search
    for item in my_items:
        doc = QandA(
            answer = item['answer'],
            answering_member = item['answering member'],
            answer_date =  item['answer date'],
            ministerial_correction = item['is it a ministerial correction'],
            first_answer_date = item['first answer date'],
            asked_date = item['asked date'],
            house = item['house'],
            question = item['question'],
            tabling_member = item['tabling member'],
            answering_members_constituency = item['answering members constituency'],
            answering_department = item['answering department'],
            tabling_members_constituency = item['tabling member constituency']
            )
        #Put the unique identifier as the uid + url 
        doc.meta.id = item['uin'] + '-' + item['item url']
        #UPLOAD TO ELASTICSEARCH
        doc.save()
        
    #Close the xml_file 
    xml_file.close()
        
#======================================
#Loop over all files in a directory

#List all files in directory
#http://stackoverflow.com/questions/3207219/how-to-list-all-files-of-a-directory-in-python
folder = 'Download\\all 2016-04-20'
files = listdir(folder)
            
#First file is desktop.ini, we don't want that
files.pop(0)


documents_uploaded = 0
for file in files:
    filepath = folder+ '\\' + file
    f = open(filepath, 'rb')
    uploadXML(f)
    documents_uploaded += 100
    print(documents_uploaded)
    

#At the hack club presentation I used Kibana to view and query the data in the elastic search database
#Point kibana at the elastic search location - 'localhost' if on your machine 
#In settings you need to set the index, here its 'hansard'  
#Kibana
#https://search-dft-hansard-rhlukwzq2jxu4g4ggcy2h47df4.eu-west-1.es.amazonaws.com/_plugin/kibana/
    
    

    


