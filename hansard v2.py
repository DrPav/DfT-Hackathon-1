# -*- coding: utf-8 -*-
"""
Script to put parlimentary Q&A's from data.parliament.uk into Elastic Search
Parliamentary Questions Answered
Number of records: 83287
2016-04-17

Number of records: 84138
2016-04-20
Need to reload ~850 records

The websites api is
http://lda.data.parliament.uk/answeredquestions.xml?_view=AnsweredQuestions&_pageSize=25&_page=0
which gets the latest 25 questions and answers in xml format
pagesize cannot be greater than 500 as the website crashes, in general 
it gets really slow above 100.
Page can be incremented
More info on the api at:
http://www.data.parliament.uk/developers

There is Json and XML format which may be more efficent than csv way to load 
into
elastic search - now using in scripted. Change xml to csv in url

@author: David Pavitt
2016-04-16

Started during dft hackathon
"""

from bs4 import BeautifulSoup
from datetime import datetime
from urllib import request
from time import sleep
from elasticsearch_dsl import DocType, String, Date, Boolean, Index
from elasticsearch_dsl.connections import connections

#==============================================================================

#PARAMETERS
#apip url excluding page number
api_url = ('http://lda.data.parliament.uk/answeredquestions.xml' +
           '?_view=AnsweredQuestions&_pageSize=100&_page=')
#es_domain = 'localhost'
es_domain = 'https://search-dft-hansard-rhlukwzq2jxu4g4ggcy2h47df4.eu-west-1.es.amazonaws.com'
index_name = 'hansard'
pages_to_load = 9
start_page = 0
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
"""
#SETUP ELASTIC SEARCH SERVER
#Connect to elastic search -insecure, no authentication
connections.create_connection(hosts=[es_domain], timeout=20)
#Create the elastic search index  
my_index = Index(index_name)
  
# delete the index, ignore if it doesn't exist
#WARNING THIS DELETES THE INDEX
my_index.delete(ignore=404)
# create the index in elasticsearch
#my_index.create()  # Done automatically

#Create the mappings
QandA.init()
"""
#==============================================================================        
#LOOP OVER THE API PAGE NUMBER
#start a logfile
f = open('mylog.csv', 'w')
for page in range(start_page, pages_to_load):
    #Download the data
    api_call = api_url + str(page)
    #http://stackoverflow.com/questions/7243750/download-file-from-web-in-python-3
    #http://stackoverflow.com/questions/4606919/in-python-try-until-no-error
    response = None
    while response is None:
        try:
            response = request.urlopen(api_call)
        except KeyboardInterrupt:
            raise #Quits on keyboard interupt, otherwise goes on forever
        except:
            print('API failed, trying again in a a bit')
            #Seomtimes they block us or internet goes down
            sleep(500) #sleep
            #Try again
     #Read the webpage response   
    data = response.read()      # a `bytes` object
    text = data.decode('utf-8') # a `str`; this step can't be used if data is binary
    
    #Save to file for quicker use in the future
    file_out = open('Download\\page_run2' + str(page) + '.xml', 'w', 
                    encoding='utf-8')
    file_out.write(text)
    file_out.close()
    """
    #Use Beautiful sout to read the xml file
    #https://www.crummy.com/software/BeautifulSoup/bs4/doc/#
    soup = BeautifulSoup(text, 'xml')
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
        #Put the unique identifier as the id + url (might be duplicate UIN from
        #this gov api)
        doc.meta.id = item['uin'] + '-' + item['item url']
        #UPLOAD TO ELASTICSEARCH
        doc.save()
        """
    #Update the log
    f.write(str(page) + '  ,' + str(datetime.now()) + '\n'  )
    print('Loaded ' + str(page))
    #Sleep a second before fetching the next page
    sleep(10)
f.close()
        
    

    
    
    
    
    


#There is also commons answered questions on the website




    
#Kibana
#https://search-dft-hansard-rhlukwzq2jxu4g4ggcy2h47df4.eu-west-1.es.amazonaws.com/_plugin/kibana/
    
    

    


