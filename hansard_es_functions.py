from elasticsearch import Elasticsearch, RequestsHttpConnection
#from requests_aws4auth import AWS4Auth
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import DocType, String, Date, Boolean, Index
from bs4 import BeautifulSoup
from datetime import datetime

class QandA(DocType):
    """Class used for QandA documents. Defines all the elastic search mappings"""
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


    def save(self, ** kwargs):
        self.created_at = datetime.now()
        return super().save(** kwargs)

# def Connect():
    # """Connect to elastic search"""
    # host = 'search-dft-hansard-rhlukwzq2jxu4g4ggcy2h47df4.eu-west-1.es.amazonaws.com'
    # awsauth = AWS4Auth('AKIAJ2EXMSWRMD7CYPYQ', 
                   # 'yheE8WSbr8W4ZUdc+Oj+OXvKuXqI2rLPIZiokx2i', 
                   # 'eu-west-1', 'es')

    # connections.create_connection(
        # hosts=[{'host': host, 
        # 'port': 443}],
        # http_auth=awsauth,
        # use_ssl=True,
        # verify_certs=True,
        # connection_class=RequestsHttpConnection
    # )

def set_QandA_mappings():
    """Initialise the class"""
    QandA.init()

    
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
    
def UploadQandA(doc_dict, index):
    """Take a dictionary and pass its values
    to the doctype and then upload it to
    the index specified
    """
    doc = QandA(
            answer = doc_dict['answer'],
            answering_member = doc_dict['answering member'],
            answer_date =  doc_dict['answer date'],
            ministerial_correction = doc_dict['is it a ministerial correction'],
            first_answer_date = doc_dict['first answer date'],
            asked_date = doc_dict['asked date'],
            house = doc_dict['house'],
            question = doc_dict['question'],
            tabling_member = doc_dict['tabling member'],
            answering_members_constituency = doc_dict['answering members constituency'],
            answering_department = doc_dict['answering department'],
            tabling_members_constituency = doc_dict['tabling member constituency']
            )
    #Put the unique identifier as the id + url (might be duplicate UIN from
    #this gov api)
    #Use str() in case its None
    doc.meta.id = str(doc_dict['uin']) + '-' + str(doc_dict['item url'])
    #Set the index
    doc.meta.index = index
    #UPLOAD TO ELASTICSEARCH
    doc.save()

def DeleteIndex(index):
    """Delete the given index"""
    #ignore if it doesn't exist
    Index(index).delete(ignore=404)
    



def upload_QandA_xml(xml_filepath, index):
    """Function to go through the xml file and upload it to elastic serach"""
    #Open the file and convert to beautiful soup
    #open in binary to avoid error with encoding
    #http://stackoverflow.com/questions/24371601/beautifulsoup-decode-error
    f = open(xml_filepath, 'rb')
    soup = BeautifulSoup(f, 'xml', from_encoding="utf-8")
    #Use beautiful soup and next siblings to iterate throug the <item> tabs
    #There is an issue where using next sibling returns the string '\n' for every
    #other sibling, e.e. the records are separeted by new lines. 
    #Also the next_siblings iterator misses teh first item so thats added outside
    #the loop 
    
    #First item in xml
    first_item = soup.result.items.item
    UploadQandA(xml_item_to_dict(first_item), index)
    
    #Loop over the rest of the items 
    for i in first_item.next_siblings:
        if i != '\n': #ignore the blank new line rows
            UploadQandA(xml_item_to_dict(i), index)




