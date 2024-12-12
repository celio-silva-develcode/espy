# importing os module for environment variables
import os
import es.ElasticSearch as es
# importing necessary functions from dotenv library
from dotenv import load_dotenv, dotenv_values 
# loading variables from .env file
load_dotenv()

def summary(val):
    if type(val) == type(None):
        print('TODO: implement this')    
    else:
        print(len(val))
        print(type(val))    
        print(val)



user = os.getenv("ES_USER")
password = os.getenv("ES_PASSWORD")
uri = os.getenv("ES_URI")
useSSL = os.getenv("ES_USE_SSL")
indexName1 = os.getenv("ES_INDEX_NAME1")
indexName2 = os.getenv("ES_INDEX_NAME2")
indexName3 = os.getenv("ES_INDEX_NAME3")

fieldNameSTATUS = os.getenv("ES_FIELD_NAME_STATUS")
fieldNameDATE = os.getenv("ES_FIELD_NAME_DATE")
fieldNameID = os.getenv("ES_FIELD_NAME_ID")
fieldNameSUB_ID = os.getenv("ES_FIELD_NAME_SUB_ID")
id = 1665

esCli = es.ESClient(esAuth = es.ESAuth(esUser = user, esPassword = password, esUri = uri))
index1 = es.ESIndex(cli=esCli,indexName=indexName1)

print(index1.count())
summary(index1.count(query = {"query": {"match_all": {}}}))
summary(index1.search(query = {"query": {"match_all": {}}}))
summary(index1.getById(id))
summary(index1.getMapping())
summary(index1.dump())



