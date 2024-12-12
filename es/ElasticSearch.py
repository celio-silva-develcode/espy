# classe exemplo da alura Restaurante
import pandas as pd
import requests
import json


class ESAuth:
    def __init__(self, esUser, esPassword, esUri, useSSL = False):
        self.esUser = esUser
        self.esPassword = esPassword
        self.esUri = esUri
        self.useSSL = useSSL
    def __str__(self):
        return f'{self.esUser} @ {self.esUris}'
    def isUsingSSL(self):
        return self.useSSL
    def makeUrl(self):
       tmp = ''
       if self.useSSL:
           tmp = 's'
       urlTmp = f'http{tmp}://{self.esUser}:{self.esPassword}@{self.esUri}'       
       return urlTmp


class ESClient:
    def __init__(self, esAuth: ESAuth):
        self.esAuth = esAuth        
    def __str__(self):
        return f'Client for {self.ESAuth}'


    def ___convertSearch(self, jsonSearch):
        if type(jsonSearch) != type(""):
          jsonSearch = json.dumps(jsonSearch)
        return jsonSearch    


    def ____parseAggResponse(self, aggResponse):
        aggResponse = aggResponse['aggregations']['group_by_index']['buckets']
        aggResponse = pd.DataFrame(data=aggResponse)
        aggResponse.columns = ['index_name', 'count']
        return aggResponse

    def ____parseSearchHitsResponse(self, response):    
        jsonList = []
        for v in response['hits']['hits']:
          jsonList.append(v['_source'])
        
        return pd.DataFrame(data=jsonList)

    def ____search(self, jsonSearch, *indexNames):
        indexName = ','.join(indexNames)
        url = self.esAuth.makeUrl() + "/" + indexName + "/_search"
        response = requests.get(url, headers={'Content-type': 'application/json'}, 
                                data=self.___convertSearch(jsonSearch))
        return response.json()

    def getById(self, id, *indexNames):
        indexName = ','.join(indexNames)
        url = self.esAuth.makeUrl() + "/" + indexName + "/_doc/" + str(id)
        response = requests.get(url, headers={'Content-type': 'application/json'})
        return response.json()

    def search(self, jsonSearch = {"query": {"match_all": {}}}, *indexNames):        
        return self.____parseSearchHitsResponse(self.____search(jsonSearch, *indexNames))
    
    
    def getMapping(self, jsonSearch = {"query": {"match_all": {}}}, *indexNames):        
        indexName = ','.join(indexNames)
        url = self.esAuth.makeUrl() + "/" + indexName + "/_mapping"
        response = requests.get(url, headers={'Content-type': 'application/json'})
        return response.json()     

    def count(self, jsonSearch = {"query": {"match_all": {}}}, *indexNames):
        indexName = ','.join(indexNames)
        url = self.esAuth.makeUrl() + "/" + indexName + "/_count"
        response = requests.get(url, headers={'Content-type': 'application/json'}, 
                                data=self.___convertSearch(jsonSearch))
        return response.json()
    def listAll(self):
        pass



class ESIndex:
    def __init__(self, cli: ESClient, indexName):
        self.cli = cli
        self.indexName = indexName
    """
        TODO: replace with getSchema? python compatible definition?
    """
    def __str__(self):
        return f'{self.esUser} @ {self.esUris}/{self.indexName}'
    
    # promiscuous count
    def count(self, query = {"query": {"match_all": {}}}):
        return self.cli.count(query, self.indexName)
    
    # promiscuous search
    def search(self, query = {"query": {"match_all": {}}}):
        return self.cli.search(query, self.indexName)
    
    # find by index ID _id
    def getById(self, id):
        return self.cli.getById(id, self.indexName)
    
    # json definition of the index
    def getMapping(self):
        return self.cli.getMapping(self.indexName)        
    
    """
        TODO: Dump by sequencial field or date or some field well distributed
    """
    # extract the entire index
    def dump(self, *fieldNames):
        pass
    def delete(self, id):
        pass
    def put(self, id, jsonValue):
        pass
