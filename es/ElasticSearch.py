# classe exemplo da alura Restaurante
import pandas as pd
import requests
import json


class ElasticSearch:
  def __init__(self, esUser, esPassword, esUri):
    self.esUser = esUser
    self.esPassword = esPassword
    self.esUri = esUri
    self.esIndex = []

  def __str__(self):
    return f'{self.esUser} @ {self.esUris}'

  def ___makeUrl(self):
    return f'http://{self.esUser}:{self.esPassword}@{self.esUri}'

  def ___convertSearch(self, jsonSearch):
    if type(jsonSearch) != type(""):
      jsonSearch = json.dumps(jsonSearch)
    return jsonSearch    

  def ____esIndexToString(self):
    return ','.join(self.esIndex)

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


  def getById(self, id):
    url = self.___makeUrl() + "/" + self.____esIndexToString() + "/_doc/" + str(id)
    response = requests.get(url, headers={'Content-type': 'application/json'})
    return response.json()

  def search(self, jsonSearch = {"query": {"match_all": {}}}):
    return self.____parseSearchHitsResponse(self.____search(jsonSearch=jsonSearch))

  def ____search(self, jsonSearch):
    url = self.___makeUrl() + "/" + self.____esIndexToString() + "/_search"
    response = requests.get(url, headers={'Content-type': 'application/json'}, 
                            data=self.___convertSearch(jsonSearch))
    return response.json()

  def count(self, jsonSearch = {"query": {"match_all": {}}}):    
    url = self.___makeUrl() + "/" + self.____esIndexToString() + "/_count"
    response = requests.get(url, headers={'Content-type': 'application/json'}, 
                            data=self.___convertSearch(jsonSearch))
    return response.json()


  def countAll(self, groupSize = 30):
    jsonSearch = {
        "size": 0,
        "query": {"match_all": {}},
        "aggs": {
          "group_by_index": {
            "terms": {
              "field": "_index",
              "size": groupSize
            }
          }
        }
      }
    aggResponse = self.____search(jsonSearch)
    return self.____parseAggResponse(aggResponse)

