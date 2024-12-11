import es.ElasticSearch as es

user = 'aaa'
password = 'aa'
uri = 'aaa'


esAdv = es.ElasticSearch(user, password, uri)

esAdv.esIndex.append("servc_ord_advanced_search_v4_dev")
print(esAdv.countAll())


