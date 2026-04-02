from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200")
res =es.cat.indices(h="index", s="index").split()
for i in res:
    print(i)