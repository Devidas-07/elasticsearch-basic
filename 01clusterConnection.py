from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200")
res = es.info()
print(res)
delteres = es.indices.delete(index="index_by_yt", ignore=[400, 404])
print(delteres)
indexres =es.indices.create(index="index_by_yt")
print(indexres)