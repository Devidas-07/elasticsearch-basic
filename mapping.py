from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200")

res = es.indices.get_mapping(index="index_by_yt")
print(res)