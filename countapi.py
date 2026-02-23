from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200/")
cres = es.count(index="first_index")

print(f'total item in index {cres['count']},type is :  {type(cres)}')