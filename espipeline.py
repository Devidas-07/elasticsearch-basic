from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200")
ires = es.indices.create(index="first_index")
if ires:
    print("index created")
else:
    print("not created")