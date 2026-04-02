from elasticsearch import Elasticsearch 
es = Elasticsearch("http://localhost:9200")

if not es.indices.exists(index="autocomplete"):
    es.indices.create(
        index="autocomplete",
        mappings={
            "properties": {
                "title": {
                    "type": "search_as_you_type"
                }
            }
        }
    )
# index a document on the field that is mapped as search_as_you_type
es.index(
    index="autocomplete",
    id=1,
    document={
        "title": "windows mac linux"
    },
    refresh=True
)

query = {
    "query": {
        "multi_match": {
            "query": "win ma",
            "type": "bool_prefix",
            "fields": [
                "title",
                "title._2gram",
                "title._3gram"
            ]
        }
    }
}

response = es.search(
    index="autocomplete",
    body=query
)

print("hits:", response["hits"]["hits"])
