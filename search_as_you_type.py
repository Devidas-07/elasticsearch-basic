from elastcsearch import Elasticsearch 
es = Elasticsearch("http://localhost:9200")

es.indices.create(
    index = "autocomplete",
    mappings = {
        "properties":{
            "title":{
                "type": "search_as_you_type"
            }
        }
    }

)
es.index(
    index="autocomplete",
    id = 1,
    document = {
        "assets":"windows, mac, linux"
    },
    refresh = True
)

query = {
    "query": {
        "multi_match": {
            "query": "quick bro",
            "type": "bool_prefix",
            "fields": [
                "my_field",
                "my_field._2gram",
                "my_field._3gram"
            ]
        }
    }
}

response = es.search(
    index=INDEX_NAME,
    query=query["query"]
)
