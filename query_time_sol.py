from updated_data import doc_updated as doc
from es_connection import get_es_client

# Elasticsearch client
es = get_es_client()
#delete index if exists
es.options(ignore_status=[400, 404]).indices.delete(index="bank_index_updated")

# query for keyword field
def create_index():
    """Create the bank_index with custom analyzers and mappings."""
    es.options(ignore_status=[400, 404]).indices.delete(index="bank_index")
    if not es.indices.exists(index="bank_index"):
        es.indices.create(
            index="bank_index_updated",
            settings={
                "analysis": {
                    "tokenizer": {
                        "edge_ngram_tokenizer": {
                            "type": "edge_ngram",
                            "min_gram": 2,
                            "max_gram": 15,
                            "token_chars": ["letter", "digit"]
                        }
                    },
                    "analyzer": {
                        "edge_ngram_analyzer": {
                            "type": "custom",
                            "tokenizer": "edge_ngram_tokenizer",
                            "filter": ["lowercase"]
                        }
                    }
                }
            },
            mappings={
                "properties": {
                    "firstname": {
                        "type": "text"
                    },
                    "firstname_edge": {
                        "type": "text",
                        "analyzer": "edge_ngram_analyzer",
                        "search_analyzer": "standard"
                    },
                    "middlename": {
                        "type": "keyword"
                    },
                    "lastname": {
                        "type": "text"
                    },
                    "nickname":{
                        "type": "search_as_you_type"
                    }
                }
            }
        )

def insert_documents(documents):
   
    for d in documents:
        d["firstname_edge"] = d["firstname"]
        es.index(
            index="bank_index_updated",
            document=d,
            refresh=True
        )

create_index()
insert_documents(doc)

# query for keyword field
input = "Pa"
print(f"input is {input}")
# 
query_on_keyword = {
    "wildcard": {
        "middlename": {
            "value": "{input}*"
        }
    }
}
# Execute the query
result_of_keyword_query = es.search(
    index="bank_index_updated",
    query=query_on_keyword,
    _source=["middlename"]
)
print("Keyword query result:", [hit["_source"] for hit in result_of_keyword_query["hits"]["hits"]])
#fuzzines query for keyword field
fuzzy_query_on_keyword = {
    "query": {
        "fuzzy": {
            "middlename": {
                "value": input,
                "fuzziness": "AUTO",
                "max_expansions": 50,
                "prefix_length": 0
            }
        }
    }
}
result_of_fuzzy_keyword_query = es.search(
    index="bank_index_updated",
    query=fuzzy_query_on_keyword["query"],
    _source=["middlename"]
)
print("Fuzzy query result on keyword field:", [hit["_source"] for hit in result_of_fuzzy_keyword_query["hits"]["hits"]])
#edge n gram query 
edge_ngram_query ={
    "query":{
        "match":{
            "firstname_edge": input
        }
    }
}
result_of_edge_ngram_query = es.search(
    index= "bank_index_updated",
    query=edge_ngram_query["query"],
    _source=["firstname"]
)
print("Edge n-gram query result:", [hit["_source"] for hit in result_of_edge_ngram_query["hits"]["hits"]])

#using fuzziness for text field
query ={
    "query": {
        "bool": {
        "should": [
            {
            "match": {
                "lastname": {
                "query": input,
                "boost": 5
                }
            }
            },
            {
            "match": {
                "lastname": {
                "query": input,
                "fuzziness": "AUTO",
                "prefix_length": 2,
                "max_expansions": 30,
                "fuzzy_transpositions": True
                }
            }
            }
        ]
        }
    }
}

result_of_fuzzy_query = es.search(
    index= "bank_index_updated",
    query=query["query"],
    _source=["lastname"]
)
print("Fuzzy query result on text field:", [hit["_source"] for hit in result_of_fuzzy_query["hits"]["hits"]])   
