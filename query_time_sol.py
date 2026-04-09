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

# 
query = {
    "wildcard": {
        "middlename": {
            "value": "*Pa*",
            "case_insensitive": True
        }
    }
}
# Execute the query
result_of_keyword_query = es.search(
    index="bank_index_updated",
    query=query,
    _source=["middlename"]
)
print("Keyword query result:", [hit["_source"] for hit in result_of_keyword_query["hits"]["hits"]])

#edge n gram query 
query ={
    "query":{
        "match":{
            "firstname_edge": "Deepak Pa"
        }
    }
}
result_of_edge_ngram_query = es.search(
    index= "bank_index_updated",
    query=query["query"],
    _source=["firstname"]
)
print("Edge n-gram query result:", [hit["_source"] for hit in result_of_edge_ngram_query["hits"]["hits"]])