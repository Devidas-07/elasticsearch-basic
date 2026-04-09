from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv
from updated_data import doc_updated as doc

# Load environment variables
load_dotenv()

# Elasticsearch client
es = Elasticsearch(hosts=[os.getenv('ELASTICSEARCH_URL')])
#delete index if exists
es.options(ignore_status=[400, 404]).indices.delete(index="bank_index_updated")
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
                        "type": "keyword",
                        "fields": {
                            "prefix": {
                                "type": "text",
                                "analyzer": "edge_ngram_analyzer",
                                "search_analyzer": "standard"
                            }
                        }
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
input = "Deepak Pa"
print("Input:", input)
def run_queries():
   # Prefix query using edge-ngram subfield
    prefix_query = {
        "match": {
            "middlename.prefix": {
                "query": input
            }
        }
    }
    prefix_result = es.search(
        index="bank_index_updated",
        query=prefix_query,
        _source=["middlename"]
    )
    print("prefix result:", [hit["_source"] for hit in prefix_result["hits"]["hits"]])
    # match_phrase_prefix query
    query = {
        "query": {
            "match_phrase_prefix": {
                "lastname": {
                    "query": input
                }
            }
        }
    }
    result = es.search(
        index="bank_index_updated",
        query=query["query"],
        _source=["lastname"]
    )
    print("match_phrase_prefix result:", [hit["_source"] for hit in result["hits"]["hits"]])

    

    

    #search_as_you_type query

    search_as_you_type_query ={
        "query": {
            "multi_match": {
                "query": input,
                "type": "bool_prefix",
                "fields": [
                    "nickname",
                    "nickname._2gram",
                    "nickname._3gram"
                ]
            }
        }
    }

    search_as_you_type_response = es.search(
        index="bank_index_updated",
        body=search_as_you_type_query
    )

    print("search_as_you_type result:", [hit["_source"]["nickname"] for hit in search_as_you_type_response["hits"]["hits"]])

    # Edge n-gram query
    edge_ngram_query = {
        "query": {
            "match": {
                "firstname_edge": input
            }
        }
    }
    edge_ngram_result = es.search(
        index="bank_index_updated",
        query=edge_ngram_query["query"],
        _source=["firstname"]
    )
    print("edge n-gram result:", [hit["_source"] for hit in edge_ngram_result["hits"]["hits"]])

if __name__ == "__main__":
    create_index()
    insert_documents(doc)
    run_queries()