from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv
from data import doc

# Load environment variables
load_dotenv()

# Elasticsearch client
es = Elasticsearch(hosts=[os.getenv('ELASTICSEARCH_URL')])

def create_index():
    """Create the bank_index with custom analyzers and mappings."""
    es.options(ignore_status=[400, 404]).indices.delete(index="bank_index")
    if not es.indices.exists(index="bank_index"):
        es.indices.create(
            index="bank_index",
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
                    "customer_name": {
                        "type": "text"
                    },
                    "customer_name_edge": {
                        "type": "text",
                        "analyzer": "edge_ngram_analyzer",
                        "search_analyzer": "standard"
                    },
                    "account_no": {
                        "type": "keyword"
                    },
                    "location": {
                        "type": "text"
                    }
                }
            }
        )

def insert_documents(documents):
   
    for d in documents:
        d["customer_name_edge"] = d["customer_name"]
        es.index(
            index="bank_index",
            document=d,
            refresh=True
        )

def run_queries():
   
    query = {
        "query": {
            "match_phrase_prefix": {
                "location": {
                    "query": "Falls vall",
                    "max_expansions": 3
                }
            }
        }
    }
    result = es.search(
        index="bank_index",
        query=query["query"],
        _source=["location"]
    )
    print("match_phrase_prefix result:", [hit["_source"] for hit in result["hits"]["hits"]])

    # Prefix query
    prefix_query = {
        "prefix": {
            "account_no": {
                "value": "ici"
            }
        }
    }
    prefix_result = es.search(
        index="bank_index",
        query=prefix_query,
        _source=["account_no"]
    )
    print("prefix result:", [hit["_source"] for hit in prefix_result["hits"]["hits"]])

    # Edge n-gram query
    edge_ngram_query = {
        "query": {
            "match": {
                "customer_name_edge": "deepak pa"
            }
        }
    }
    edge_ngram_result = es.search(
        index="bank_index",
        query=edge_ngram_query["query"],
        _source=["customer_name"]
    )
    print("edge n-gram result:", [hit["_source"] for hit in edge_ngram_result["hits"]["hits"]])

if __name__ == "__main__":
    create_index()
    insert_documents(doc)
    run_queries()