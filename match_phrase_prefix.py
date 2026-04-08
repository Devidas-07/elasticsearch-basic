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
                        "type": "keyword"
                    },
                    "lastname": {
                        "type": "text"
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

def run_queries():
   
    query = {
        "query": {
            "match_phrase_prefix": {
                "lastname": {
                    "query": "Deepak Pa"
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

    # Prefix query
    prefix_query = {
        "prefix": {
            "middlename": {
                "value": "Deepak Pa"
            }
        }
    }
    prefix_result = es.search(
        index="bank_index_updated",
        query=prefix_query,
        _source=["middlename"]
    )
    print("prefix result:", [hit["_source"] for hit in prefix_result["hits"]["hits"]])

    # Edge n-gram query
    edge_ngram_query = {
        "query": {
            "match": {
                "firstname_edge": "Deepak Pa"
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