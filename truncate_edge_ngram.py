from count_doc import es

INDEX_NAME = "demo_truncate_edge"

# -----------------------------------
# Delete index if already exists
# -----------------------------------
if es.indices.exists(index=INDEX_NAME):
    es.indices.delete(index=INDEX_NAME)

# -----------------------------------
# Create index with edge_ngram + truncate
# -----------------------------------
es.indices.create(
    index=INDEX_NAME,
    body={
        "settings": {
            "analysis": {
                "tokenizer": {
                    "edge_3": {
                        "type": "edge_ngram",
                        "min_gram": 1,
                        "max_gram": 3,
                        "token_chars": ["letter"]
                    }
                },
                "filter": {
                    "truncate_3": {
                        "type": "truncate",
                        "length": 3
                    }
                },
                "analyzer": {
                    # Index-time analyzer (edge n-gram)
                    "index_autocomplete": {
                        "tokenizer": "edge_3",
                        "filter": ["lowercase"]
                    },
                    # Search-time analyzer (truncate)
                    "search_truncate": {
                        "tokenizer": "standard",
                        "filter": ["lowercase", "truncate_3"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "word": {
                    "type": "text",
                    "analyzer": "index_autocomplete",
                    "search_analyzer": "search_truncate"
                }
            }
        }
    }
)

print("✅ Index created")

# -----------------------------------
# Index sample documents
# -----------------------------------
docs = ["apple", "apply", "approximate", "banana"]

for i, word in enumerate(docs, start=1):
    es.index(index=INDEX_NAME, id=i, document={"word": word})

es.indices.refresh(index=INDEX_NAME)
print("✅ Documents indexed")

# -----------------------------------
# Search for 'apple'
# -----------------------------------
query = "apple"

response = es.search(
    index=INDEX_NAME,
    body={
        "query": {
            "match": {
                "word": query
            }
        }
    }
)

print(f"\n🔍 Search results for '{query}':\n")

for hit in response["hits"]["hits"]:
    print("•", hit["_source"]["word"])