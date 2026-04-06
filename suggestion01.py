from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")
index_name = "bank_autocomplete"

# Delete the index first so the mapping is clean for repeated runs.
if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

# Create an index with a search_as_you_type title field, a normal text field,
# and a keyword field to compare behavior.
es.indices.create(
    index=index_name,
    mappings={
        "properties": {
            "title": {
                "type": "search_as_you_type"
            },
            "customer_name": {
                "type": "text"
            },
            "account_type": {
                "type": "keyword"
            }
        }
    }
)

# Index sample bank-domain documents.
docs = [
    {
        "id": 1,
        "document": {
            "title": "Personal Savings Account",
            "customer_name": "Alice Morgan",
            "account_type": "savings"
        }
    },
    {
        "id": 2,
        "document": {
            "title": "Business Checking Account",
            "customer_name": "Bob Singh",
            "account_type": "checking"
        }
    },
    {
        "id": 3,
        "document": {
            "title": "Student Savings Plan",
            "customer_name": "Charlie Davis",
            "account_type": "savings"
        }
    }
]

for doc in docs:
    es.index(index=index_name, id=doc["id"], document=doc["document"])

es.indices.refresh(index=index_name)

# Search as you type on the title field.
search_as_you_type_query = {
    "query": {
        "multi_match": {
            "query": "pers sav",
            "type": "bool_prefix",
            "fields": [
                "title",
                "title._2gram",
                "title._3gram"
            ]
        }
    }
}

# Normal text search on the customer_name field for comparison.
text_search_query = {
    "query": {
        "match": {
            "customer_name": "Alice"
        }
    }
}

# Exact match search on the keyword field.
keyword_exact_query = {
    "query": {
        "term": {
            "account_type": "savings"
        }
    }
}

# Partial keyword search using wildcard to illustrate keyword behavior.
keyword_partial_query = {
    "query": {
        "wildcard": {
            "account_type": {
                "value": "sav*"
            }
        }
    }
}

print("=== search_as_you_type title query ===")
response = es.search(index=index_name, body=search_as_you_type_query)
for hit in response["hits"]["hits"]:
    print(hit["_source"])

print("\n=== normal text match on customer_name ===")
response = es.search(index=index_name, body=text_search_query)
for hit in response["hits"]["hits"]:
    print(hit["_source"])

print("\n=== exact keyword term search on account_type ===")
response = es.search(index=index_name, body=keyword_exact_query)
for hit in response["hits"]["hits"]:
    print(hit["_source"])

print("\n=== partial keyword search on account_type with wildcard ===")
response = es.search(index=index_name, body=keyword_partial_query)
for hit in response["hits"]["hits"]:
    print(hit["_source"])
