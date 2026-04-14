from es_connection import get_es_client
es = get_es_client()
input = "mi"
print("Input:", input)
#match_bool_prefix query on keyword field
match_bool_prefix_query_for_keyword = {
    "query":{
        "match_bool_prefix":{
            "middlename": input
        }
    }
}
result_of_match_bool_prefix_query_for_keyword = es.search(
    index="bank_index_updated",
    query=match_bool_prefix_query_for_keyword["query"],
    _source=["middlename"]
)
print("Match bool prefix query result on keyword field:", [hit["_source"] for hit in result_of_match_bool_prefix_query_for_keyword["hits"]["hits"]])


#prefix on keyword

prefix_query = {
    "query":{
        "prefix":{
            "middlename":{
                "value": input 
            }
        }
    }
}
# result of prefix query 
result_of_prefix_on_keyword = es.search(
    index = "bank_index_updated",
    query = prefix_query["query"],
    _source = ["middlename"]
)
print("result_of_prefix_on_keyword", [hit["_source"] for hit in result_of_prefix_on_keyword["hits"]["hits"]])

#match bool prefix query on text field
match_bool_prefix_query_for_text = {
    "query":{
        "match_bool_prefix":{
            "firstname": input
        }
    }
}
result_of_match_bool_prefix_query_for_text = es.search(
    index="bank_index_updated",
    query=match_bool_prefix_query_for_text["query"],
    _source=["firstname"]
)
print("Match bool prefix query result on text field:", [hit["_source"] for hit in result_of_match_bool_prefix_query_for_text["hits"]["hits"]])

# Mimic match_bool_prefix on keyword field using wildcard query
# Error meaning: Elasticsearch regexp queries use Lucene regex, and `\b` is not supported there.
# The `invalid character class \98` comes from the regex parser rejecting `\b` escape sequences.
wildcard_query = {
    "query": {
        "wildcard": {
            "middlename": {
                "value": f"*{input}*",
                "case_insensitive": True
            }
        }
    }
}

result_of_wildcard_query = es.search(
    index="bank_index_updated",
    query=wildcard_query["query"],
    _source=["middlename"]
)
print("Wildcard query result mimicking match_bool_prefix on keyword field:", [hit["_source"] for hit in result_of_wildcard_query["hits"]["hits"]])  


#custom logic on keyword field - improved with fuzziness and boost
# must: ensure prefix matches are always returned
# should: add fuzzy matching for typo tolerance, with lower boost score
# aggregation: surface the most frequent matched middlenames
custom_logic_query = {
    "query": {
        "bool": {
            "must": [
                {
                    "prefix": {
                        "middlename": {
                            "value": input,
                            "boost": 5,
                            "case_insensitive": True
                        }
                    }
                }
            ],
            "should": [
                {
                    "match": {
                        "middlename": {
                            "query": input,
                            "fuzziness": "AUTO",
                            "boost": 1,
                            "prefix_length": 0,
                            "fuzzy_transpositions": True
                        }
                    }
                }
            ],
            "minimum_should_match": 0
        }
    },
    "aggs": {
        "middlename_terms": {
            "terms": {
                "field": "middlename.keyword",
                "size": 10
            }
        }
    }
}
result_of_custom_logic_query = es.search(
    index="bank_index_updated",
    query=custom_logic_query["query"],
    aggs=custom_logic_query["aggs"],
    _source=["middlename"]
)
print("Custom logic query result on keyword field:", [hit["_source"] for hit in result_of_custom_logic_query["hits"]["hits"]])
print("Custom logic query aggregation:", result_of_custom_logic_query["aggregations"]["middlename_terms"]["buckets"])


# Alternative: wildcard with fuzziness for even broader matching
custom_logic_query_v2 = {
    "query": {
        "bool": {
            "should": [
                {
                    "prefix": {
                        "middlename": {
                            "value": input,
                            "boost": 10  # Highest: exact prefix match
                        }
                    }
                },
                {
                    "wildcard": {
                        "middlename": {
                            "value": f"*{input}*",
                            "case_insensitive": True,
                            "boost": 5  # Medium: wildcard substring match
                        }
                    }
                },
                {
                    "match": {
                        "middlename": {
                            "query": input,
                            "fuzziness": "AUTO",
                            "boost": 2  # Lower: fuzzy match for typos
                        }
                    }
                }
            ],
            "minimum_should_match": 1
        }
    }
}
result_of_custom_logic_query_v2 = es.search(
    index="bank_index_updated",
    query=custom_logic_query_v2["query"],
    _source=["middlename"]
)
print("Custom logic query v2 (broader matching):", [hit["_source"] for hit in result_of_custom_logic_query_v2["hits"]["hits"]])  