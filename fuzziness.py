from es_connection import get_es_client
es = get_es_client()
input = "Pa"
#fuzziness query on keyword field
query = {
  "query": {
    "bool": {
      "should": [
        {
          "match": {
            "middlename": {
              "query": input,
              "boost": 5
            }
          }
        },
        {
          "match": {
            "middlename": {
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
result_of_fuzzy_keyword_query = es.search(
    index="bank_index_updated",
    query=query["query"],
    _source=["middlename"]
)
print("Fuzzy query result on keyword field:", [hit["_source"] for hit in result_of_fuzzy_keyword_query["hits"]["hits"]])    



#using fuzziness for text field
{
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
