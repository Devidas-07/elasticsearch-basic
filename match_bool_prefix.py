from es_connection import get_es_client
es = get_es_client()
input = "Pa"
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