from elasticsearch import Elasticsearch
es = Elasticsearch("https://localhost:9200")

if not es.indices.exists(index="autocomplete02"):
    es.indices.create(
        index="autocomplete02",
        mappings={
            "properties":{
                "movie_name":{
                    "type":"search_as_you_type"
                },
                "genre":{
                    "type":"text"
                },
                "length":{
                    "type":"keyword"
                }
            }
        
        }
    )


## movie list 
movie_list =[
    {"movie_name":"hera-pheri","genre":"comedy","length":178},
    {"movie_name":"phir hera pheri","genre":"comedy","length":180},
    {"movie_name":"3 idiots","genre":"comedy","length":170},
    {"movie_name":"dangal","genre":"sports","length":161},
    {"movie_name":"daniel","genre":"action","length":150},
    {"movie_name":"sholay","genre":"action","length":204},
    {"movie_name":"titanic","genre":"romance","length":195}
]
def insert_doc(movie_list):
    for movie in movie_list:
        es.index(
            index="autocomplete02",
            document=movie,
            refresh=True
        )
query = {
    "query":{
        "bool":{
            "should":[
                {
                    "multi_match":{
                        "query":" ",
                        "type":"bool_prefix",
                        "fields":[
                            "movie_name",
                            "movie_name._2gram",
                            "movie_name._3gram"
                        ]
                    }
                },
                {
                    "match_phrase_prefix":{
                        "genre":" ",
                    }
                },
                {
                    "prefix":{
                        "length":" "
                    }
                }
            ]
        }
    }
}
#insert_doc(movie_list)
res =es.search(
    index="autocomplete02",
    query=query["query"]
)
print(res)