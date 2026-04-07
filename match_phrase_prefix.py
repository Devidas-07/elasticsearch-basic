from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv

load_dotenv()

es = Elasticsearch(
    hosts=[os.getenv('ELASTICSEARCH_URL')]
)
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
                        "max_gram": 10,
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
            "properties":{
                "customer_name":{
                    "type":"text" 
                },
                "customer_name_edge":{
                    "type":"text",
                    "analyzer":"edge_ngram_analyzer",
                    "search_analyzer":"standard"
                },
                "account_no":{
                    "type":"keyword"
                },
                "location":{
                    "type":"text"
                }
            }
        }
    )

doc = [
    {
        "customer_name": "John Doe",
        "account_no": "spi123456789",
        "location": "India"
    },
    {
        "customer_name": "Jane Smith",
        "account_no": "987654321",
        "location": "Indonesia"
    },
    {
        "customer_name": "Alice Johnson",
        "account_no": "456789123",
        "location": "Japan"
    },
    {
        "customer_name": "Bob Brown",
        "account_no": "789123456",
        "location": "China"
    },
    {
        "customer_name": "Nora Newman",
        "account_no": "234567890",
        "location": "Newark"
    },
    {
        "customer_name": "Noah Nichols",
        "account_no": "345678901",
        "location": "Norfolk"
    },
    {
        "customer_name": "Jonathan Joy",
        "account_no": "567890123",
        "location": "Naples"
    },
    {
        "customer_name": "Joanne Jordan",
        "account_no": "678901234",
        "location": "Newton"
    },
    {
        "customer_name": "Sara Patel",
        "account_no": "sbi123455",
        "location": "Newport"
    },
    {
        "customer_name": "Ravi Kumar",
        "account_no": "icici676554",
        "location": "Nashville"
    },
    {
        "customer_name": "Priya Singh",
        "account_no": "hdfc998877",
        "location": "Naperville"
    },
    {
        "customer_name": "Amit Shah",
        "account_no": "pnb112233",
        "location": "New Haven"
    },
    {
        "customer_name": "Meera Iyer",
        "account_no": "axis556677",
        "location": "Northfield"
    },
    {
        "customer_name": "Rahul Bose",
        "account_no": "sbi334455",
        "location": "Newport Beach"
    },
    {
        "customer_name": "Tina Das",
        "account_no": "icici223344",
        "location": "Norman"
    },
    {
        "customer_name": "Vikram Nair",
        "account_no": "hdfc445566",
        "location": "Newburgh"
    },
    {
        "customer_name": "Nikita Verma",
        "account_no": "pnb778899",
        "location": "Niles"
    },
    {
        "customer_name": "Samir Sethi",
        "account_no": "axis110022",
        "location": "Napa"
    },
    {
        "customer_name": "Alisha Mehta",
        "account_no": "sbi998822",
        "location": "New London"
    },
    {
        "customer_name": "Deepak Rao",
        "account_no": "icici556611",
        "location": "Newton Falls"
    },
    {
        "customer_name": "Kavita Reddy",
        "account_no": "hdfc223311",
        "location": "Newark Valley"
    },
    {
        "customer_name": "Manish Gupta",
        "account_no": "pnb334422",
        "location": "Northampton"
    },
    {
        "customer_name": "Jyoti Sen",
        "account_no": "axis669900",
        "location": "Nanuet"
    },
    {
        "customer_name": "Anil Bhatia",
        "account_no": "sbi445522",
        "location": "Neptune"
    },
    {
        "customer_name": "Leena Nair",
        "account_no": "icici887766",
        "location": "Nokomis"
    },
    {
        "customer_name": "Varun Kapoor",
        "account_no": "hdfc332211",
        "location": "Norwalk"
    },
    {
        "customer_name": "Rhea Kaur",
        "account_no": "pnb554433",
        "location": "Northport"
    },
    {
        "customer_name": "Nagesh Rao",
        "account_no": "axis223344",
        "location": "Newberry"
    },
    {
        "customer_name": "Simran Gill",
        "account_no": "sbi776655",
        "location": "Norton"
    },
    {
        "customer_name": "Harish Malik",
        "account_no": "icici112277",
        "location": "Nashua"
    },
    {
        "customer_name": "Divya Sharma",
        "account_no": "hdfc667788",
        "location": "Netcong"
    },
    {
        "customer_name": "Kunal Joshi",
        "account_no": "pnb998811",
        "location": "New Brighton"
    },
    {
        "customer_name": "Pooja Chawla",
        "account_no": "axis334411",
        "location": "Newport News"
    },
    {
        "customer_name": "Nitin Desai",
        "account_no": "sbi551122",
        "location": "Niles"
    },
    {
        "customer_name": "Seema Reddy",
        "account_no": "icici223311",
        "location": "Nassau"
    },
    {
        "customer_name": "Rohit Patel",
        "account_no": "hdfc889900",
        "location": "Nebraska City"
    }
]

def insert_doc(doc):
    for d in doc:
        d["customer_name_edge"] = d["customer_name"]
        es.index(
            index="bank_index",
            document=d,
            refresh=True
        )

insert_doc(doc)
query = {
    "query":{
        "match_phrase_prefix":{
            "location": {
                "query": "na",
                "max_expansions": 50
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

prefix_query = {
    "prefix": {
        "account_no": {
            "value": "icici"
        }
    }
}
prefix_result = es.search(
    index="bank_index",
    query=prefix_query, 
    _source=["account_no"]
)
print("prefix result:", [hit["_source"] for hit in prefix_result["hits"]["hits"]])

edge_ngram_query = {
    "query":{
        "match":{
            "customer_name_edge":"r"
        }
    }
}
edge_ngram_result = es.search(
    index="bank_index",
    query=edge_ngram_query["query"],
    _source=["customer_name"]
)
print("edge n-gram result:", [hit["_source"] for hit in edge_ngram_result["hits"]["hits"]])