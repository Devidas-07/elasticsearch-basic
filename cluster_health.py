from es_connection import get_es_client
es = get_es_client()

#get health of cluster 
response = es.cluster.health()
print("Cluster health:", response)