from count_doc import es
res = es.indices.exists(index="bank_index")
print(f"Does bank_index exist? {res}")

#document exists or not 
doc_exists = es.exists(index="bank_index", id="1")
print(f"Does document with ID 1 exist in bank_index? {doc_exists}")

#see all documents id in bank_index 
es.search(index="bank_index", query={"match_all": {}}, _source=False, size=1000)