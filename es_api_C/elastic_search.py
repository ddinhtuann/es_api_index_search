
from elasticsearch import Elasticsearch
import base64
import numpy as np


import os

_float32_dtype = np.dtype('>f4')

def decode_float_list(base64_string):
    buffer = base64.b64decode(base64_string)
    return np.frombuffer(buffer, dtype=_float32_dtype).tolist()

def encode_array(arr):
    base64_str = base64.b64encode(np.array(arr).astype(_float32_dtype)).decode("utf-8")
    return base64_str

def decode_base64C(base64_string):
    str_arr = base64.b64decode(base64_string).decode("utf-8")
    return np.array(str_arr.split(",")).astype(_float32_dtype).tolist()


es = Elasticsearch(["http://elastic:123456a@@10.0.68.37:9200","http://elastic:123456a@@10.0.68.103:9200"],  send_get_body_as='POST', retry_on_timeout=True, timeout=5000)
#dims = 512

def create_index(name, dims) :     
    request_body = {
        "settings": {
            "number_of_shards": 3,
            "number_of_replicas": 0
            },

        "mappings": {

                "properties": {
                    "embedding_vector": {
                        "type": "dense_vector",
                        "dims": dims
                        }
                        
                    }
                }
    
        }
    
    es.indices.create(index = name, body = request_body)
    print(f"Created {name} index... {request_body['mappings']['properties']}")


def index_one(index_name, vector, id_doc, lang):
    if lang== "1":
        query_vector = decode_base64C(vector)
    else:
        query_vector = decode_float_list(vector)

    body = {
        "embedding_vector": query_vector
    }

    es.index(index= index_name, body= body, id = id_doc)
    #print("Indexing successfully")


def search(name, query_vector, number):
    query = {
        "query": {
            "function_score": {
                "boost_mode": "replace",
                "script_score": {
                    "script": {
                        "source": "dotProduct(params.vector, 'embedding_vector') + 1.0 ",
                            
                        "params": {
                            "vector": query_vector
                            }
                        }
                    }
                }
            }
        }

    #es.indices.refresh(index=name)
    return es.search(index = name, body = query, size =number)

def searchv2(name, query_vector, number):
    query = {
    "query": {
        "function_score": {
            "boost_mode": "replace",
            "script_score": {
                "script": {
                    "source": "cosineSimilarity(params.vector, 'embedding_vector') + 1.0",
                        
                    "params": {
                        "vector": query_vector
                        }
                    }
                }
            }
        }
    }
    #es.indices.refresh(index=name)
    return es.search(index = name, body = query, size =number)

def searchv3(name, query_vector, number):
    query = {
    "query": {
        "function_score": {
            "boost_mode": "replace",
            "script_score": {
                "script": {
                    "source": "1000 - l2norm(params.vector, 'embedding_vector')", 
                    "params": {
                        "vector": query_vector
                        }
                    }
                },
            
            }
        }
    }

    return es.search(index = name, body = query, size =number)


def check_index(index_name):
    return es.indices.exists(index= index_name)

def delete_document(index_name, id):
    es.delete(index=index_name, id = id)
    es.indices.refresh(index=index_name)
