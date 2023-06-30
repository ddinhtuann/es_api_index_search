import base64
import numpy as np
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from elasticsearch import Elasticsearch
import json

_float32_dtype = np.dtype('>f4')
admin_client = KafkaAdminClient(bootstrap_servers = ["10.0.68.37:9092"])
topic_lists = []

def decode_float_list(base64_string):
    buffer = base64.b64decode(base64_string)
    return np.frombuffer(buffer, dtype=_float32_dtype).tolist()

def encode_array(arr):
    base64_str = base64.b64encode(np.array(arr).astype(_float32_dtype)).decode("utf-8")
    return base64_str

def decode_base64C(base64_string):
    str_arr = base64.b64decode(base64_string).decode("utf-8")
    return np.array(str_arr.split(",")).astype(_float32_dtype).tolist()

def json_serializer(data):
    return json.dumps(data).encode("utf-8")
    #return json.dumps(data)

def create_topics(topic = "_bigdata"):

        topic_exists = admin_client.list_topics()
        topic_lists.append(NewTopic(topic, num_partitions = 9 , replication_factor = 3))
        for topic in topic_lists:
            if topic not in topic_exists:
                admin_client.create_topics(topic)

class Kafka_ES(object):
    
    def __init__(self):
        
        self.kk = KafkaProducer(bootstrap_servers = ["10.0.68.37:9092", "10.0.68.103:9092", "10.0.68.100:9092"],
                            value_serializer = json_serializer, \
                            #acks = 0, \
                            retries = 1
                            )

        self.es = Elasticsearch(["http://elastic:123456a@@10.0.68.100:9200","http://elastic:123456a@@10.0.68.103:9200"],  send_get_body_as='POST', retry_on_timeout=True, timeout=5000)
        # self.admin_client = KafkaAdminClient(bootstrap_servers = ["10.0.68.37:9092"])
        # topic_list = []
        # topic_list.append(NewTopic("_bigdata", num_partitions = 1 , replication_factor = 1))
        #create_topics()

    def create_index(self, index_name, dims):
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
        self.es.indices.create(index = index_name, body= request_body)


    def check_index(self, index_name):
        return self.es.indices.exists(index= index_name)

    def delete_document(self, index_name, id):
        self.es.delete(index=index_name, id = id)
        self.es.indices.refresh(index=index_name) 
    
    def index_one(self, index_name, vector, id_doc, lang):
        if lang == "1":
            em_vector = decode_base64C(vector)
        else:
            em_vector = decode_float_list(vector)
        body_request = {
                "id": id_doc,
                "embedding_vector": em_vector,
                "index": index_name
            }
        if self.check_index(index_name =  index_name):
            #self.kk.send(index_name, body_request)
            self.kk.send("_bigdata", body_request)
        else:
            dims = len(em_vector)
            self.create_index(index_name= index_name, dims= dims)
            #self.kk.send(index_name, body_request)
            self.kk.send("_bigdata", body_request)
            self.kk.flush()
    
    def search_binary(self, index_name, query_vector, number):
        
        body_request = {
                "query": {
                    "function_score": {
                        "boost_mode": "replace",
                        "script_score": {
                            "script": {
                                "source": "binary_vector_score",
                                "lang": "knn",
                                "params": {
                                    "cosine": True,
                                    "field": "embedding_vector",
                                    "vector": decode_float_list(query_vector)
                                }
                            }
                        }
                    }
                },
                "size": number
        }
        self.es.search(index= index_name, body= body_request)

    def search(self, index_name, query_vector, number, lang, method):
        results = []
        if lang == "1":
            em_vector = decode_base64C(query_vector)
        else:
            em_vector = decode_float_list(query_vector)
        if method == "dot":

            body_request = {
                            "query": {
                                "function_score": {
                                    "boost_mode": "replace",
                                    "script_score": {
                                        "script": {
                                            "source": "dotProduct(params.vector, 'embedding_vector') + 1.0",
                                            "params": {
                                                "vector": em_vector
                                                }
                                            }
                                        }
                                    }
                                },
                            "size": number
                            }
        elif method == "cosine":

            body_request = {
                    "query": {
                        "function_score": {
                            "boost_mode": "replace",
                            "script_score": {
                                "script": {
                                    "source": "cosineSimilarity(params.vector, 'embedding_vector') + 1.0",
                                    "params": {
                                        "vector": em_vector
                                        }
                                    }
                                }
                            }
                        },
                    "size": number
                    }

        elif method == "euclidean":
            body_request = {
                    "query": {
                        "function_score": {
                            "boost_mode": "replace",
                            "script_score": {
                                "script": {
                                    "source": "1000 - l2norm(params.vector, 'embedding_vector')",
                                    "params": {
                                        "vector": em_vector
                                        }
                                    }
                                },

                            }
                        },
                    "size": number
                    }
        res = self.es.search(index= index_name, body = body_request)
        for i, result in enumerate(res['hits']['hits']):
            r = {}
            r['rank'] = i+1
            if ("@" in result['_id']) and ("&" in result['_id']):
                r['id'], url =  result['_id'].split("@")
                url1, url2 = url.split("&")
                r['url1'] = url1
                r['url2'] = url2
                
            else:
                r['id'] = result['_id']
                r['url1'] = ""
                r['url2'] = ""
            if method == "dot":
                r['score'] = result['_score'] - 1.0
            elif method == "cosine":
                r['score'] = result['_score'] - 1.0
            elif method == "euclidean":
                r['score'] = 1000 - result['_score']
            
            results.append(r)
        
        return results
    

    


    









