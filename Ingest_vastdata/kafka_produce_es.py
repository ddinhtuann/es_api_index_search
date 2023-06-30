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

def create_topics(topic = "bigdata"):

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
        #self.create_index(index_name = "bigdata")

    def create_index(self, index_name):
        request_body = {
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 1
            },

            "mappings": {
                "properties": {
                    "pos": {
                        "type": "geo_point",
                        "null_value": "rng"
                        },
                    "text-speech": {
                        "type": "text",
                        },
                    "text-speech-sms": {
                        "type": "text",
                        },
                    "text-speech-call": {
                        "type": "text",
                        },
                    "voice_embedd": {
                        "type": "dense_vector",
                        "dims": 192,
                        },
                    "speaker": {
                        "type": "text"
                    },
                    "vggface_embedd": {
                        "type": "dense_vector",
                        "dims": 128
                    },
                    "isface_embedd": {
                        "type": "dense_vector",
                        "dims": 512
                    },
                    "facenet128_embedd": {
                        "type": "dense_vector",
                        "dims": 128
                    },

                    "facenet512_embedd": {
                        "type": "dense_vector",
                        "dims": 512
                    },

                    "obj_embedd": {
                        "type": "dense_vector",
                        "dims": 512,
                        },
                    "time": {
                        "type": "date",
                        "null_value": "1858-02-10"
                        },
                    "name": {
                        "type": "text",
                    },
                    "age": {
                        "type": "byte",
                        "null_value": "0"
                        },
                    "male": {
                        "type": "boolean",
                        },
                    "phone": {
                        "type": "text",
                        },
                    "cid": {
                        "type": "text",
                        },
                    "car": {
                        "type": "text"
                    },
                    "model_make": {
                        "type": "text"
                    },
                    "plate": {
                        "type": "text"
                    },
                    "pos_from": {
                        "type": "geo_point",
                        "null_value": "rng"
                        },
                    "pos_to": {
                        "type": "geo_point",
                        "null_value": "rng"
                        },
                    "phone_from": {
                        "type": "text",
                        },
                    "phone_to": {
                        "type": "text",
                    },
                    "time_from": {
                        "type": "date",
                        "null_value": "1858-02-10"
                    },
                    "time_to": {
                        "type": "date",
                        "null_value": "1858-02-10"
                    },
                    "src": {
                        "type": "text"
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
    
    def index_one(self, data, lang):

        if "voice_embedd" in data:
            if lang == "1":
                data['voice_embedd'] = decode_base64C(data['voice_embedd'])
            else:
                data['voice_embedd'] = decode_float_list(data['voice_embedd'])

        if "vggface_embedd" in data:
            if lang == "1":
                data['vggface_embedd'] = decode_base64C(data['vggface_embedd'])
            else:
                data['vggface_embedd'] = decode_float_list(data['vggface_embedd'])

        if "isface_embedd" in data:
            if lang == "1":
                data['isface_embedd'] = decode_base64C(data['isface_embedd'])
            else:
                data['isface_embedd'] = decode_float_list(data['isface_embedd'])

        if "facenet128_embedd" in data:
            if lang == "1":
                data['facenet128_embedd'] = decode_base64C(data['facenet128_embedd'])
            else:
                data['facenet128_embedd'] = decode_float_list(data['facenet128_embedd'])
        if "facenet512_embedd" in data:
            if lang == "1":
                data['facenet512_embedd'] = decode_base64C(data['facenet512_embedd'])
            else:
                data['facenet512_embedd'] = decode_float_list(data['facenet512_embedd'])

        
        if "obj_embedd" in data:
            if lang == "1":
                data['obj_embedd'] = decode_base64C(data['obj_embedd'])
            else:
                data['obj_embedd'] = decode_float_list(data['obj_embedd'])

        #data = json.dumps(data)

        self.kk.send("bigdata", data)

        #self.kk.send(index_name, body_request)
        #self.kk.flush()
    
    def search_binary(self, index_name, query_vector, number):
        
        body_request = {
                "query": {
                    "function _score": {
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

    def search(self, body_request, method ):
        results = []
    #     if lang == "1":
    #         em_vector = decode_base64C(query_vector)
    #     else:
    #         em_vector = decode_float_list(query_vector)
    #     if method == "dot":

    #         body_request = {
    #                         "query": {
    #                             "function_score": {
    #                                 "boost_mode": "replace",
    #                                 "script_score": {
    #                                     "script": {
    #                                         "source": "dotProduct(params.vector, 'embedding_vector') + 1.0",
    #                                         "params": {
    #                                             "vector": em_vector
    #                                             }
    #                                         }
    #                                     }
    #                                 }
    #                             },
    #                         "size": number
    #                         }
    #     elif method == "cosine":

    #         body_request = {
    #                 "query": {
    #                     "function_score": {
    #                         "boost_mode": "replace",
    #                         "script_score": {
    #                             "script": {
    #                                 "source": "cosineSimilarity(params.vector, 'embedding_vector') + 1.0",
    #                                 "params": {
    #                                     "vector": em_vector
    #                                     }
    #                                 }
    #                             }
    #                         }
    #                     },
    #                 "size": number
    #                 }

    #     elif method == "euclidean":
    #         body_request = {
    #                 "query": {
    #                     "function_score": {
    #                         "boost_mode": "replace",
    #                         "script_score": {
    #                             "script": {
    #                                 "source": "1000 - l2norm(params.vector, 'embedding_vector')",
    #                                 "params": {
    #                                     "vector": em_vector
    #                                     }
    #                                 }
    #                             },

    #                         }
    #                     },
    #                 "size": number
    #                 }
        res = self.es.search(index= "bigdata", body = body_request)

        for i, result in enumerate(res['hits']['hits']):
            r = {}

            r['rank'] = i+1
            r['ids'] =  result['_id']
            r['source'] = result['_source']
            r['source'].pop('_id')
            
            if (method == "cosine") | (method == "dot"):
                r['score'] =  result['_score'] - 1
            elif (method == "euclidean"):
                r['score'] =  1000 - result['_score']

            if 'voice_embedd' in r['source']:
                r['source'].pop('voice_embedd')

            if 'vggface_embedd' in r['source']:
                r['source'].pop('vggface_embedd')
            if 'isface_embedd' in r['source']:
                r['source'].pop('isface_embedd')
            if 'facenet128_embedd' in r['source']:
                r['source'].pop('facenet128_embedd')
            if 'facenet512_embedd' in r['source']:
                r['source'].pop('facenet512_embedd')
            
            if 'obj_embedd' in r['source']:
                r['source'].pop('obj_embedd')

            results.append(r)
        return results
    

    


    






