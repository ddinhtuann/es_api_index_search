
from kafka_produce_es import decode_base64C, decode_float_list


def query_time(gte, lte):
    body_query = {
        "range": {
            "time": {
                "time_zone": "+07:00",
            }
        }
    }
    if gte != None:
        body_query['range']['time']['gte'] = gte
    if lte != None:
        body_query['range']['time']['lte'] = lte

    return body_query

def query_age(gte, lte):
    body_query = {
        "range": {
            "age": {
                "gte": gte,
                "lte": lte
            }
        }
    }
    return body_query

def query_pos(pos, distance):
    
    body_query = {
        "geo_distance": {
          "distance": distance,
          "pos": pos
        }
    }
    return body_query

def query_text(text):
    body_query = {
        "multi_match": {
                "query": text,
                "fields": ["text-speech", "text-speech-sms", "text-speech-call"],

            }
        }
    return body_query

def query_speaker(speaker):
    body_query = {
        "match_phrase": {
            "speaker": {
                "query": speaker
            }
        }
    }
    return body_query

def query_name(name):
    body_query = {
        "match_phrase": {
            "name": {
                "query": name
            }
        }
    }
    return body_query

def query_gender(gender):
    male = True if gender == 'male' else "female"
    body_query = {
        "term": {
            "male": male
        }
    }

    return body_query

def query_phone(phone):
    body_query = {
        "match": {
            "phone": {
                "query": phone
            }
        }
    }
    return body_query

def query_cid(cid):
    body_query = {
        "match": {
            "cid": {
                "query": cid
            }
        }
    }
    return body_query

def query_car(car):
    body_query = {
        "match_phrase": {
            "car": {
                "query": car
            }
        }
    }
    return body_query 

def query_model_make(model_make):
    body_query = {
        "match_phrase": {
            "model_make": {
                "query": model_make
            }
        }
    }
    return body_query

def query_plate(plate):
    body_query = {
        "match": {
            "plate": {
                "query": plate
            }
        }
    }
    return body_query

def query_posfrom(pos_from, distance):
    body_query = {
        "geo_distance": {
          "distance": distance,
          "pos_from": pos_from
        }
    }
    return body_query

def query_posto(pos_to, distance):
    body_query = {
        "geo_distance": {
          "distance": distance,
          "pos_to": pos_to
        }
    }
    return body_query

def query_phonefrom(phonefrom):
    body_query = {
        "match": {
            "phone_from": {
                "query": phonefrom
            }
        }
    }
    return body_query

def query_phonefrom(phoneto):
    body_query = {
        "match": {
            "phone_to": {
                "query": phoneto
            }
        }
    }
    return body_query

def query_timefrom(gte, lte):

    body_query = {
        "range": {
            "time_from": {
                "time_zone": "+07:00",
                "gte": gte,
                "lte": lte
            }
        }
    }

    return body_query

def query_timefrom(gte, lte):

    body_query = {
        "range": {
            "time_to": {
                "time_zone": "+07:00",
                "gte": gte,
                "lte": lte
            }
        }
    }
    return body_query

def query_src(src):

    body_query = {
        "match_phrase": {
            "src": {
                "query": src
            }
        }
    }
    return body_query 

def query_vggface(vggface_base64, method, lang):

    body_query = [{
        "function_score": {
            "boost_mode": "replace",
            "script_score": {
                "script": {}
            }

        }
    }]
    vector = {
        "vector": decode_base64C(vggface_base64) if lang == "1" else decode_float_list(vggface_base64)
    }
    if method == "cosine":
        score = "cosineSimilarity(params.vector, 'vggface_embedd') + 1.0"
        body_query[0]['function_score']['script_score']['script']['source'] = score
        

    elif method == "dot":
        score = "dotProduct(params.vector, 'vggface_embedd') + 1.0"
        body_query[0]['function_score']['script_score']['script']['source'] = score

    elif method == "euclidean":
        score = "1000 - l2norm(params.vector, 'vggface_embedd')"
        body_query[0]['function_score']['script_score']['script']['source'] = score

    body_query[0]['function_score']['script_score']['script']['params'] = vector

    return body_query

def query_isface(isface_base64, method, lang):

    body_query = [{
        "function_score": {
            "boost_mode": "replace",
            "script_score": {
                "script": {}
            }

        }
    }]
    vector = {
        "vector": decode_base64C(isface_base64) if lang == "1" else decode_float_list(isface_base64)
    }
    if method == "cosine":
        score = "cosineSimilarity(params.vector, 'isface_embedd') + 1.0"
        body_query[0]['function_score']['script_score']['script']['source'] = score
        

    elif method == "dot":
        score = "dotProduct(params.vector, 'isface_embedd') + 1.0"
        body_query[0]['function_score']['script_score']['script']['source'] = score

    elif method == "euclidean":
        score = "1000 - l2norm(params.vector, 'isface_embedd')"
        body_query[0]['function_score']['script_score']['script']['source'] = score

    body_query[0]['function_score']['script_score']['script']['params'] = vector

    return body_query


def query_facenet128(facenet128_base64, method, lang):

    body_query = [{
        "function_score": {
            "boost_mode": "replace",
            "script_score": {
                "script": {}
            }

        }
    }]
    vector = {
        "vector": decode_base64C(facenet128_base64) if lang == "1" else decode_float_list(facenet128_base64)
    }
    if method == "cosine":
        score = "cosineSimilarity(params.vector, 'facenet128_embedd') + 1.0"
        body_query[0]['function_score']['script_score']['script']['source'] = score
        

    elif method == "dot":
        score = "dotProduct(params.vector, 'facenet128_embedd') + 1.0"
        body_query[0]['function_score']['script_score']['script']['source'] = score

    elif method == "euclidean":
        score = "1000 - l2norm(params.vector, 'facenet128_embedd')"
        body_query[0]['function_score']['script_score']['script']['source'] = score

    body_query[0]['function_score']['script_score']['script']['params'] = vector

    return body_query

def query_facenet512(facenet512_base64, method, lang):

    body_query = [{
        "function_score": {
            "boost_mode": "replace",
            "script_score": {
                "script": {}
            }

        }
    }]
    vector = {
        "vector": decode_base64C(facenet512_base64) if lang == "1" else decode_float_list(facenet512_base64)
    }
    if method == "cosine":
        score = "cosineSimilarity(params.vector, 'facenet512_embedd') + 1.0"
        body_query[0]['function_score']['script_score']['script']['source'] = score
        

    elif method == "dot":
        score = "dotProduct(params.vector, 'facenet512_embedd') + 1.0"
        body_query[0]['function_score']['script_score']['script']['source'] = score

    elif method == "euclidean":
        score = "1000 - l2norm(params.vector, 'facenet512_embedd')"
        body_query[0]['function_score']['script_score']['script']['source'] = score

    body_query[0]['function_score']['script_score']['script']['params'] = vector

    return body_query

def query_object(obj_base64, method, lang):
    body_query = [{
        "function_score": {
            "boost_mode": "replace",
            "script_score": {
                "script": {}
            }

        }
    }]
    vector = {
        "vector": decode_base64C(obj_base64) if lang == "1" else decode_float_list(obj_base64)
    }
    if method == "cosine":
        score = "cosineSimilarity(params.vector, 'obj_embedd') + 1.0"
        body_query[0]['function_score']['script_score']['script']['source'] = score
        

    elif method == "dot":
        score = "dotProduct(params.vector, 'obj_embedd') + 1.0"
        body_query[0]['function_score']['script_score']['script']['source'] = score

    elif method == "euclidean":
        score = "1000 - l2norm(params.vector, 'obj_embedd')"
        body_query[0]['function_score']['script_score']['script']['source'] = score

    body_query[0]['function_score']['script_score']['script']['params'] = vector

    return body_query


    
    















        


