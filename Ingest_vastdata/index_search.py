
import numpy as np
import os
from kafka_produce_es import *
from flask import Flask, jsonify, request
from threading import Thread
import json
import time
from init_query import *

app = Flask(__name__)
methods = ["cosine", "dot", "euclidean"]
@app.route('/index_data', methods=['GET','POST'])
def index_vector():
    if request.method == 'POST':
        response = {}
        try:
            pos = request.form.get("location", None)
            text_speech = request.form.get("text-speech", None)
            text_speech_sms = request.form.get("text-speech-sms", None)
            text_speech_call = request.form.get("text-speech-call", None)
            voice_base64 = request.form.get("voice_embedd", type = str)
            speaker = request.form.get("speaker", type = str)

            vggface_base64 = request.form.get("vggface_embedd", None)
            isface_base64 = request.form.get("isface_embedd", None)
            facenet128_base64 = request.form.get("facenet128_embedd", None)
            facenet512_base64 = request.form.get("facenet512_embedd", None)

            obj_base64 = request.form.get("obj_embedd", type = str)

            tempora = request.form.get("time", type = str)

            _id = request.form.get("ids", type = str)
            name =  request.form.get("name", type = str)
            age =   request.form.get("age", type = str)
            gender = request.form.get("gender", type = str)
            phone = request.form.get("phone", type = str)
            cid = request.form.get("cid", type = str)

            car = request.form.get("car", type = str)
            model_make = request.form.get("model_make", type = str)
            plate = request.form.get("plate", type = str)

            pos_from = request.form.get("location_from", type = str)
            pos_to = request.form.get("location_to", type = str)

            phone_from = request.form.get("phone_from", type = str)
            phone_to = request.form.get("phone_to", type = str)

            tempora_from = request.form.get("time_from", type = str)
            tempora_to = request.form.get("time_to", type = str)

            url1 = request.form.get("file_path", type = str)
            url2 = request.form.get("face_path", type = str)
            url3 = request.form.get("obj_path", type = str)

            src = request.form.get("src", type = str)
            lang = request.form.get("type", type = str)

            if url1 == None:
                url1 = ""
            if url2 == None:
                url2 = ""
            if url3 == None:
                url3 = ""

            url = url1+"&"+url2+"&"+url3


            data = {}

            if _id == None:
                response['error'] = 1
                response['message'] = "Document missed id"
                return jsonify(response)

            if gender != None:
                if (gender.lower() != "male") & (gender.lower() !=  "female"):
                    response['error'] = 1
                    response['message'] = "Exception in gender"
                else:
                    data['male'] = "true" if gender == "male" else "false"
            
            if (pos == None ) | (pos == ""):
                data['pos'] = "rng"
            else:
                data['pos'] = pos

            if text_speech != None:
                data['text_speech'] = text_speech
            if text_speech_sms != None:
                data['text_speech_sms'] = text_speech_sms
            if text_speech_call != None:
                data['text_speech_call'] = text_speech_call

                
            if (voice_base64 != None ) & (voice_base64 != ""):
                data['voice_embedd'] = voice_base64
            
            if speaker != None:
                data['speaker'] = speaker

            if (vggface_base64 != None ) & (vggface_base64 != ""):
                data['vggface_embedd'] = vggface_base64
            if (isface_base64 != None ) & (isface_base64 != ""):
                data['isface_embedd'] = isface_base64
            if (facenet128_base64 != None ) & (facenet128_base64 != ""):
                data['facenet128_embedd'] = facenet128_base64
            if (facenet512_base64 != None ) & (facenet512_base64 != ""):
                data['facenet512_embedd'] = facenet512_base64

            if (obj_base64 != None ) & (obj_base64 != ""):
                data['obj_embedd'] = obj_base64

            if tempora == None:
                data['time'] = "1858-02-10"
            else:
                data['time'] = tempora
            
            data['_id'] = _id
            if name != None:
                data['name'] = name
            if (age == None) | (age == ""):
                data['age'] = "0"
            else:
                data['age'] = int(age)
                
            if phone != None:
                data['phone'] = phone
            if cid != None:
                data['cid'] = cid

            if car != None:
                data['car'] = car
            if model_make != None:
                data['model_make'] = model_make
            if plate != None:
                data['plate'] = model_make   

            if pos_from == None:
                data['pos_from'] = "rng"
            else:
                data['pos_from'] = pos_from 
                
            if pos_to == None:
                data['pos_to'] = "rng"
            else:
                data['pos_to'] = pos_from
                 
            if phone_from != None:
                data['phone_from'] = phone_from
            if phone_to != None:
                data['phone_to'] = phone_to

            if tempora_from == None:
                data['time_from'] = "1858-02-10"
            else:
                data['time_from'] = tempora_from

            if tempora_to == None:
                data['time_to'] = "1858-02-10"
            else:
                data['time_to'] = tempora_to


            data['src'] = src

            data['path'] = url


            produce.index_one(data, lang)
            #index_one(index_name = "test", vector= vector_b64, id_doc= id_doc, lang = lang )

            response['error'] = 0
            response['results'] = "Indexed successfully"
            
            return jsonify(response)
        
        except Exception as e:
            response['error'] = 1
            response['message'] = e
            return jsonify(response)


@app.route('/search_data', methods=['GET', 'POST'])
def search_vector():
    if request.method == 'POST':
        response = {}
        try:
            pos = request.form.get("location", None)
            text_speech = request.form.get("text-speech", None)
            text_speech_sms = request.form.get("text-speech-sms", None)
            text_speech_call = request.form.get("text-speech-call", None)
            voice_base64 = request.form.get("voice_embedd", type = str)
            speaker = request.form.get("speaker", type = str)

            vggface_base64 = request.form.get("vggface_embedd", None)
            isface_base64 = request.form.get("isface_embedd", None)
            facenet128_base64 = request.form.get("facenet128_embedd", None)

            facenet512_base64 = request.form.get("facenet512_embedd", None)

            obj_base64 = request.form.get("obj_embedd", type = str)

            time_gte = request.form.get("time_gte", type = str)
            time_lte = request.form.get("time_lte", type = str)


            name =  request.form.get("name", type = str)
            age =   request.form.get("age", type = str)
            gender = request.form.get("gender", type = str)
            phone = request.form.get("phone", type = str)
            cid = request.form.get("cid", type = str)

            car = request.form.get("car", type = str)
            model_make = request.form.get("model_make", type = str)
            plate = request.form.get("plate", type = str)
            
            pos_from = request.form.get("location_from", type = str)
            pos_to = request.form.get("location_to", type = str)

            phone_from = request.form.get("phone_from", type = str)
            phone_to = request.form.get("phone_to", type = str)

            tempora_from = request.form.get("time_from", type = str)
            tempora_to = request.form.get("time_to", type = str)

            src = request.form.get("src", type = str)

            lang = request.form.get("type", type = str)
            number = request.form.get("number", type = str)
            method = request.form.get("method", type = str)
        
            if gender != None:
                if (gender.lower() != "male") & (gender.lower() !=  "female"):
                    response['error'] = 1
                    response['message'] = "No dataset given"
                    return jsonify(response)
            else:
                male = "true" if gender == "male" else "false"

            if number == None:
                number = 10
            else:
                number = int(number)

            if method == None:
                method = "cosine"

            if method in methods:
                

                body_request = {
                    "query": {
                        "bool": {
                            "filter": []
                        }
                    }
                }
                
                
                if (pos != None) & (pos != ""):
                    pos_query = query_pos(pos, "200km")
                    body_request['query']['bool']['filter'].append(pos_query)

                if (text_speech != None) & (text_speech != ""):
                    txt_query = query_text(text_speech)
                    body_request['query']['bool']['filter'].append(txt_query)

                if (text_speech_sms != None) & (text_speech_sms != ""):
                    txt_sms_query = query_text(text_speech_sms)
                    body_request['query']['bool']['filter'].append(txt_sms_query)
                if (text_speech_call != None) & (text_speech_call != ""):
                    txt_call_query = query_text(text_speech_call)
                    body_request['query']['bool']['filter'].append(txt_call_query)

                if voice_base64:
                    voice_query = query_facenet128(voice_base64, "cosine", lang = None)
                    body_request['query']['bool']['should'] = voice_query
                if ( speaker != None) & (speaker != ""):
                    speaker_query  = query_speaker(speaker)
                    body_request['query']['bool']['filter'].append(speaker_query)

                
                if vggface_base64:
                    vggface_query = query_vggface(vggface_base64, method, lang = None)
                    body_request['query']['bool']['should'] = vggface_query

                if isface_base64:
                    isface_query = query_isface(isface_base64, method, lang = None)
                    body_request['query']['bool']['should'] = isface_query

                if facenet128_base64:

                    facenet128_query = query_facenet128(facenet128_base64, method, lang = None)
                    body_request['query']['bool']['should'] = facenet128_query

                if facenet512_base64:
                    facenet512_query = query_facenet512(facenet512_base64, method, lang = None)
                    body_request['query']['bool']['should'] = facenet512_query

                if obj_base64:
                    obj_query = query_object(obj_base64, method, lang = None)
                    body_request['query']['bool']['should'] = obj_query

                if (time_lte != None) | (time_gte != None):
                        time_query = query_time(time_gte, time_lte)
                        body_request['query']['bool']['filter'].append(time_query)

                if (name != None):
                    name_query = query_name(name)
                    body_request['query']['bool']['filter'].append(name_query)
                
                if (age != None):
                    age_query = query_age(age)
                    body_request['query']['bool']['filter'].append(age_query)

                if gender != None:
                    if (gender.lower() != "male") & (gender.lower() !=  "female"):
                        response['error'] = 1
                        response['message'] = "No dataset given"
                        return jsonify(response)
                    else:
                        male = "true" if gender == "male" else "false"
                        gender_query = query_gender(male)

                if (phone != None):
                    phone_query = query_phone(phone)
                    body_request['query']['bool']['filter'].append(phone_query)

                if (cid != None):
                    cid_query = query_cid(cid)
                    body_request['query']['bool']['filter'].append(cid_query)

                if (car != None):
                    car_query = query_car(car)
                    body_request['query']['bool']['filter'].append(car_query)

                if (model_make != None):
                    mm_query = query_model_make(model_make)
                    body_request['query']['bool']['filter'].append(mm_query)

                if (plate != None):
                    plate_query = query_plate(plate)
                    body_request['query']['bool']['filter'].append(plate_query)
                
                src_query = query_src(src)
                body_request['query']['bool']['filter'].append(src_query)
                # body_request = json.dumps(body_request)
                try:
                    
                    response['results'] = produce.search(body_request= body_request, method= method)
                    response['error'] = 0
                    return jsonify(response)

                except Exception as e:
                    response['error'] = 1
                    response['message'] = e
                    return jsonify(response)

            else:
                response['message'] = "Invalid method"
                response['error'] = 1
                return jsonify(response)
        
        except Exception as e:
            response['error'] = 1
            response['message'] = str(e)
            return jsonify(response)
        
@app.route('/delete_data', methods=['GET', 'POST'])
def delete_vectors():
    if request.method == 'POST':
        response = {}
        try:
            
            pid = request.form.get("id", type = str)
            url1 = request.form.get("file_origin", type = str)
            url2 = request.form.get("file_roi", type = str)
            if url1 == None:
                url1 = ""
            if url2 == None:
                url2 = ""
            if pid == None:
                pid = ""
            
            eli_id = pid+"@"+url1+"&"+url2

            dataset = request.form.get("dataset", type = str)
            if dataset == None:
                response['error'] = 1
                response['message'] = "No dataset given"
                return jsonify(response)

            else:
                if not produce.check_index(dataset):
                    response['error'] = 1
                    response['message'] = "The dataset is not available"
                    return jsonify(response)
                else:
                    produce.delete_document(dataset, eli_id)
                    response['error'] = 0
                    response['results'] = "Deleted successfully"

            return jsonify(response)
        except Exception as e:
            response['error'] = 1
            response['message'] = str(e)
            return jsonify(response)

if __name__=="__main__":

    produce = Kafka_ES()
    if not produce.check_index("bigdata"):
        produce.create_index("bigdata")

    app.run(host='0.0.0.0', debug=True)







