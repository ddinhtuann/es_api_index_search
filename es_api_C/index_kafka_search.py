
import numpy as np
import os
from kafka_produce_es import *
from elastic_search import index_one
from flask import Flask, jsonify, request
from threading import Thread
import json
import time

app = Flask(__name__)
methods = ["cosine", "dot", "euclidean"]
@app.route('/index_vectors', methods=['GET','POST'])
def index_vector():
    if request.method == 'POST':
        response = {}
        try:
            vector_b64 = request.form.get("vector_b64", type= str)
            dataset = request.form.get("dataset", type = str)
            pid = request.form.get("id", type= str)

            url1 = request.form.get("file_origin", type = str)
            url2 = request.form.get("file_roi", type = str)

            lang = request.form.get("type", type = str)

            if url1 == None:
                url1 = ""
            if url2 == None:
                url2 = ""
            if pid == None:
                pid = ""
            
            id_doc = pid+"@"+url1+"&"+url2

            if dataset == None:
                response['error'] = 1
                response['message'] = "No dataset given"
                return jsonify(response)
            else:
                    produce.index_one(dataset, vector_b64, id_doc, lang)
                    #index_one(index_name = "test", vector= vector_b64, id_doc= id_doc, lang = lang )

                    response['error'] = 0
                    response['results'] = "Indexed successfully"
            
            return jsonify(response)

        except Exception as e:
            response['error'] = 1
            response['message'] = str(e)
            return jsonify(response)
        
@app.route('/search_vectors', methods=['GET', 'POST'])
def search_vector():
    if request.method == 'POST':
        response = {}
        try:
            query_vectors =  request.form.get("query_vectors", type= str)
            method = request.form.get("method", type = str)
            number = request.form.get("number", type= str)
            dataset = request.form.get("dataset", type = str)
            print(number)

            lang = request.form.get("type", type = str)
        
            if dataset == None:
                response['error'] = 1
                response['message'] = "No dataset given"
                return jsonify(response)
            else:
                if not produce.check_index(dataset):
                    response['error'] = 1
                    response['message'] = "The dataset is not available"
                    return jsonify(response)

            if number == None:
                number = 10
            else:
                number = int(number)
            print(number)

            if method == None:
                method = "cosine"

            if method in methods:
                response['results'] = produce.search(index_name = dataset, query_vector = query_vectors, number= number, lang = lang, method = method)
            else:
                response['message'] = "Invalid method"
                response['error'] = 1
                return jsonify(response)
            response['error'] = 0
            return jsonify(response)
        
        except Exception as e:
            response['error'] = 1
            response['message'] = str(e)
            return jsonify(response)
        
@app.route('/delete_vectors', methods=['GET', 'POST'])
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

    app.run(host='0.0.0.0', debug=False)







