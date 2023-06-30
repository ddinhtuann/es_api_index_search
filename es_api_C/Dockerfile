FROM dungmn/ubun18.04miniconda:latest

RUN conda install -c anaconda -y python=3.8
RUN apt-get update && apt install vim -y
RUN mkdir -p /usr/share/es_api_C/
ADD . /usr/share/es_api_C
WORKDIR /usr/share/es_api_C
RUN pip3 install -r requirements.txt
CMD python3 index_kafka_search.py