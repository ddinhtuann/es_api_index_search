FROM ubuntu:18.04

RUN apt-get update && apt-get install -y software-properties-common gcc && \
    add-apt-repository -y ppa:deadsnakes/ppa

RUN apt-get update && apt-get install -y python3.6 python3-distutils python3-pip python3-apt

RUN apt-get update && apt install vim -y

RUN mkdir -p /usr/share/ingest_bigdata/
ADD . /usr/share/ingest_bigdata
WORKDIR /usr/share/ingest_bigdata
RUN pip3 install -r requirements.txt
CMD python3 index_search.py