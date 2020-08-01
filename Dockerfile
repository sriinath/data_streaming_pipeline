FROM ubuntu:latest

LABEL maintainer = "sriinathk@gmail.com"

RUN apt-get update && apt-get install -y python3-pip && apt-get install -y git
# RUN git clone https://github.com/sriinath/data_streaming_pipeline.git
COPY . /data_streaming_pipeline
WORKDIR /data_streaming_pipeline
RUN pip3 install -r requirements.txt

CMD uwsgi --ini uwsgi.ini
