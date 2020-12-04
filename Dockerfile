FROM python:3.7
LABEL maintainer = "sriinathk@gmail.com"
  
COPY . /data_streaming_pipeline
WORKDIR /data_streaming_pipeline
RUN pip install -r requirements.txt
