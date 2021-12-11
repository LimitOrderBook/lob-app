FROM debian:buster
LABEL maintainer="B. Hanrieder (Oxford)"

RUN apt-get update && apt-get install -y \
    python3 python3-pip  libffi-dev libxml2-dev libxslt-dev ffmpeg libsm6 libxext6
    
RUN python3 -m pip install --upgrade pip \
  && python3 -m pip install numpy pymongo pandas sortedcontainers lxml bs4 flexx redis matplotlib opencv-python ipywidgets google-cloud-storage google-cloud-speech google-cloud-videointelligence 

EXPOSE 8123

COPY misc/_assetstore.py /usr/local/lib/python3.7/dist-packages/flexx/app/
COPY app/ /app/
WORKDIR /app/
RUN chmod +x /app/docker-entrypoint.sh

VOLUME ["/data"]

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["python3", "startup.py"]
