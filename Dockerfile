FROM ubuntu:latest
RUN apt-get update -y
RUN apt-get install -y python3
RUN apt-get install -y python3-pip

EXPOSE 9090

COPY . /app
WORKDIR /app
RUN pip3 install -r /app/requirements.txt
ENTRYPOINT ["python3"]
CMD ["main.py"]
