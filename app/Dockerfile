FROM ubuntu:xenial-20180417

#create the environment
RUN apt-get update
RUN apt-get -y install python3-pip
RUN apt-get -y install htop

# install requirements first
RUN pip3 install --upgrade google-cloud-bigquery
RUN pip3 install --upgrade google-oauth2-tool
RUN pip3 install --upgrade pandas
COPY ./requirements.txt /code/requirements.txt
RUN pip3 install -r /code/requirements.txt

FROM python:3.7

ADD coronascript.py /

VOLUME /data, /output

CMD [ "python", "./coronascript.py" ]
