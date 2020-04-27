FROM python:3.7

ADD coronascript.py /

#create the environment
RUN apt-get update
RUN apt-get -y install python3-pip

# install requirements first
RUN pip3 install --upgrade google-cloud-bigquery
RUN pip3 install --upgrade google-oauth2-tool
RUN pip3 install --upgrade pandas
RUN pip3 install --upgrade gcsfs
RUN python -m pip install "dask[dataframe]"
#COPY ./requirements.txt requirements.txt
#RUN pip3 install -r requirements.txt

VOLUME /data, /output

CMD [ "python", "./coronascript.py" ]
