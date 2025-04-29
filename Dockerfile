FROM python:3.10.13-slim

COPY . ./

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ENV PYTHONPATH=${PYTHONPATH}:${PWD} 
