FROM ubuntu:18.04

LABEL maintainer="adelkuanysheva7@gmail.com"

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip

COPY ./requirements.txt /Storage/requirements.txt

WORKDIR /Storage
RUN pip3 install -r requirements.txt
COPY . /Storage

ENTRYPOINT [ "python3" ]
CMD [ "app.py" ]