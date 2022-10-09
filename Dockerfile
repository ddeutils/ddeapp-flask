# set base image (host OS)
FROM python:3.8-slim

RUN apt-get clean \
    && apt-get -y update

RUN apt-get -y install \
    nginx \
    python3-dev \
    build-essential

# set the working directory in the container
WORKDIR /app

# copy the dependencies file to the working directory
COPY requirements.pre.txt ./requirements.pre.txt
COPY ./requirements.txt ./requirements.txt

# install dependencies
RUN pip install --no-cache-dir -r requirements.pre.txt
RUN pip install --no-cache-dir -r requirements.txt

# copy the content of the local directory to the working directory
COPY component ./component
COPY src ./src
COPY conf ./conf
COPY ./server.py ./server.py

# touch config file before run container with config.yaml option
RUN touch ./config.yaml
RUN mkdir -p ./log

# copy config.yaml if the deploy process can not add config.yaml in agent after run docker
COPY ./config.yaml ./config.yaml

# the container listens on the specified network port
EXPOSE 5000

# command to run on container start
CMD [ "python", "./manage.py", "run" ]