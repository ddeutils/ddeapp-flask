# set base image (host OS)
# TODO: Look more images in https://hub.docker.com/r/tiangolo/uwsgi-nginx/
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
COPY app ./app
COPY conf ./conf
COPY tests ./tests
COPY ./manage.py ./manage.py

# touch config file before run container with .env option
RUN touch ./.env
RUN mkdir -p ./logs

# copy .env if the deploy process can not add .env in agent after run docker
COPY ./.env ./.env

# the container listens on the specified network port
EXPOSE 5000

# command to run on container start
CMD [ "python", "./manage.py", "run", "--api=True", "recreate=True"]
