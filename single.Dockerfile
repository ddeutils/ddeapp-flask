FROM python:3.9-slim

RUN apt-get update \
    && apt-get -y install nginx curl \
    && apt-get clean

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file to the working directory
COPY ./requirements.pre.txt ./requirements.pre.txt
COPY ./requirements.analytic.txt ./requirements.analytic.txt
COPY ./requirements.txt ./requirements.txt

# Install Python Dependencies
RUN pip install --no-cache-dir -r requirements.analytic.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the content of the local directory to the working directory
COPY app ./app
COPY conf ./conf
COPY tests ./tests
COPY ./manage.py ./manage.py

# Touch config file before run container with .env option
RUN touch ./.env
RUN mkdir -p ./logs

# Copy .env if the deploy process can not add .env in agent after run docker
COPY ./.env ./.env

# The container listens on the specified network port
EXPOSE 5000

# Command to run on container start
CMD [ "python", "./manage.py", "run", "--api", "--recreate"]
