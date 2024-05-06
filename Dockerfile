FROM python:3.9 AS build

RUN apt-get update  \
    && apt-get -y install build-essential nginx curl \
    && apt-get clean

ENV VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

ADD --chmod=755 https://astral.sh/uv/install.sh /install.sh

RUN /install.sh  \
    && rm /install.sh

COPY ./requirements.analytic.txt ./requirements.analytic.txt
COPY ./requirements.txt ./requirements.txt

RUN /root/.cargo/bin/uv venv /opt/venv

RUN /root/.cargo/bin/uv pip install -U pip \
    && /root/.cargo/bin/uv pip install --no-cache -r requirements.analytic.txt \
    && /root/.cargo/bin/uv pip install --no-cache -r requirements.txt

FROM python:3.9-alpine

COPY --from=build /opt/venv /opt/venv

# Activate the virtualenv in the container
# See here for more information:
# https://pythonspeed.com/articles/multi-stage-docker-python/
ENV PATH="/opt/venv/bin:$PATH"

# Copy the content of the local directory to the working directory
COPY app ./app
COPY conf ./conf
COPY tests ./tests
COPY ./manage.py ./manage.py

# Touch config file before run container with .env option
RUN touch ./.env  \
    && mkdir -p ./logs

# Copy .env if the deploy process can not add .env in agent after run docker
COPY ./.env ./.env

EXPOSE 5000

ENTRYPOINT python ./manage.py run --api --recreated
