FROM python:3.6-slim as build

RUN apt-get update
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt requirements.txt
RUN /opt/venv/bin/python -m pip install --upgrade pip && pip install -r requirements.txt

FROM python:3.6-slim

RUN apt-get update -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY . /producers
WORKDIR /producers
