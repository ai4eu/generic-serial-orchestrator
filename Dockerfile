# Dockerfile for building generic-serial-orchestrator image from Ubuntu 18.04 image
FROM ubuntu:18.04

# Install necessary system packages
RUN apt-get update \
    && apt-get install -y \
    python3 \
    python3-pip \
    autoconf \
    automake \
    cmake \
    curl \
    g++ \
    git \
    graphviz \
    libatlas3-base \
    libtool \
    make \
    pkg-config \
    sox \
    subversion \
    unzip \
    wget \
    zlib1g-dev \
    vim \
    nano

RUN export PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Make python3 default
RUN ln -s /usr/bin/python3 /usr/bin/python \
    && ln -s /usr/bin/pip3 /usr/bin/pip

# Install necessary Python packages
RUN pip install --upgrade pip \
    numpy \
    glob2 \
    zipfile36


RUN python3 -m pip install grpcio \
    grpcio-tools \
    googleapis-common-protos


# Copy required files into the container
COPY ./orchestrator_container /orchestartor_container
RUN ls -laR /orchestartor_container/*

WORKDIR /orchestartor_container

CMD [ "python3", "./orchestrator_server.py" ]

