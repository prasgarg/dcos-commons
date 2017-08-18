FROM ubuntu:16.04
# install go 1.7
RUN add-apt-repository -y ppa:longsleep/golang-backports && apt-get update && apt-get install -y golang-go
RUN apt-get update && apt-get install -y \
    golang-go \
    python3 \
    git \
    curl \
    jq \
    default-jdk \
    python-pip \
    python3-dev \
    python3-pip \
    tox \
    software-properties-common \
    python-software-properties \
    libssl-dev \
    wget \
    zip && apt-get clean
# AWS CLI for uploading build artifacts
RUN pip install awscli
# shakedown and dcos-cli require this to output cleanly
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8
# use an arbitrary path for temporary build artifacts
ENV GOPATH=/go-tmp
