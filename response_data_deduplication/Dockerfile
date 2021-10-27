FROM python:3.8

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY

ENV AWS_ACCESS_KEY_ID ${AWS_ACCESS_KEY_ID}
ENV AWS_SECRET_ACCESS_KEY ${AWS_SECRET_ACCESS_KEY}

# update and install python pip
RUN sed -i '/jessie-updates/d' /etc/apt/sources.list        # Now archived
RUN apt-get update && apt-get upgrade -y && apt-get install python3-pip -y
# install `aws` cli tool
RUN apt-get install -y awscli
# install pipenv
RUN pip install pipenv==2021.5.29 --force-reinstall

# Install python environment to as system default
COPY Pipfile /src/Pipfile
COPY Pipfile.lock /src/Pipfile.lock
# Set working directory
WORKDIR /src
RUN pipenv install --system --ignore-pipfile --deploy

# Creating /src partitions
COPY . /src/
