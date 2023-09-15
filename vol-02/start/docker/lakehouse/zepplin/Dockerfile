## This Dockerfile is used to build an image containing an environment with Apache Zeppelin and Spark

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
#This Dockerfile is based on the work of Mahmud Oyinloye (https://github.com/le-oasis)
# Set up variables for customizing the build
# Spark version: 3.0.1
## The image is built executing the following command in the same folder as this file:
## docker build -t zep_setup .
## The image is run executing the following command:
## docker run -it -p 8080:8080 -p 4040:4040 -v /home/username/zeppelin:/zeppelin zep_setup
## The image is run with a volume mounted to the host to persist the notebooks and logs
## The image is run with ports 8080 and 4040 exposed to the host to access the Zeppelin UI and Spark UI

FROM apache/zeppelin:0.9.0
ENV SPARK_VERSION=3.0.1
ENV HADOOP_VERSION=3.2
ENV SPARK_INSTALL_ROOT=/spark

ENV SPARK_HOME=${SPARK_INSTALL_ROOT}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

USER root
RUN mkdir "${SPARK_INSTALL_ROOT}"
USER $USER

RUN cd "${SPARK_INSTALL_ROOT}" && \
    wget --show-progress https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz