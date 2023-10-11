# Use the base Zeppelin image as the starting point
FROM apache/zeppelin:0.9.0

# Define Spark and Hadoop versions
ENV SPARK_VERSION=3.0.1
ENV HADOOP_VERSION=3.2
ENV SPARK_INSTALL_ROOT=/spark

# Define the Spark home
ENV SPARK_HOME=${SPARK_INSTALL_ROOT}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

# Create a temporary directory for Spark installation
USER root
RUN mkdir "${SPARK_INSTALL_ROOT}"
USER $USER

# Download and extract Spark
RUN cd "${SPARK_INSTALL_ROOT}" && \
    wget --show-progress https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Copy your custom Spark configuration files into the Zeppelin container
COPY lakehouse/spark/conf* ${SPARK_HOME}/conf/

# Copy your custom JAR files into the Zeppelin container
COPY lakehouse/spark/jars/* ${SPARK_HOME}/jars/

# Continue with your Dockerfile (if you have more customizations)
