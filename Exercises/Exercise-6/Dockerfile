FROM ubuntu:18.04

# Install core dependencies
RUN apt-get update && \
    apt-get install -y default-jdk scala wget vim software-properties-common python3.8 python3-pip curl unzip libpq-dev build-essential libssl-dev libffi-dev python3-dev && \
    apt-get clean

# Download and set up Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz && \
    tar xvf spark-3.0.1-bin-hadoop3.2.tgz && \
    mv spark-3.0.1-bin-hadoop3.2 /usr/local/spark && \
    ln -s /usr/local/spark /spark

# Download required Hadoop AWS libraries
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.890/aws-java-sdk-bundle-1.11.890.jar && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar && \
    mv aws-java-sdk-bundle-1.11.890.jar /usr/local/spark/jars/ && \
    mv hadoop-aws-3.2.0.jar /usr/local/spark/jars/

# Set environment variables
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-core_2.12:0.8.0 pyspark-shell"

# Set working directory
WORKDIR /app
COPY . /app

# Upgrade pip + install Python dependencies
RUN pip3 install --upgrade pip setuptools && \
    pip3 install markupsafe==1.1.1 cryptography==3.3.2 cython==0.29.21 numpy==1.18.5 && \
    pip3 install -r requirements.txt

# Ensure output folder exists
RUN mkdir -p /app/reports
