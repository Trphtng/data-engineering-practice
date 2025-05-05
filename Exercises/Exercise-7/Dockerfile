FROM ubuntu:18.04

# Cài đặt Java, Scala, Spark, Python và các dependency
RUN apt-get update && \
    apt-get install -y default-jdk scala wget vim software-properties-common \
    python3.8 python3-pip curl unzip libpq-dev build-essential libssl-dev \
    libffi-dev python3-dev && \
    apt-get clean

# Cài đặt Apache Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar xvf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 /usr/local/spark && \
    ln -s /usr/local/spark /spark

# Cài đặt thư viện AWS cho Spark
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.890/aws-java-sdk-bundle-1.11.890.jar && \
    mv aws-java-sdk-bundle-1.11.890.jar /spark/jars && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar && \
    mv hadoop-aws-3.2.0.jar /spark/jars

# Thiết lập thư mục làm việc
WORKDIR /app
COPY . /app

# Cài đặt các Python dependencies (và thêm pytest)
RUN pip3 install markupsafe==1.1.1 cryptography==3.3.2 cython==0.29.21 numpy==1.18.5 && \
    pip3 install -r requirements.txt && \
    pip3 install pytest

# Cấu hình biến môi trường Spark
ENV PYSPARK_PYTHON=python3
ENV SPARK_HOME=/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_SUBMIT_ARGS='--packages io.delta:delta-core_2.12:0.8.0 pyspark-shell'
