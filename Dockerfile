FROM apache/airflow:2.6.3
USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Java home using Debian's default symlink
RUN ln -s /usr/lib/jvm/java-11-openjdk-amd64 /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Ensure PATH includes standard system directories
ENV PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:${PATH}"

ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Set Airflow's user paths
ENV PATH="/home/airflow/.local/bin:${PATH}"
ENV PYTHONPATH="/opt/airflow:/home/airflow/.local/lib/python3.7/site-packages:${PYTHONPATH}"

USER airflow
COPY --chown=airflow:airflow requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
