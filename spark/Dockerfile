FROM apache/spark:3.5.0

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    pyspark==3.5

# Download Kafka connector JARs for Spark
RUN mkdir -p /opt/spark-jars && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
         -o /opt/spark-jars/spark-sql-kafka.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
         -o /opt/spark-jars/kafka-clients.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
         -o /opt/spark-jars/spark-token-provider.jar && \
    curl -fsSL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
         -o /opt/spark-jars/commons-pool2.jar

WORKDIR /app
COPY fraud_detection.py .

CMD ["spark-submit", \
     "--master", "local[*]", \
     "--jars", "/opt/spark-jars/spark-sql-kafka.jar,/opt/spark-jars/kafka-clients.jar,/opt/spark-jars/spark-token-provider.jar,/opt/spark-jars/commons-pool2.jar", \
     "fraud_detection.py"]
