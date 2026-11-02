FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# Connecteurs ajoutés à l'image (pas de téléchargement au démarrage) :
# source Kafka pour Structured Streaming + pilote JDBC PostgreSQL.
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars && \
    curl -fsSLO $MAVEN/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSLO $MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSLO $MAVEN/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSLO $MAVEN/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSLO $MAVEN/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Répertoire du checkpoint : appartient à l'utilisateur spark (volume persistant).
RUN mkdir -p /checkpoints && chown spark:spark /checkpoints

COPY streaming.py /app/streaming.py
USER spark
WORKDIR /app

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--driver-memory", "1g", \
     "--conf", "spark.sql.shuffle.partitions=4", \
     "--conf", "spark.sql.session.timeZone=UTC", \
     "/app/streaming.py"]
