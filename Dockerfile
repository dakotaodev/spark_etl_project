FROM bitnami/spark:3.4.1

ENV DELTA_VERSION=2.4.0
ENV SPARK_HOME=/opt/bitnami/spark

# Add Delta Lake JAR
RUN mkdir -p $SPARK_HOME/jars && \
    curl -L -o $SPARK_HOME/jars/delta-core_2.12-${DELTA_VERSION}.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_VERSION}/delta-core_2.12-${DELTA_VERSION}.jar

ENV JAVA_HOME=/opt/bitnami/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"