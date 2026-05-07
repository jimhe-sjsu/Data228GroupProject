ARG SPARK_BASE_IMAGE=apache/spark:3.5.3
FROM ${SPARK_BASE_IMAGE}

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-numpy python3-yaml \
    && rm -rf /var/lib/apt/lists/*

USER spark
