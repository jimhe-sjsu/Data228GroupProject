ARG SPARK_BASE_IMAGE=apache/spark:3.5.3
FROM ${SPARK_BASE_IMAGE}

ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-numpy python3-yaml python3-pip \
    && python3 -m pip install --no-cache-dir pyzmq==25.1.2 \
    && python3 -m pip install --no-cache-dir notebook==6.5.7 ipykernel==6.29.5 matplotlib==3.7.5 pandas==2.0.3 seaborn==0.13.2 pyarrow==12.0.1 scikit-learn==1.3.2 xgboost==2.1.4 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark

USER spark
