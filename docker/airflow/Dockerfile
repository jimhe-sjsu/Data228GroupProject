# Custom Airflow image with Docker CLI
# Allows BashOperator tasks to run `docker exec` against sibling containers
# (e.g. spark-master) via the mounted Docker socket.
FROM apache/airflow:2.9.3-python3.10

USER root

# Install Docker CLI only (not the daemon) so Airflow tasks can
# run `docker exec` commands against the Spark containers.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg \
       | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
       https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
       > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

USER airflow
