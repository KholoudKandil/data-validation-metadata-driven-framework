FROM eclipse-temurin:11-jdk-focal

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8

RUN apt-get update && apt-get install -y \
    python3.8 \
    python3-pip && \
    apt-get clean

ENV JAVA_HOME=/opt/java/openjdk
ENV PYTHONPATH=/app:$PYTHONPATH

# Configure PySpark to load Delta 3.3.2 (compatible with Spark 3.5.3+)
ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.3.2 pyspark-shell"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY config/ ./config/
COPY tests/ ./tests/

RUN mkdir -p data/input data/output

CMD ["python3.8", "-c", "print('Container ready.')"]