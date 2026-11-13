# One image for both engines: the pandas reference engine and PySpark + Delta Lake.
FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps make \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    IVY_HOME=/opt/ivy

WORKDIR /app
COPY requirements.txt requirements-spark.txt ./
RUN pip install --no-cache-dir -r requirements-spark.txt

COPY config ./config
COPY src ./src
COPY notebooks ./notebooks

# Resolve the Delta Lake jars at build time so containers start without a Maven download.
RUN python -c "from lakehouse.spark.jobs import get_spark; get_spark().stop()"

VOLUME ["/app/data"]
ENTRYPOINT ["python", "-m", "lakehouse"]
CMD ["--help"]
