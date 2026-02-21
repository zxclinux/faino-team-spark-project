ARG PYTHON_VERSION=3.8

FROM python:${PYTHON_VERSION}-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN pip --no-cache-dir install pyspark

COPY . .

CMD ["python", "main.py"]
