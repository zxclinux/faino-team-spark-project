ARG PYTHON_VERSION=3.8

FROM python:${PYTHON_VERSION}-slim-bullseye
ARG PYSPARK_VERSION=3.5.6

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip --no-cache-dir install -r requirements.txt \
    && pip --no-cache-dir install pyspark==${PYSPARK_VERSION}

COPY . .

CMD ["python", "main.py"]
