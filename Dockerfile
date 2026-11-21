ARG PYTHON_VERSION=3.8

FROM python:${PYTHON_VERSION}-slim-bullseye
ARG PYSPARK_VERSION=3.5.6

# bullseye is EOL and its packages moved to archive.debian.org
RUN sed -i 's|deb.debian.org/debian-security|archive.debian.org/debian-security|g; s|deb.debian.org/debian|archive.debian.org/debian|g' \
        /etc/apt/sources.list $(ls /etc/apt/sources.list.d/* 2>/dev/null) \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# print() output shows up in logs immediately
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip --no-cache-dir install -r requirements.txt \
    && pip --no-cache-dir install pyspark==${PYSPARK_VERSION}

COPY . .

CMD ["python", "main.py"]
