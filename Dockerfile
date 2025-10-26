FROM apache/airflow:2.8.1

# Paigaldame git root'ina
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && rm -rf /var/lib/apt/lists/*

# Tagasi 'airflow' kasutajale enne pip'i
USER airflow

# Paigaldame Python'i sõltuvused
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
