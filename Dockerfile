# pull official python image
FROM python:3.13-alpine

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONASYNCIODEBUG=1

WORKDIR /app

COPY app/* .
COPY app/protos protos
COPY app/protos/helium protos/helium
COPY app/protos/helium/iot_config protos/helium/iot_config

COPY requirements.txt .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt --no-cache-dir

# ENTRYPOINT ["python3", "app.py"]
