# pull official python image
FROM python:3.12-alpine
#FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONASYNCIODEBUG 1

WORKDIR /app

# COPY . .
COPY app/* .
COPY app/protos protos
COPY app/protos/helium protos/helium
COPY app/protos/helium/iot_config protos/helium/iot_config
# COPY app/Publishers Publishers

COPY requirements.txt .

# RUN pip install --upgrade pip
RUN pip install -r requirements.txt --no-cache-dir

# ENTRYPOINT ["python3", "app.py"]

