# pull official base image.
FROM python:3.11.4-slim-bullseye
# Do not generate .pyc files.
ENV PYTHONDONTWRITEBYTECODE 1
# Turn off buffering for easier container logging.
ENV PYTHONUNBUFFERED 1

WORKDIR /app
# COPY bin/helium-config-service-cli /usr/local/bin/hpr
COPY app/* .
COPY app/protos protos
COPY app/protos/helium protos/helium
COPY app/protos/helium/iot_config protos/helium/iot_config
COPY app/Publishers Publishers

COPY requirements.txt .
RUN pip install -r requirements.txt

ENTRYPOINT ["python3", "app.py"]
#RUN pip install --no-cache-dir -r requirements.txt
