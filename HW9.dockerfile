# Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY . .

RUN pip install kafka-python pymongo

CMD ["python", "producer.py"]  # or "consumer.py"
