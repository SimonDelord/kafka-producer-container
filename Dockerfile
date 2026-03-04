FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy producer script
COPY producer.py .

# Configuration defaults (override at deployment)
ENV KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap:9092
ENV KAFKA_TOPIC=truck-telemetry
ENV MESSAGE_INTERVAL_SECONDS=1.0
ENV TRUCK_ID=TRK-001
ENV TRUCK_NUMBER=1
ENV FLEET_ID=BHP-WA-001
ENV FIRMWARE_VERSION=2.4.1-build.2847

CMD ["python", "producer.py"]
