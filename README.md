# Truck Telemetry Kafka Producer

A containerized Kafka producer that generates realistic mining truck telemetry data and sends it to a Kafka topic at a configurable rate.

## Overview

This producer simulates telemetry data from mining haul trucks, generating comprehensive JSON payloads that include:

- **Identification** - Truck ID, model, firmware version, fleet information
- **Location** - GPS coordinates, speed, heading, altitude
- **Engine Metrics** - RPM, temperature, oil pressure, fuel level
- **Payload** - Load weight, status (empty/loading/loaded/dumping)
- **Brakes** - Temperature, pressure, wear levels, retarder status
- **Hydraulics** - Pressure, temperature, fluid levels
- **Electrical** - Battery voltage, alternator output
- **Tyres** - Pressure and temperature for all 6 tyres
- **Safety** - Operator status, seatbelt, fatigue level, emergency systems
- **Proximity** - Nearest vehicle distance, collision warnings
- **Zone** - Current operational zone, speed limits
- **Operations** - Odometer, shift info, efficiency metrics
- **Maintenance** - Service hours, fault codes, inspection status

## Quick Start

### Build the Container Image

```bash
# Using Docker
docker build -t truck-kafka-producer:latest .

# Using Podman
podman build -t truck-kafka-producer:latest .
```

### Run Locally

```bash
docker run -e KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
           -e KAFKA_TOPIC=truck-telemetry \
           -e TRUCK_ID=TRK-001 \
           truck-kafka-producer:latest
```

### Deploy on Kubernetes/OpenShift

```bash
# Edit kubernetes/deployment.yaml to configure your Kafka server and topic
kubectl apply -f kubernetes/deployment.yaml
```

## Configuration

All configuration is done via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `my-cluster-kafka-bootstrap:9092` | Kafka broker address(es), comma-separated for multiple |
| `KAFKA_TOPIC` | `truck-telemetry` | Target Kafka topic name |
| `MESSAGE_INTERVAL_SECONDS` | `1.0` | Time between messages (seconds) |
| `TRUCK_ID` | `TRK-001` | Truck identifier (e.g., TRK-001 to TRK-010) |
| `TRUCK_NUMBER` | `1` | Numeric truck number (1-10), affects telemetry profiles |
| `FLEET_ID` | `BHP-WA-001` | Fleet identifier |
| `FIRMWARE_VERSION` | `2.4.1-build.2847` | Simulated firmware version |

## Truck Profiles

The producer includes 10 pre-configured truck profiles with unique characteristics:

| Truck # | ID | Zone | Model | Firmware |
|---------|-----|------|-------|----------|
| 1 | TRK-001 | Main Pit North | Caterpillar 797F | 2.4.1 |
| 2 | TRK-002 | Main Pit North | Caterpillar 797F | 2.4.1 |
| 3 | TRK-003 | Main Pit South | Komatsu 930E-5 | 2.4.1 |
| 4 | TRK-004 | Main Pit South | Komatsu 930E-5 | 2.4.1 |
| 5 | TRK-005 | North Haul Road | Liebherr T 284 | 3.0.0 |
| 6 | TRK-006 | North Haul Road | Liebherr T 284 | 3.0.0 |
| 7 | TRK-007 | Waste Dump Alpha | Caterpillar 797F | 3.0.0 |
| 8 | TRK-008 | Waste Dump Alpha | Hitachi EH5000AC-3 | 3.1.2 |
| 9 | TRK-009 | Loading Bay East | Komatsu 980E-5 | 3.1.2 |
| 10 | TRK-010 | Loading Bay East | Caterpillar 793F | 3.1.2 |

## Sample Output

Each message is a JSON payload with the following structure:

```json
{
  "identification": {
    "truck_id": "TRK-001",
    "asset_number": "BHP-TRK-001",
    "model": "Caterpillar 797F",
    "fleet_id": "BHP-WA-001",
    "firmware_version": "2.4.1-build.2847"
  },
  "location": {
    "latitude": -23.3601,
    "longitude": 119.7310,
    "altitude": 455.2,
    "heading": 126.5,
    "speed": 38.4,
    "timestamp": "2024-03-04T00:21:52.592Z"
  },
  "engine": {
    "engine_rpm": 1650,
    "engine_temp": 91.5,
    "fuel_level": 68.2,
    "ignition_on": true
  },
  "payload": {
    "payload_weight": 312.5,
    "load_status": "loaded",
    "cycle_count": 7
  },
  "safety": {
    "operator_id": "OP-1001",
    "seatbelt_fastened": true,
    "collision_warning_active": false
  },
  "proximity": {
    "nearest_vehicle_id": "TRK-003",
    "nearest_vehicle_distance": 45.2,
    "vehicles_in_range": 3
  },
  "_metadata": {
    "generated_at": "2024-03-04T00:21:52.592Z",
    "truck_number": 1,
    "producer_version": "1.0.0"
  }
}
```

*Note: Output shown is abbreviated. Full payload includes all subsystems.*

## OpenShift Deployment

### Build on OpenShift

```bash
# Create namespace
oc new-project kafka-producer

# Create build config
oc new-build --name=truck-kafka-producer --binary --strategy=docker

# Build the image
oc start-build truck-kafka-producer --from-dir=. --follow

# Deploy (edit deployment.yaml first with your Kafka details)
oc apply -f kubernetes/deployment.yaml
```

### Cross-Namespace Kafka Access

When Kafka is in a different namespace, use the fully qualified service name:

```yaml
env:
  - name: KAFKA_BOOTSTRAP_SERVERS
    value: "my-cluster-kafka-bootstrap.kafka-namespace.svc:9092"
```

## Running Multiple Producers

To simulate multiple trucks, deploy multiple instances with different configurations:

```bash
# Deploy truck 1
kubectl create deployment truck-01 --image=truck-kafka-producer:latest \
  -- env TRUCK_ID=TRK-001 TRUCK_NUMBER=1

# Deploy truck 2  
kubectl create deployment truck-02 --image=truck-kafka-producer:latest \
  -- env TRUCK_ID=TRK-002 TRUCK_NUMBER=2
```

Or use a single deployment with multiple replicas and unique configurations via a ConfigMap or Helm values.

## Project Structure

```
kafka-producer-container/
├── Dockerfile              # Container image definition
├── producer.py             # Main producer application
├── requirements.txt        # Python dependencies
├── kubernetes/
│   └── deployment.yaml     # Kubernetes/OpenShift deployment manifest
└── README.md               # This file
```

## Dependencies

- Python 3.11+
- kafka-python 2.0.2

## License

MIT License
