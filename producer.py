"""
Truck Telemetry Kafka Producer

Generates randomized truck telemetry JSON payloads and sends them to Kafka.
One message per second by default.
"""

import json
import os
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'my-cluster-kafka-bootstrap:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'truck-telemetry')
MESSAGE_INTERVAL_SECONDS = float(os.getenv('MESSAGE_INTERVAL_SECONDS', '1.0'))
TRUCK_ID = os.getenv('TRUCK_ID', 'TRK-001')
TRUCK_NUMBER = int(os.getenv('TRUCK_NUMBER', '1'))
FLEET_ID = os.getenv('FLEET_ID', 'BHP-WA-001')
FIRMWARE_VERSION = os.getenv('FIRMWARE_VERSION', '2.4.1-build.2847')

# Truck models
TRUCK_MODELS = [
    "Caterpillar 797F",
    "Caterpillar 793F", 
    "Komatsu 980E-5",
    "Komatsu 930E-5",
    "Liebherr T 284",
    "Hitachi EH5000AC-3"
]

LOAD_STATUSES = ["empty", "loading", "loaded", "dumping"]
OPERATING_MODES = ["manual", "autonomous", "remote", "maintenance", "standby"]
TRAY_POSITIONS = ["lowered", "raising", "raised", "lowering"]
ZONE_TYPES = ["pit", "haul_road", "dump", "stockpile", "workshop"]

# Truck profiles for different truck numbers
TRUCK_PROFILES = {
    1: {"zone": "PIT-01", "zone_name": "Main Pit North", "model": "Caterpillar 797F", "base_lat": -23.3601, "base_lon": 119.7310},
    2: {"zone": "PIT-01", "zone_name": "Main Pit North", "model": "Caterpillar 797F", "base_lat": -23.3615, "base_lon": 119.7325},
    3: {"zone": "PIT-02", "zone_name": "Main Pit South", "model": "Komatsu 930E-5", "base_lat": -23.3680, "base_lon": 119.7290},
    4: {"zone": "PIT-02", "zone_name": "Main Pit South", "model": "Komatsu 930E-5", "base_lat": -23.3695, "base_lon": 119.7305},
    5: {"zone": "HAUL-01", "zone_name": "North Haul Road", "model": "Liebherr T 284", "base_lat": -23.3520, "base_lon": 119.7450},
    6: {"zone": "HAUL-01", "zone_name": "North Haul Road", "model": "Liebherr T 284", "base_lat": -23.3535, "base_lon": 119.7465},
    7: {"zone": "DUMP-01", "zone_name": "Waste Dump Alpha", "model": "Caterpillar 797F", "base_lat": -23.3400, "base_lon": 119.7600},
    8: {"zone": "DUMP-01", "zone_name": "Waste Dump Alpha", "model": "Hitachi EH5000AC-3", "base_lat": -23.3415, "base_lon": 119.7615},
    9: {"zone": "LOAD-01", "zone_name": "Loading Bay East", "model": "Komatsu 980E-5", "base_lat": -23.3750, "base_lon": 119.7200},
    10: {"zone": "LOAD-01", "zone_name": "Loading Bay East", "model": "Caterpillar 793F", "base_lat": -23.3765, "base_lon": 119.7215},
}


def add_variation(base_value: float, variance_percent: float = 5.0) -> float:
    """Add realistic variation to a base value."""
    variance = base_value * (variance_percent / 100.0)
    return round(base_value + random.uniform(-variance, variance), 2)


def get_profile(truck_num: int) -> dict:
    """Get truck profile, with fallback to profile 1."""
    return TRUCK_PROFILES.get(truck_num, TRUCK_PROFILES[1])


def generate_truck_telemetry() -> dict:
    """Generate a complete truck telemetry payload with randomized values."""
    
    profile = get_profile(TRUCK_NUMBER)
    load_status = random.choice(LOAD_STATUSES)
    
    # Calculate payload based on load status
    if load_status == "loaded":
        payload_weight = add_variation(300.0 + (TRUCK_NUMBER * 10), 5.0)
    elif load_status == "loading":
        payload_weight = add_variation(150.0 + (TRUCK_NUMBER * 5), 10.0)
    elif load_status == "dumping":
        payload_weight = add_variation(50.0, 20.0)
    else:
        payload_weight = 0.0

    # Generate nearby trucks (excluding self)
    nearby_trucks = [f"TRK-{str(i).zfill(3)}" for i in range(1, 11) if i != TRUCK_NUMBER]
    nearest_distance = add_variation(25.0 + (TRUCK_NUMBER * 3), 30.0)
    
    telemetry = {
        "identification": {
            "truck_id": TRUCK_ID,
            "asset_number": f"BHP-{TRUCK_ID}",
            "vin": f"1HGBH41JXMN{100000 + TRUCK_NUMBER}",
            "model": profile["model"],
            "fleet_id": FLEET_ID,
            "site_id": "SITE-WA-001",
            "firmware_version": FIRMWARE_VERSION,
            "hardware_version": "1.2.0",
            "registration_date": "2024-01-15T00:00:00"
        },
        "location": {
            "latitude": add_variation(profile["base_lat"], 0.01),
            "longitude": add_variation(profile["base_lon"], 0.01),
            "altitude": add_variation(450.0 + (TRUCK_NUMBER * 5), 2.0),
            "heading": add_variation(90.0 + (TRUCK_NUMBER * 36), 10.0) % 360,
            "speed": add_variation(35.0 + (TRUCK_NUMBER % 3) * 5, 15.0),
            "accuracy": round(random.uniform(0.5, 2.0), 2),
            "timestamp": datetime.utcnow().isoformat(),
            "gps_quality": random.choice([4, 5, 5, 5]),
            "satellites_visible": random.randint(8, 14)
        },
        "engine": {
            "engine_hours": 12500.0 + (TRUCK_NUMBER * 850),
            "engine_rpm": int(add_variation(1600 + (TRUCK_NUMBER * 50), 8.0)),
            "engine_temp": add_variation(90.0 + (TRUCK_NUMBER % 3) * 2, 5.0),
            "oil_pressure": add_variation(440.0 + (TRUCK_NUMBER * 5), 3.0),
            "oil_temp": add_variation(95.0, 5.0),
            "coolant_temp": add_variation(85.0 + (TRUCK_NUMBER % 4) * 2, 4.0),
            "transmission_temp": add_variation(80.0, 5.0),
            "fuel_level": max(15.0, min(95.0, add_variation(70.0 - (TRUCK_NUMBER * 4), 10.0))),
            "fuel_consumption_rate": add_variation(180.0, 15.0),
            "throttle_position": add_variation(45.0, 30.0),
            "def_level": max(20.0, min(95.0, add_variation(80.0 - (TRUCK_NUMBER * 3), 8.0))),
            "ignition_on": True
        },
        "payload": {
            "payload_weight": payload_weight,
            "max_payload": 400.0,
            "load_status": load_status,
            "tray_position": random.choice(TRAY_POSITIONS),
            "cycle_count": 5 + (TRUCK_NUMBER * 2),
            "total_tonnes_hauled": (5 + (TRUCK_NUMBER * 2)) * 310.0
        },
        "brakes": {
            "brake_temp_front": add_variation(120.0, 15.0),
            "brake_temp_rear": add_variation(115.0, 15.0),
            "retarder_active": TRUCK_NUMBER % 3 == 0,
            "retarder_temp": add_variation(180.0 if TRUCK_NUMBER % 3 == 0 else 45.0, 10.0),
            "brake_wear_front": add_variation(75.0 - (TRUCK_NUMBER * 3), 10.0),
            "brake_wear_rear": add_variation(70.0 - (TRUCK_NUMBER * 3), 10.0),
            "parking_brake_engaged": False,
            "emergency_brake_active": False,
            "brake_pressure_front": add_variation(2800.0, 3.0),
            "brake_pressure_rear": add_variation(2750.0, 3.0)
        },
        "hydraulics": {
            "hydraulic_pressure": add_variation(3200.0, 2.0),
            "hydraulic_temp": add_variation(65.0 + (TRUCK_NUMBER % 4) * 3, 5.0),
            "hydraulic_fluid_level": add_variation(92.0, 3.0),
            "steering_pressure": add_variation(2400.0, 3.0),
            "tray_angle": random.choice([0.0, 25.0, 55.0, 30.0])
        },
        "electrical": {
            "battery_voltage": add_variation(24.2 + (TRUCK_NUMBER % 3) * 0.2, 2.0),
            "alternator_output": add_variation(28.5, 1.5),
            "main_power_on": True,
            "auxiliary_power_on": True,
            "communication_status": True,
            "electrical_load": add_variation(45.0 + (TRUCK_NUMBER * 2), 10.0)
        },
        "tyres": [
            {
                "position": pos,
                "pressure": add_variation(700.0 + (i * 5), 2.0),
                "temperature": add_variation(55.0 + (TRUCK_NUMBER % 3) * 3, 8.0),
                "wear_percentage": add_variation(80.0, 10.0),
                "last_checked": datetime.utcnow().isoformat()
            }
            for i, pos in enumerate([
                "front_left", "front_right",
                "rear_inner_left", "rear_inner_right",
                "rear_outer_left", "rear_outer_right"
            ])
        ],
        "safety": {
            "seatbelt_fastened": True,
            "operator_id": f"OP-{1001 + ((TRUCK_NUMBER - 1) % 5)}",
            "operator_logged_in": True,
            "fatigue_score": random.choice([95.0, 98.0, 100.0, 92.0, 88.0]),
            "fatigue_level": random.choice([0, 0, 0, 1, 1, 2]),
            "emergency_stop_active": False,
            "horn_active": False,
            "lights_on": True,
            "beacon_active": True,
            "fire_suppression_armed": True,
            "door_closed": True,
            "horn_functional": True,
            "emergency_stop_status": False
        },
        "proximity": {
            "proximity_system_active": True,
            "nearest_vehicle_id": random.choice(nearby_trucks),
            "nearest_vehicle_distance": nearest_distance,
            "nearest_vehicle_bearing": add_variation(180.0, 50.0) % 360,
            "collision_warning_active": nearest_distance < 10.0,
            "collision_warning_level": "warning" if nearest_distance < 10.0 else "info",
            "vehicles_in_range": random.randint(1, 4),
            "zone_violations": [],
            "last_proximity_scan": datetime.utcnow().isoformat()
        },
        "zone": {
            "current_zone_id": f"ZONE-{profile['zone']}",
            "current_zone_type": profile["zone"].split("-")[0].lower(),
            "current_zone_name": profile["zone_name"],
            "speed_limit": {"pit": 25.0, "haul": 45.0, "dump": 15.0, "load": 10.0}.get(
                profile["zone"].split("-")[0].lower(), 30.0
            ),
            "authorized_for_zone": True,
            "time_in_zone": random.randint(60, 3600),
            "in_restricted_area": False
        },
        "operations": {
            "odometer": 100000.0 + (TRUCK_NUMBER * 15000),
            "trip_distance": add_variation(25.0, 30.0),
            "operating_mode": "manual",
            "shift_id": f"SHIFT-{datetime.now().strftime('%Y%m%d')}-{((TRUCK_NUMBER - 1) // 4) + 1}",
            "shift_start_time": datetime.utcnow().replace(hour=6, minute=0, second=0).isoformat(),
            "total_idle_time": random.randint(300, 1800),
            "total_moving_time": random.randint(3600, 14400),
            "efficiency_score": add_variation(85.0, 10.0),
            "current_task": {
                "empty": "Returning to loader",
                "loading": "Being loaded",
                "loaded": "Hauling to dump",
                "dumping": "Dumping load"
            }.get(load_status, "Unknown")
        },
        "maintenance": {
            "last_service_date": "2024-02-01T00:00:00",
            "last_service_hours": 12000.0,
            "next_service_due_hours": 13000.0,
            "hours_until_service": 500 - (TRUCK_NUMBER * 30),
            "active_fault_codes": [],
            "warning_lights": [],
            "maintenance_mode": False,
            "last_inspection": datetime.utcnow().replace(
                day=max(1, datetime.now().day - TRUCK_NUMBER)
            ).isoformat(),
            "tyre_hours_remaining": [
                1500 - (TRUCK_NUMBER * 50),
                1520 - (TRUCK_NUMBER * 50),
                1480 - (TRUCK_NUMBER * 50),
                1510 - (TRUCK_NUMBER * 50),
                1490 - (TRUCK_NUMBER * 50),
                1500 - (TRUCK_NUMBER * 50)
            ],
            "next_service_hours": 500 - (TRUCK_NUMBER * 30)
        },
        "last_updated": datetime.utcnow().isoformat(),
        "data_quality_score": random.choice([98.0, 99.0, 100.0, 97.0]),
        "_metadata": {
            "generated_at": datetime.utcnow().isoformat(),
            "truck_number": TRUCK_NUMBER,
            "producer_version": "1.0.0",
            "source": "truck-kafka-producer"
        }
    }
    
    return telemetry


def create_producer() -> KafkaProducer:
    """Create and return a Kafka producer instance."""
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3,
        retry_backoff_ms=500
    )
    
    return producer


def main():
    """Main producer loop."""
    logger.info(f"Starting Truck Kafka Producer")
    logger.info(f"  Truck ID: {TRUCK_ID}")
    logger.info(f"  Truck Number: {TRUCK_NUMBER}")
    logger.info(f"  Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Topic: {KAFKA_TOPIC}")
    logger.info(f"  Message Interval: {MESSAGE_INTERVAL_SECONDS}s")
    
    producer = None
    message_count = 0
    
    while True:
        try:
            if producer is None:
                producer = create_producer()
                logger.info("Connected to Kafka successfully")
            
            telemetry = generate_truck_telemetry()
            
            future = producer.send(
                KAFKA_TOPIC,
                key=TRUCK_ID,
                value=telemetry
            )
            
            record_metadata = future.get(timeout=10)
            message_count += 1
            
            if message_count % 10 == 0:
                logger.info(
                    f"Sent {message_count} messages. "
                    f"Latest: partition={record_metadata.partition}, "
                    f"offset={record_metadata.offset}"
                )
            
            time.sleep(MESSAGE_INTERVAL_SECONDS)
            
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            if producer:
                try:
                    producer.close()
                except:
                    pass
                producer = None
            time.sleep(5)


if __name__ == "__main__":
    main()
