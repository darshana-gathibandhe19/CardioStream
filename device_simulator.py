import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer  # <--- NEW IMPORT

# 1. Initialize Kafka Producer
# connecting to localhost:9092 (where our Docker container is listening)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TOPIC_NAME = 'medtronic_vitals'  # <--- The Topic Name

DEVICE_IDS = [f"D-{str(i).zfill(3)}" for i in range(1, 101)]
PATIENT_IDS = [f"PT-{str(i).zfill(4)}" for i in range(1000, 1100)]

def generate_vital_sign():
    # ... (Same logic as before) ...
    if random.random() < 0.95:
        heart_rate = random.randint(60, 100)
        oxygen_level = random.randint(95, 100)
        status = "Normal"
    else:
        heart_rate = random.randint(150, 200)
        oxygen_level = random.randint(80, 90)
        status = "CRITICAL"

    return {
        "event_id": str(int(time.time() * 1000)),
        "device_id": random.choice(DEVICE_IDS),
        "patient_id": random.choice(PATIENT_IDS),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "heart_rate": heart_rate,
        "oxygen_level": oxygen_level,
        "status": status,
        "battery_level": random.randint(20, 100)
    }

if __name__ == "__main__":
    print("--- 🏥 Starting Medtronic IoT Stream to Kafka ---")
    try:
        while True:
            vital_sign = generate_vital_sign()
            
            # 2. SEND to Kafka
            producer.send(TOPIC_NAME, value=vital_sign)
            
            # Print minimal log to know it's working
            print(f"Sent event: {vital_sign['event_id']} | Status: {vital_sign['status']}")
            
            time.sleep(1) 
            
    except KeyboardInterrupt:
        print("\n🛑 Simulation Stopped.")