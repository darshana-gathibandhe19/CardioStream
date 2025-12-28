import json
from kafka import KafkaConsumer

# 1. Configuration
TOPIC_NAME = 'medtronic_vitals'

print("--- 🩺 Starting Real-Time Anomaly Detector ---")
print("Listening for CRITICAL events...")

# 2. Connect to the Stream
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',       # Start reading from NOW, ignore old history
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) # Unpack the JSON
)

# 3. Process the Stream (The "Spark" Logic)
try:
    for message in consumer:
        data = message.value
        
        # The Logic: We only care if status is CRITICAL
        if data['status'] == "CRITICAL":
            print("\n" + "!"*50)
            print(f"🚨 URGENT ALERT DETECTED!")
            print(f"Patient: {data['patient_id']}")
            print(f"Device:  {data['device_id']}")
            print(f"Vitals:  HR={data['heart_rate']} bpm, O2={data['oxygen_level']}%")
            print(f"Time:    {data['timestamp']}")
            print("!"*50 + "\n")
            
            # (In a real project, here is where you would add:
            #  sns.publish_message() -> Send SMS to Doctor
            #  db.save_incident()    -> Log to Database)

except KeyboardInterrupt:
    print("\n🛑 Monitoring Stopped.")