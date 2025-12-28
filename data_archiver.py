import json
import os
import time
from kafka import KafkaConsumer
from datetime import datetime

# 1. Configuration
TOPIC_NAME = 'medtronic_vitals'
BATCH_SIZE = 10  # Save to file every 10 records
DATA_LAKE_PATH = "./med_data_lake/raw" # Acts as our S3 bucket

print("--- 💾 Starting Data Lake Archiver ---")
print(f"Saving data to: {DATA_LAKE_PATH}")

# Ensure the folder exists
os.makedirs(DATA_LAKE_PATH, exist_ok=True)

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', # Catch up on missed data
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='archival-group' # Important: Different group than the Alert system!
)

buffer = []

for message in consumer:
    data = message.value
    buffer.append(data)
    
    # 2. Check if Buffer is full (Batch Processing)
    if len(buffer) >= BATCH_SIZE:
        # Generate a unique filename based on time
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{DATA_LAKE_PATH}/vitals_batch_{timestamp}.json"
        
        # Write to file (Simulating S3 Upload)
        with open(filename, 'w') as f:
            json.dump(buffer, f, indent=4)
            
        print(f"✅ Archived batch of {len(buffer)} records to {filename}")
        
        # Clear buffer
        buffer = []