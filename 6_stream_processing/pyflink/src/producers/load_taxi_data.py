import csv
import json
from kafka import KafkaProducer

def main():
    """Load taxi data from CSV and stream to Kafka for real-time processing"""
    # Create a Kafka producer
    # Set up streaming data producer with JSON serialization for taxi data
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Convert taxi records to JSON bytes
    )

    csv_file = 'data/green_tripdata_2019-10.csv'  # change to your CSV file path if needed

    # Read taxi trip data from CSV file for streaming
    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        # Stream each taxi trip record to Kafka topic
        for row in reader:
            # Each row will be a dictionary keyed by the CSV headers
            # Send data to Kafka topic "green-data"
            producer.send('green-data', value=row)  # Stream taxi trip record to Kafka

    # Make sure any remaining messages are delivered
    # Ensure all taxi records are sent before finishing
    producer.flush()
    producer.close()


if __name__ == "__main__":
    # Start streaming taxi data to Kafka
    main()