import json
import time
from kafka import KafkaProducer

def json_serializer(data):
    """Convert Python data to JSON bytes for streaming transmission"""
    return json.dumps(data).encode('utf-8')

# Kafka broker connection for streaming data pipeline
server = 'localhost:9092'

# Create streaming data producer with JSON serialization
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer  # Convert Python objects to JSON for streaming
)
# Track streaming performance
t0 = time.time()

# Kafka topic where streaming messages will be sent
topic_name = 'test-topic'

# Generate and stream test data continuously
for i in range(10, 11):
    # Create streaming message with test data and timestamp
    message = {'test_data': i, 'event_timestamp': time.time() * 1000}
    producer.send(topic_name, value=message)  # Send message to streaming topic
    print(f"Sent: {message}")
    time.sleep(0.05)  # Simulate real-time data arrival rate

# Ensure all streaming messages are delivered before finishing
producer.flush()

# Calculate total streaming time
t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')