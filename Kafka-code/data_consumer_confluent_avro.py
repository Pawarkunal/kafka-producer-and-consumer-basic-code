from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer, SerializationContext, MessageField
from dotenv import load_dotenv
import os

#Load environment variables
load_dotenv()

#Define Kafka Configuration
kafka_config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP"),
    'sasl.mechanisms': os.getenv("KAFKA_SASL_MECHANISM"),
    'security.protocol': os.getenv("KAFKA_SECURITY_PROTOCOL"),
    'sasl.username': os.getenv("KAFKA_SASL_USERNAME"),
    'sasl.password': os.getenv("KAFKA_SASL_PASSWORD"),
    'group.id': os.getenv("KAFKA_GROUP_ID", "G1"),
    'auto.offset.reset': os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
}

# Create Schem registry Client
schema_registry_client = SchemaRegistryClient({
    'url': os.getenv("SCHEMA_REGISTRY_URL"),
    'basic.auth.user.info': os.getenv("SCHEMA_REGISTRY_BASIC_AUTH")
})

# Fetch the latest avro schema for the value
subject_name = os.getenv("SCHEMA_SUBJECT_NAME")
schema_format = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_format)

# topic to subscribe
topic = os.getenv("KAFKA_TOPIC")

# Define the DeserializingConsumer
consumer = Consumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to topic
consumer.subscribe([topic])

# Contineously read messages from Kafka
try:
    while True:
        msg = consumer.poll(1)
        
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Manual Deserialization
        byte_key = msg.key()
        byte_value = msg.value()

        key = key_deserializer(byte_key)
        try:
            value = avro_deserializer(
                byte_value,
                SerializationContext(topic, MessageField.VALUE)
            )
        except Exception as e:
            print(f"Value Deserialization error at offset {msg.offset()}: {e}")
            continue

        print(f"Successfully consumed record from partition {msg.partition()} and offset {msg.offset()}")
        print(f"Key: {key} and Value: {value}")
        print("=====================")
        
except KeyboardInterrupt:
    pass
finally:
    consumer.close()      
