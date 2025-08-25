import time
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import pandas as pd
from dotenv import load_dotenv
import os

#All configuration details stored in env file. You can add your configs at the same place to run this code.
#Load environment variables
load_dotenv()

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for user record {msg.key().decode('utf_8')}: {err}")
        return
    print(f"User record {msg.key().decode('utf_8')} successfully produced to {msg.topic()} in [{msg.partition()}] at offset {msg.offset()}")
    print("========================")

# Define Kafka Configuration
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP'),
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISM'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD')
}

# Create a Schema Registry Client
schema_registry_client = SchemaRegistryClient({
    'url': os.getenv("SCHEMA_REGISTRY_URL"),
    'basic.auth.user.info': os.getenv("SCHEMA_REGISTRY_BASIC_AUTH")
})

# Setting up serialization context
topic = os.getenv('KAFKA_TOPIC')
key_ctx = SerializationContext(topic,MessageField.KEY)
value_ctx = SerializationContext(topic, MessageField.VALUE)

# Fetch the latest avro schema for the value
subject_name = os.getenv("SCHEMA_SUBJECT_NAME")
schema_format = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("Schema from Registry.....")
print(schema_format)
print("======================")

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_format)

# Create producer
producer = Producer(kafka_config)

#Load CSV data into pandas dataframe
df = pd.read_csv('retail_data.csv')
df = df.fillna('null')

# Iterate over DataFrame and Produce to Kafka
for index, row in df.iterrows():
    # Create a dictionary from row values
    data_value = row.to_dict()
    data_value["CustomerID"] = int(data_value["CustomerID"])
    print(f"Data Value {data_value}")

    #serialized key, value with standered serialization
    serialized_byte_key = key_serializer(str(index), key_ctx)
    serialized_byte_value = avro_serializer(data_value, value_ctx)

    # Produce with pre-serialized data
    producer.produce(
        topic = topic,
        key = serialized_byte_key,
        value = serialized_byte_value,
        callback = delivery_report
    )

    # Keep buffer draining
    producer.poll(0)
    time.sleep(1) # temperrory delay for testing

producer.flush(timeout=30)
print("All Data Successfully Published to Kafka")
