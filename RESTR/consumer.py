#!/usr/bin/env python3

from confluent_kafka import Consumer
from dotenv import load_dotenv
import os
import json

if __name__ == "__main__":
    load_dotenv()
    config = {
        # User-specific properties that you must set
        "bootstrap.servers": os.getenv("KAFKA_IP"),
        # Fixed properties
        "group.id": "kafka-python-getting-started",
        "auto.offset.reset": "earliest",
    }

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = os.getenv("TOPIC")
    consumer.subscribe([topic])

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: {}".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                msg_key = msg.key().decode("utf-8")
                msg_value = msg.value().decode("utf-8")
                print(
                    "Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                        topic=msg.topic(), key=msg_key, value=msg_value
                    )
                )

                jsonObj = json.loads(msg_value)

                temperature = jsonObj["temperatura"]
                humidity = jsonObj["humedad"]
                wind = jsonObj["direccion_viento"]

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
