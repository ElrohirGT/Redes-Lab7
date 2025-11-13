#!/usr/bin/env python3

from confluent_kafka import Producer
from dotenv import load_dotenv
import random
import math
import os
import json

#  N
# E O
#  S
DIRECTIONS = [
    "N",
    "NO",
    "O",
    "SO",
    "O",
    "SE",
    "E",
    "NE",
]

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
    producer = Producer(config)
    topic = os.getenv("TOPIC")

    timeout = int(os.getenv("TIMEOUT"))

    # Poll for new messages from Kafka and print them.
    def acked(err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))

    try:
        while True:
            obj = {
                "temperatura": random.gauss(25, 10),
                "humedad": math.ceil(random.gauss(25, 10)),
                "direccion_viento": random.choice(DIRECTIONS),
            }
            jsonStr = json.dumps(obj)
            producer.produce(topic, key="sensor1", value=jsonStr, callback=acked)
            # Wait up to 1 second for events. Callbacks will be invoked during
            # this method call if the message is acknowledged.
            producer.poll(timeout)

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print("FATAL ERROR: {}".format(e))
    finally:
        # Leave group and commit final offsets
        producer.close()
