#!/usr/bin/env python3

import random
import math
import os
import json
from confluent_kafka import Producer
from dotenv import load_dotenv
from lib import encode_msg, decode_msg, DIRECTIONS


if __name__ == "__main__":
    load_dotenv()

    config = {
        # User-specific properties that you must set
        "bootstrap.servers": os.getenv("KAFKA_IP"),
    }

    # Create Consumer instance
    producer = Producer(config)
    topic = os.getenv("TOPIC")

    timeout = int(os.getenv("TIMEOUT_S"))

    # Poll for new messages from Kafka and print them.
    def acked(err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            print("Message produced: %s" % (str(msg)))

    try:
        print("Starting to generate data...")
        while True:
            obj = {
                "temperatura": abs(random.gauss(25, 10)),
                "humedad": abs(math.ceil(random.gauss(25, 10))),
                "direccion_viento": random.choice(DIRECTIONS),
            }
            encoded = encode_msg(obj)
            decoded = decode_msg(encoded)

            print("Original: {}".format(obj), "Encoded:  {}".format(decoded), sep="\n")
            print("=" * 20 * 2)
            # producer.produce(topic, key="sensor1", value=encoded, callback=acked)
            # Wait up to 1 second for events. Callbacks will be invoked during
            # this method call if the message is acknowledged.
            # producer.poll(timeout)

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print("FATAL ERROR: {}".format(e))
    finally:
        # Leave group and commit final offsets
        producer.flush()
