from confluent_kafka import Producer
from dotenv import load_dotenv
import random
import math
import os
import json
import lib
import time


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
        print(f"Message produced: {str(msg)}")

try:
    print("Starting to generate data...")
    while True:
        obj = lib.NewDataRow(
            temp=abs(random.gauss(25, 10)),
            humidity=abs(math.ceil(random.gauss(25, 10))),
            wind=random.choice(lib.DIRECTIONS),
        )
        encoded = json.dumps(obj)
        print(encoded)
        producer.produce(topic, key="sensor2", value=encoded, callback=acked)
        # Wait up to 1 second for events. Callbacks will be invoked during
        # this method call if the message is acknowledged.
        producer.poll(5)
        time.sleep(timeout) # Sleep for 15 seconds until new data

except KeyboardInterrupt:
    pass
except Exception as e:
    print("FATAL ERROR: {}".format(e))
finally:
    # Leave group and commit final offsets
    producer.flush()
