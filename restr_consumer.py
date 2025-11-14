#!/usr/bin/env python3

from confluent_kafka import Consumer
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import os
import json
import random
import lib

if __name__ == "__main__":
    load_dotenv()
    config = {
        # User-specific properties that you must set
        "bootstrap.servers": os.getenv("KAFKA_IP"),
        # Fixed properties
        "group.id": "kafka-python-getting-started",
        "auto.offset.reset": "earliest",
    }
    timeout_s = int(os.getenv("TIMEOUT_S"))

    # Create Consumer instance
    consumer = Consumer(config)

    # Subscribe to topic
    topic = os.getenv("TOPIC")
    consumer.subscribe([topic])

    temps = []
    humid = []
    winds = []

    # Initialize figure with 3 subplots
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8))
    fig.suptitle("Live Weather Data", fontsize=14, fontweight="bold")

    # Initialize line objects
    (line1,) = ax1.plot([], [], "r-", linewidth=2, label="Temperature")
    (line2,) = ax2.plot([], [], "b-", linewidth=2, label="Humidity")

    # Direction mapping for visualization
    dir_map = {
        "N": 0,
        "NE": 45,
        "E": 90,
        "SE": 135,
        "S": 180,
        "SO": 225,
        "O": 270,
        "NO": 315,
    }

    def init():
        """Initialize the plot"""
        ax1.set_xlim(0, 50)
        ax1.set_ylim(0, 40)
        ax1.set_ylabel("Temperature (°C)", fontsize=10)
        ax1.legend(loc="upper left")
        ax1.grid(True, alpha=0.3)

        ax2.set_xlim(0, 50)
        ax2.set_ylim(0, 100)
        ax2.set_ylabel("Humidity (%)", fontsize=10)
        ax2.legend(loc="upper left")
        ax2.grid(True, alpha=0.3)

        ax3.set_xlim(0, 50)
        ax3.set_ylim(-45, 360)
        ax3.set_ylabel("Direction (degrees)", fontsize=10)
        ax3.set_xlabel("Time (samples)", fontsize=10)
        ax3.set_yticks([0, 90, 180, 270, 360])
        ax3.set_yticklabels(["N", "E", "S", "W", "N"])
        ax3.grid(True, alpha=0.3)

        return line1, line2

    def update(frame):
        """Update function called for each animation frame"""
        # Simulate adding new data (replace with your actual data source)
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
                # temps.append(random.gauss(25, 10))
                # humid.append(int(random.gauss(25, 10)))
                # winds.append(random.choice(lib.DIRECTIONS))
                break
            elif msg.error():
                print("ERROR: {}".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                msg_key = msg.key().decode("utf-8")
                msg_value = msg.value()
                print(
                    "Consumed event from topic {topic}: key = {key:12} value = {}".format(
                        bytes(msg_value),
                        topic=msg.topic(),
                        key=msg_key,
                    )
                )

                dictObj = lib.decode_msg(msg_value)
                print("Decoded:  {}".format(dictObj), sep="\n")
                temps.append(dictObj["temperatura"])
                humid.append(dictObj["humedad"])
                winds.append(dictObj["direccion_viento"])
        # Get current data length
        n = len(temps)
        x = list(range(n))

        # Update temperature plot
        line1.set_data(x, temps)
        # ax1.set_xlim(max(0, n - 50), n)

        # Update humidity plot
        line2.set_data(x, humid)
        # ax2.set_xlim(max(0, n - 50), n)

        # Update direction plot
        dir_values = [dir_map.get(d, 0) for d in winds]
        ax3.clear()
        ax3.scatter(x, dir_values, c="green", s=30, alpha=0.6)
        # ax3.set_xlim(max(0, n - 50), n)
        ax3.set_ylim(-45, 360)
        ax3.set_ylabel("Direction (degrees)", fontsize=10)
        ax3.set_xlabel("Time (samples)", fontsize=10)
        ax3.set_yticks([0, 90, 180, 270, 360])
        ax3.set_yticklabels(["N", "E", "S", "W", "N"])
        ax3.grid(True, alpha=0.3)

        return line1, line2

    try:
        ani = FuncAnimation(
            fig,
            update,
            init_func=init,
            interval=timeout_s * 1000,
            blit=False,
            cache_frame_data=False,
        )

        plt.tight_layout()
        plt.show()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print("FATAL ERROR: {}", e)
    finally:
        # Leave group and commit final offsets
        consumer.close()
