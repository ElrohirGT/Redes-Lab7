#!/usr/bin/env python3

from confluent_kafka import Consumer
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import os
import json
import random
import lib
import numpy as np

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
    fig.suptitle("Live Weather Data - Histogramas", fontsize=14, fontweight="bold")

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
        ax1.set_ylabel("Frecuencia", fontsize=10)
        ax1.set_xlabel("Temperatura (°C)", fontsize=10)
        ax1.set_title("Histograma de Temperatura", fontsize=11)
        ax1.grid(True, alpha=0.3, axis="y")

        ax2.set_ylabel("Frecuencia", fontsize=10)
        ax2.set_xlabel("Humedad (%)", fontsize=10)
        ax2.set_title("Histograma de Humedad", fontsize=11)
        ax2.grid(True, alpha=0.3, axis="y")

        ax3.set_ylabel("Frecuencia", fontsize=10)
        ax3.set_xlabel("Dirección del Viento", fontsize=10)
        ax3.set_title("Histograma de Dirección del Viento", fontsize=11)
        ax3.grid(True, alpha=0.3, axis="y")

        return []

    def update(frame):
        """Update function called for each animation frame"""
        # Poll for new messages
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
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
                break

        if len(temps) == 0:
            return []

        # Clear all axes
        ax1.clear()
        ax2.clear()
        ax3.clear()

        # Histograma de Temperatura
        ax1.hist(temps, bins=15, color="red", alpha=0.7, edgecolor="black")
        ax1.set_ylabel("Frecuencia", fontsize=10)
        ax1.set_xlabel("Temperatura (°C)", fontsize=10)
        ax1.set_title(f"Histograma de Temperatura (n={len(temps)})", fontsize=11)
        ax1.grid(True, alpha=0.3, axis="y")

        # Histograma de Humedad
        ax2.hist(humid, bins=15, color="blue", alpha=0.7, edgecolor="black")
        ax2.set_ylabel("Frecuencia", fontsize=10)
        ax2.set_xlabel("Humedad (%)", fontsize=10)
        ax2.set_title(f"Histograma de Humedad (n={len(humid)})", fontsize=11)
        ax2.grid(True, alpha=0.3, axis="y")

        # Histograma de Dirección del Viento
        direction_labels = list(dir_map.keys())
        direction_counts = [winds.count(d) for d in direction_labels]

        bars = ax3.bar(
            direction_labels,
            direction_counts,
            color="green",
            alpha=0.7,
            edgecolor="black",
        )
        ax3.set_ylabel("Frecuencia", fontsize=10)
        ax3.set_xlabel("Dirección del Viento", fontsize=10)
        ax3.set_title(
            f"Histograma de Dirección del Viento (n={len(winds)})", fontsize=11
        )
        ax3.grid(True, alpha=0.3, axis="y")

        # Añadir valores encima de las barras
        for bar, count in zip(bars, direction_counts):
            if count > 0:
                height = bar.get_height()
                ax3.text(
                    bar.get_x() + bar.get_width() / 2.0,
                    height,
                    f"{int(count)}",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                )

        return []

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
