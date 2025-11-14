from confluent_kafka import Consumer
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import os
import json
import random
import lib

load_dotenv()
config = {
    "bootstrap.servers": os.getenv("KAFKA_IP"),
    "group.id": "kafka-python-getting-started",
    "auto.offset.reset": "earliest",
}
timeout_s = int(os.getenv("TIMEOUT_S"))
SENSOR_KEY = os.getenv("SENSOR_KEY")

# Create Consumer instance
consumer = Consumer(config)

# Subscribe to topic
topic = os.getenv("TOPIC")
consumer.subscribe([topic])

temps = []
humid = []
winds = []

fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8))
fig.suptitle("Live Weather Data - Histograms", fontsize=14, fontweight="bold")


def init():
    """Initialize the plot"""
    ax1.set_ylabel("Frequency", fontsize=10)
    ax1.set_xlabel("Temperature (°C)", fontsize=10)
    ax1.set_title("Temperature Distribution")
    ax1.grid(True, alpha=0.3, axis='y')

    ax2.set_ylabel("Frequency", fontsize=10)
    ax2.set_xlabel("Humidity (%)", fontsize=10)
    ax2.set_title("Humidity Distribution")
    ax2.grid(True, alpha=0.3, axis='y')

    ax3.set_ylabel("Frequency", fontsize=10)
    ax3.set_xlabel("Wind Direction", fontsize=10)
    ax3.set_title("Wind Direction Distribution")
    ax3.grid(True, alpha=0.3, axis='y')

    return []

def update(frame):
    """Update function called for each animation frame"""
    # Simulate adding new data (replace with your actual data source)
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
            
            # If message is not from the expected sensor skip it
            if msg_key != SENSOR_KEY:
                break
            
            msg_value = msg.value()
            print(msg_value)
            print(
                "Consumed event from topic {topic}: key = {key:12}".format(
                    topic=msg.topic(), key=msg_key
                )
            )

            dictObj = json.loads(msg_value)
            print("Decoded:  {}".format(dictObj), sep="\n")
            temps.append(dictObj["temperatura"])
            humid.append(dictObj["humedad"])
            winds.append(dictObj["direccion_viento"])
    
    # Check if we have data to plot
    if not temps:
        return []
    
    # Clear all axes
    ax1.clear()
    ax2.clear()
    ax3.clear()
    
    # Plot temperature histogram
    ax1.hist(temps, bins=20, color='red', alpha=0.7, edgecolor='black')
    ax1.set_ylabel("Frequency", fontsize=10)
    ax1.set_xlabel("Temperature (°C)", fontsize=10)
    ax1.set_title(f"Temperature Distribution (n={len(temps)})")
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Plot humidity histogram
    ax2.hist(humid, bins=20, color='blue', alpha=0.7, edgecolor='black')
    ax2.set_ylabel("Frequency", fontsize=10)
    ax2.set_xlabel("Humidity (%)", fontsize=10)
    ax2.set_title(f"Humidity Distribution (n={len(humid)})")
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Plot wind direction histogram (categorical)
    from collections import Counter
    wind_counts = Counter(winds)
    directions = list(wind_counts.keys())
    counts = list(wind_counts.values())
    
    ax3.bar(directions, counts, color='green', alpha=0.7, edgecolor='black')
    ax3.set_ylabel("Frequency", fontsize=10)
    ax3.set_xlabel("Wind Direction", fontsize=10)
    ax3.set_title(f"Wind Direction Distribution (n={len(winds)})")
    ax3.grid(True, alpha=0.3, axis='y')
    
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