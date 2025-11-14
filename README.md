# Lab7 - IoT

The consumer is written in python since we need to graph some stuff to the
screen and what better way to do it than using matplotlib and stuff.

## Presentation

A compilation of the results and answers to questions are here: 
[**🔴 Google Slides**](https://docs.google.com/presentation/d/14oB6-YKvWJinkpf4ZpSGYrpNvtvgL99FSCNfC8JVfZI/edit?usp=sharing)

## Dependencies
You can install the dependencies using

```bash
pip install -r requirements.txt
```
or
```bash
nix develop # If you have nix installed ha, ha, ha
```
FINALLY, create an `.env` file with this values:
```
KAFKA_IP=147.182.219.133    # server ip
TOPIC=<Your Unique Code>    # topic id
TIMEOUT_S=15                # in seconds
SENSOR_KEY=sensor2          # kafka key
```

## With JSON
Once you installed the dependencies in two separate terminals run the producer and consumer

```bash
./producer.py
./consumer.py
```

## With restrictions
As the previous section, run in 2 differente terminals these commands:
```bash
./restr_producer.py
./restr_consumer.py
```
