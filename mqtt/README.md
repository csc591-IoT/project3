# README FOR MQTT

## Install Dependencies

Make sure you install the required dependencies:

```bash
pip install paho-mqtt
pip install python-dotenv
```

## Environment Configuration

Create a `.env` file in the same directory as your script with the necessary configuration variables:

```bash
MQTT_BROKER_HOST= # IP address of Broker
MQTT_BROKER_PORT=1883
```
## Start the Broker

```bash
mosquitto -v
```

## Running the Application

Once the dependencies are installed and the environment is configured, run the following on two different devices:

```bash
python mqtt_publisher.py
```
```bash
python mqtt_subscriber.py
```
