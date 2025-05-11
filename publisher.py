# Copyright Michael Solberg <mpsolberg@gmail.com>

from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import sys
import time
import board
import adafruit_dht
import json
import smbus2
import bme280

endpoint="AWS_IOT_ENDPOINT"
cert_filepath="YOUR_CERTIFICATE"
pri_key_filepath="YOUR_PRIVATE_KEY"
ca_filepath="AWS_IOT_ROOT_CERT"
clientId="AWS_IOT_CLIENT_ID"
message_topic="MQTT_TOPIC"

# Callback when connection is accidentally lost.
def on_connection_interrupted(connection, error, **kwargs):
    print("Connection interrupted. error: {}".format(error))

# Callback when an interrupted connection is re-established.
def on_connection_resumed(connection, return_code, session_present, **kwargs):
    print("Connection resumed. return_code: {} session_present: {}".format(return_code, session_present))

    if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
        print("Session did not persist. Resubscribing to existing topics...")
        resubscribe_future, _ = connection.resubscribe_existing_topics()

        # Cannot synchronously wait for resubscribe result because we're on the connection's event-loop thread,
        # evaluate result with a callback instead.
        resubscribe_future.add_done_callback(on_resubscribe_complete)
    

def on_resubscribe_complete(resubscribe_future):
    resubscribe_results = resubscribe_future.result()
    print("Resubscribe results: {}".format(resubscribe_results))

    for topic, qos in resubscribe_results['topics']:
        if qos is None:
            sys.exit("Server rejected resubscribe to topic: {}".format(topic))


# Callback when the connection successfully connects
def on_connection_success(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionSuccessData)
    print("Connection Successful with return code: {} session present: {}".format(callback_data.return_code, callback_data.session_present))

# Callback when a connection attempt fails
def on_connection_failure(connection, callback_data):
    assert isinstance(callback_data, mqtt.OnConnectionFailureData)
    print("Connection failed with error code: {}".format(callback_data.error))

# Callback when a connection has been disconnected or shutdown successfully
def on_connection_closed(connection, callback_data):
    print("Connection closed")

dhtDevice = adafruit_dht.DHT22(board.D4)

def get_temperature_and_humidity():
    data = {
        "temperature_f":  None,
        "humidity":  None
    }

    try:
        # Print the values to the serial port
        temperature_c = dhtDevice.temperature
        temperature_f = temperature_c * (9 / 5) + 32
        humidity = dhtDevice.humidity
        print(
            "Temp: {:.1f} F / {:.1f} C    Humidity: {}% ".format(
                temperature_f, temperature_c, humidity
            )
        )
        data['temperature_f'] = temperature_f
        data['humidity'] = humidity
        return data

    except RuntimeError as error:
        # Errors happen fairly often, DHT's are hard to read, just keep going
        print(error.args[0])
        return data
    except Exception as error:
        dhtDevice.exit()
        raise error

# BME280 sensor address (default address)
address = 0x77
bus = smbus2.SMBus(1)
calibration_params = bme280.load_calibration_params(bus, address)

def celsius_to_fahrenheit(celsius):
    return (celsius * 9/5) + 32

def get_pressure():
    data = bme280.sample(bus, address, calibration_params)
    return data.pressure

if __name__ == '__main__':
    # Create a MQTT connection from the command line data
    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=endpoint,
        cert_filepath=cert_filepath,
        pri_key_filepath=pri_key_filepath,
        ca_filepath=ca_filepath,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
        client_id=clientId,
        clean_session=False,
        keep_alive_secs=30,
        on_connection_success=on_connection_success,
        on_connection_failure=on_connection_failure,
        on_connection_closed=on_connection_closed)

    connect_future = mqtt_connection.connect()

    # Future.result() waits until a result is available
    connect_future.result()
    print("Connected!")

    while True:
        data = get_temperature_and_humidity()
        if data['temperature_f'] is not None:
            temperature_f = data['temperature_f']
            humidity = data['humidity']
        data['pressure'] = get_pressure()
        message = json.dumps(data)
        print("Publishing message to topic '{}': {}".format(message_topic, message))
        mqtt_connection.publish(
            topic=message_topic,
            payload=message,
            qos=mqtt.QoS.AT_LEAST_ONCE)
        time.sleep(5)

    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")
