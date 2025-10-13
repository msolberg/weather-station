# Copyright Michael Solberg <mpsolberg@gmail.com>
# Based on AWS IOT SDK samples:
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

import configparser
from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import sys
import time
import board
import json
import adafruit_bme680

config = configparser.ConfigParser()
config.read('weather-station.ini')

endpoint=config['AWS']['endpoint']
cert_filepath=config['AWS']['cert_filepath']
pri_key_filepath=config['AWS']['pri_key_filepath']
ca_filepath=config['AWS']['ca_filepath']
clientId=config['AWS']['clientId']
message_topic=config['AWS']['message_topic']

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

def read_bme680(bme680):
    data = {
        "temperature_f": None,
        "gas": None,
        "humidity": None,
        "pressure": None
        }
    
    try:
        temperature_c = bme680.temperature - 5
        data['temperature_f'] = temperature_c * (9 / 5) + 32
        data['gas'] = bme680.gas
        data['humidity'] = bme680.relative_humidity
        data['pressure'] = bme680.pressure
    except RuntimeError as error:
        # If we get an error, let's log it and return empty data.
        print(error.args[0])

    return data
        
# Create sensor object, communicating over the board's default I2C bus
i2c = board.I2C()   # uses board.SCL and board.SDA
bme680 = adafruit_bme680.Adafruit_BME680_I2C(i2c)

# change this to match the location's pressure (hPa) at sea level
bme680.sea_level_pressure = 1013.25

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
        data = read_bme680(bme680)
        message = json.dumps(data)
        if data['temperature_f'] is not None:
            print("Publishing message to topic '{}': {}".format(message_topic, message))
            mqtt_connection.publish(
                topic=message_topic,
                payload=message,
                qos=mqtt.QoS.AT_LEAST_ONCE)
        else:
            print("Failed to retrieve data from sensors")
        time.sleep(5)

    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")
