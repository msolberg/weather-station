# Copyright Michael Solberg <mpsolberg@gmail.com>
# Based on AWS IOT SDK samples:
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.

from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
from prometheus_client import start_http_server, Gauge
import sys
import threading
import time
import json
import requests
import configparser

config = configparser.ConfigParser()
config.read('weather-station.ini')

endpoint=config['AWS']['endpoint']
cert_filepath=config['AWS']['cert_filepath']
pri_key_filepath=config['AWS']['pri_key_filepath']
ca_filepath=config['AWS']['ca_filepath']
clientId=config['AWS']['clientId']
message_topic=config['AWS']['message_topic']
station_id=config['WU']['station_id']
station_pass=config['WU']['station_pass']

received_all_event = threading.Event()

temperature = 69
humidity = 50
pressure = 966

# Let's not kill the National Weather Service
nws_cooldown = 24
nws_cache = {}

outside_temperature = Gauge('outside_temperature', 'Outside temperature (f)')
outside_temperature.set(temperature)

outside_humidity = Gauge('outside_humidity', 'Outside humidity (%)')
outside_humidity.set(humidity)

outside_pressure = Gauge('outside_pressure', 'Outside air pressure (mb)')
outside_pressure.set(pressure)


# Grab NWS data for pieces we're missing
def get_nws_data():
    global config
    global nws_cooldown
    global nws_cache
    
    station_id = config['NWS']['station_id']
    require_qc = config['NWS']['require_qc']
    
    data = {}
    if nws_cooldown == 24:
        nws_cooldown = nws_cooldown - 1
        r = requests.get("https://api.weather.gov/stations/%s/observations/latest?require_qc=%s"% (station_id, require_qc))
        if r.status_code == 200:
            d = json.loads(r.text)
            nws_cache = d
        else:
            print("Unable to get weather data from NWS")
            return data
    else:
        if nws_cooldown == 0:
            nws_cooldown = 24
        else:
            nws_cooldown = nws_cooldown - 1
        d = nws_cache

    try:
        data['dewpoint'] = d['properties']['dewpoint']['value']
        data['pressure'] = d['properties']['barometricPressure']['value']
        data['windSpeed'] = d['properties']['windSpeed']['value']
        data['windDirection'] = d['properties']['windDirection']['value']
    except KeyError:
        print("Couldn't load all the data from NWS: %s"% data)
    return data

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

# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))
    global temperature
    global humidity
    global pressure

    try:
        data = json.loads(payload)
        print("Got payload %s"% (payload,))
        temperature = data['temperature_f']
        humidity = data['humidity']
        pressure = data['pressure']
    except json.decoder.JSONDecodeError:
        print("Received malformed message %s"% (payload))
        return
    except KeyError:
        print("Received missing data %s"% (payload))

    if 'pressure' not in data.keys():
        # We'll grab the pressure from NWS
        nws_data = get_nws_data()
        if 'pressure' in nws_data.keys():
            data['pressure'] = nws_data['pressure']
    
    if 'pressure' in data.keys():
        outside_temperature.set(temperature)
        outside_humidity.set(humidity)
        outside_pressure.set(pressure)
        inches = float(pressure) / 33.8639
        # https://en.wikipedia.org/wiki/Dew_point#Simple_approximation
        dewpoint = float(temperature) - 9/25 * (100 - float(humidity))
        print("Got temperature %s, humidity %s, pressure %s, calculated dewpoint %s"% (temperature, humidity, pressure, dewpoint))
        # Send data to Wunderground
        r = requests.get("https://weatherstation.wunderground.com/weatherstation/updateweatherstation.php?ID=%s&PASSWORD=%s&dateutc=now&humidity=%s&tempf=%s&baromin=%s&dewptf=%s&action=updateraw"% (station_id, station_pass, humidity, temperature, inches, dewpoint))
        if r.status_code == 200:
            print("Uploaded data to Wunderground")
    else:
        # We've only got a DHT22 and we couldn't get the data from NWS
        outside_temperature.set(temperature)
        outside_humidity.set(humidity)
        # Send data to Wunderground
        r = requests.get("https://weatherstation.wunderground.com/weatherstation/updateweatherstation.php?ID=%s&PASSWORD=%s&dateutc=now&humidity=%s&tempf=%s&action=updateraw"% (station_id, station_pass, humidity, temperature))
        if r.status_code == 200:
            print("Uploaded data to Wunderground")

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

if __name__ == '__main__':
    # Start the status page
    start_http_server(8001)

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

    # Subscribe
    print("Subscribing to topic '{}'...".format(message_topic))
    subscribe_future, packet_id = mqtt_connection.subscribe(
        topic=message_topic,
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received)

    subscribe_result = subscribe_future.result()
    print("Subscribed with {}".format(str(subscribe_result['qos'])))
   
    received_all_event.wait()

    # Disconnect
    print("Disconnecting...")
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("Disconnected!")
