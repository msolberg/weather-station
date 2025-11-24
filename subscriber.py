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

# These are the global variables we're exposing
# We'll set them all to zero so that we don't send garbage numbers up the chain
outside_temperature = 0
outside_humidity = 0
outside_pressure = 0
inside_temperature = 0
inside_humidity = 0
inside_voc = 0

inside_temperature_gauge = Gauge('inside_temperature', 'Inside temperature (f)')
inside_temperature_gauge.set(inside_temperature)

inside_humidity_gauge = Gauge('inside_humidity', 'Inside humidity (%)')
inside_humidity_gauge.set(inside_humidity)

inside_voc_gauge = Gauge('inside_voc', 'Inside VOC')
inside_voc_gauge.set(inside_voc)

outside_temperature_gauge = Gauge('outside_temperature', 'Outside temperature (f)')
outside_temperature_gauge.set(outside_temperature)

outside_humidity_gauge = Gauge('outside_humidity', 'Outside humidity (%)')
outside_humidity_gauge.set(outside_humidity)

outside_pressure_gauge = Gauge('outside_pressure', 'Outside air pressure (mb)')
outside_pressure_gauge.set(outside_pressure)

# Let's not kill the National Weather Service
nws_cooldown = 24
nws_cache = {}

# Grab NWS data for pieces we're missing - unused for now.
def get_nws_data():
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
        data['pressure'] = d['properties']['barometricPressure']['value'] / 100
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

def send_data_to_wunderground():
    # Send data to Wunderground every 2 seconds
    
    while not received_all_event.is_set():
        # do some conversions on the data
        inches = float(outside_pressure) / 33.8639
        # https://en.wikipedia.org/wiki/Dew_point#Simple_approximation
        dewpoint = float(outside_temperature) - 9/25 * (100 - float(outside_humidity))
        print("Sending temperature %s, humidity %s, pressure %s, calculated dewpoint %s"% (outside_temperature, outside_humidity, outside_pressure, dewpoint))
        
        # Make sure we have data. Outside humidity should never be 0
        if (outside_humidity == 0):
            print("Humidity is 0 - not sending data")
        else:
            r = requests.get("https://weatherstation.wunderground.com/weatherstation/updateweatherstation.php?ID=%s&PASSWORD=%s&dateutc=now&humidity=%s&tempf=%s&baromin=%s&dewptf=%s&action=updateraw"% (station_id, station_pass, outside_humidity, outside_temperature, inches, dewpoint))
            if r.status_code == 200:
                print("Uploaded data to Wunderground")
            else:
                print("Error uploading data to Wunderground %d"% (r.status_code,))
        
        time.sleep(2)
    

# Callback when the subscribed topic receives a message
def on_message_received(topic, payload, dup, qos, retain, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))
    global inside_temperature
    global inside_humidity
    global inside_voc
    global outside_temperature
    global outside_humidity
    global outside_pressure

    try:
        data = json.loads(payload)
        print("Got payload %s"% (payload,))
        temperature = data['temperature_f']
        humidity = data['humidity']
        pressure = data['pressure']
        voc = data['gas']
    except json.decoder.JSONDecodeError:
        print("Received malformed message %s"% (payload))
        return
    except KeyError:
        print("Received data %s"% (payload))

    if 'pressure' in data.keys():
        # This is from the bme688 inside the house
        inside_temperature = temperature
        inside_humidity = humidity
        outside_pressure = pressure
        inside_voc = voc
        inside_temperature_gauge.set(temperature)
        inside_humidity_gauge.set(humidity)
        outside_pressure_gauge.set(pressure)
        inside_voc_gauge.set(voc)
    else:
        # This is from the DHT outside the house
        outside_temperature = temperature
        outside_humidity = humidity
        outside_temperature_gauge.set(temperature)
        outside_humidity_gauge.set(humidity)
    

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
