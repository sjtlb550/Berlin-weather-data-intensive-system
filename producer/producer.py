#imports
import requests
import datetime
import uuid
import time
import json

from kafka import  KafkaProducer


# using the open_meteo open source API to ingest data
# Tesing the ouput by printing out before connecting to kafka.
def get_data():
    url = f"https://api.open-meteo.com/v1/forecast?latitude=52.5244&longitude=13.4105&current=relative_humidity_2m,temperature_2m,precipitation,wind_speed_10m,rain,apparent_temperature,is_day,showers,wind_direction_10m,pressure_msl,surface_pressure,cloud_cover,weather_code&timezone=Europe%2FBerlin&past_days=7"
    response = requests.get(url).json()
    values ={
        'id': str(uuid.uuid4()),
        'time':  datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        'metrics':{
            'temperature': response['current']['temperature_2m'],
            'apparent_temp': response['current']['apparent_temperature'],
            'humidity': response['current']['relative_humidity_2m'],
            'windSpeed': response['current']['wind_speed_10m'] }}
    
    for metric, val in values['metrics'].items():
        message = {
            'id': values['id'],
            'time': values['time'],
            'metric': metric,
            'value': val} # the final message shape.

        time.sleep(1) # sends a message every 1 sec
        yield message



        
  
#kafka producer instant
#encoding the input to be accepted by kafka
#used loop and try-except statement to automatically creates a connection once kafka is ready.
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        break
    except Exception as e:
        time.sleep(5)

try:
    while True:
        for weather_data in get_data():
            producer.send('weather-topic', value = weather_data,
                           key = weather_data['metric'].encode('utf-8'))

        producer.flush()
        time.sleep(5)
except KeyboardInterrupt:
    print('STOP!!!')

finally:
    producer.close()

