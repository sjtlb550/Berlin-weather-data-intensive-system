#imports
import requests
import datetime
import uuid
import time
import json

from kafka import  KafkaProducer


def get_data():
    url = f"https://api.tomorrow.io/v4/weather/realtime?location=berlin&apikey=N44iq7V8vdgJovgQVgl2DQZMe62ifttY"
    response = requests.get(url).json()
    values = {
        'id': str(uuid.uuid4()),
        'time': response['data']['time'],
        'metrics':{
            'dewPoint': response['data']['values']['dewPoint'],
            'humidity': response['data']['values']['humidity'],
            'temperature': response['data']['values']['temperature'],
            'windSpeed': response['data']['values']['windSpeed']}}


    for metric, val in values['metrics'].items():
        message = {
            'id': values['id'],
            'time': values['time'],
            'metric': metric,
            'value': val
                }

        time.sleep(1)
        yield message
  
while True:
    weather_data = get_data()    
    for m in weather_data:
        print(m)
