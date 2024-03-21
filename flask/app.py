from flask import Flask, request
from kafka import KafkaProducer
from json import dumps
import datetime

producer = KafkaProducer(bootstrap_servers=['192.168.49.2:32152'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

def serialize_datetime(obj): 
    if isinstance(obj, datetime.datetime): 
        return obj.isoformat() 
    raise TypeError("Type not serializable") 

app = Flask(__name__)

@app.route("/")
def default_endpoint():
    return "http://localhost:5000/choose-color"

@app.route("/<color>")
def color_endpoint(color):
    data = {"timestamp": dumps(datetime.datetime.now(), default=serialize_datetime), "color" : color, "host": request.headers.get("Host"), "user-agent": request.headers.get("User-Agent")}
    producer.send("clickstream-topic", value=data)
    return f"{color}"

# ~$ flask run