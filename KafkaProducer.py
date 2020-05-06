from kafka import KafkaProducer
import time
import json


producer = KafkaProducer( bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
for i in range(200):
    producer.send('test', {'foo': 'message ' + str(i)})
    print("message sent: " + str(i))
    time.sleep(3)