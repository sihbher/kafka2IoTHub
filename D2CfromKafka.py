#pip install kafka-python
#pip azure.iot.device
import time,json,sys
from azure.iot.device import IoTHubDeviceClient, Message
from kafka import KafkaConsumer

CONNECTION_STRING = '<device connection string>'
topic = '<topic name>'
kafkaServer = '<localhost:9092>'

def iothub_client_init():
    client = IoTHubDeviceClient.create_from_connection_string(
        CONNECTION_STRING)
    return client


def initKafkaReader():
    reader = KafkaConsumer(topic, bootstrap_servers=kafkaServer)
    # ,value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    return reader


def readFromKafka():
    try:
        client = iothub_client_init()
        kafkaReader = initKafkaReader()
        
        for msg in kafkaReader:
            decodeMessage = msg.value.decode('utf-8','ignore')
            print('Message received: {}'.format(decodeMessage))
            message = Message(decodeMessage)
            client.send_message(message)

    except KeyboardInterrupt:
        print("Kafka reader stopped")
    except:
        print("Unexpected error:", sys.exc_info()[0])


if __name__ == '__main__':
    print("Starting to read frorm kafka")
    print("Press Ctrl-C to exit")
    readFromKafka()
