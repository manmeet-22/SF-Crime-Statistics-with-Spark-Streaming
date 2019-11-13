from pykafka.simpleconsumer import OffsetType
import logging
from pykafka import KafkaClient

logging.getLogger("pykafka.broker").setLevel('ERROR')
client = KafkaClient(hosts="localhost:9092")
topic = client.topics["service-calls"]
consumer_messages = topic.get_balanced_consumer(
    consumer_group = b'pytkafka-test-2',
    auto_commit_enable = False,
    zookeeper_connect = 'localhost:2181')

for ms in consumer_messages:
    if ms is not None:
        print( ms.value.decode('utf-8') )
