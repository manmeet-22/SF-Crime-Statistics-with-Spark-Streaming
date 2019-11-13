import pathlib
import json
import logging
import pykafka
import time

INPUT_FILE = 'police-department-calls-for-service.json'

logger = logging.getLogger(__name__)


def read_file() -> json:
    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)
    return data


def generate_data() -> None:
    data = read_file()
    for i in data:
        message = dict_to_binary(i)
        producer.produce(message)
        time.sleep(2)


# TODO complete this function
def dict_to_binary(json_dict: dict) -> bytes:
    """
    Encode your json to utf-8
    :param json_dict:
    :return:
    """
    data=json.dumps(json_dict)
    return data.encode('utf-8')

# TODO set up kafka client
if __name__ == "__main__":
    client =pykafka.KafkaClient(hosts="localhost:9092")
    print("topics", client.topics)
    producer = client.topics[b'service-calls'].get_producer()

    generate_data()
