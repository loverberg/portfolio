import logging
from json import dumps
from random import uniform
from time import sleep

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format=" %(levelname)s %(asctime)s: %(message)s")

log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = 'localhost:29092'
TOPIC_NAME = 'taxi'
DATA_FILE = './data/yellow_tripdata_2020-04.csv'

# публикация в топик
def produce(csv_file: str, bootstrap_servers: str, topic: str):
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

    with open(csv_file, 'r') as data_file:
        header = data_file.readline()
        log.info(f'Header is [{header}]')
        count = 0
        while True:
            sleep(uniform(0.001, 0.21))
            line = data_file.readline().strip()

            if not line:
                log.info("File ended")
                break

            count += 1
            fields = line.split(',')

            data = {
                'vendor_id': int(fields[0]),
                'tpep_pickup_datetime': fields[1],
                'tpep_dropoff_datetime': fields[2],
                'passenger_count': int(fields[3]),
                'trip_distance': float(fields[4]),
                'ratecode_id': int(fields[5]),
                'store_and_fwd_flag': fields[6],
                'pulocation_id': int(fields[7]),
                'dolocation_id': int(fields[8]),
                'payment_type': int(fields[9]),
                'fare_amount': float(fields[10]),
                'extra': float(fields[11]),
                'mta_tax': float(fields[12]),
                'tip_amount': float(fields[13]),
                'tolls_amount': float(fields[14]),
                'improvement_surcharge': float(fields[15]),
                'total_amount': float(fields[16]),
                'congestion_surcharge': float(fields[17]),
            }

            producer.send(topic=topic, value=data)
            # log.debug("Line {}: {}".format(count, line.strip()))
            log.info(f"Line {count} sent")

        data_file.close()


if __name__ == '__main__':
    produce(DATA_FILE, BOOTSTRAP_SERVERS, TOPIC_NAME)
