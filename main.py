from importlib.metadata import metadata
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import logging
import uuid
import random
import time
import json
import threading


KAFKA_BROKERS = "localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 20
REPLICATION_FACTOR = 3
TOPIC_NAME = "financial_transactions"

logging.basicConfig(
    level=logging.INFO
)

logger = logging.getLogger(__name__)

producer_config = {
    'bootstrap.servers': KAFKA_BROKERS,
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.kbytes': 1048576,
    'batch.num.messages': 10000,
    'linger.ms': 100,
    'acks': 1,
    'compression.type': 'gzip'
}

producer = Producer(producer_config)

def create_topic(topic_name):
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
   
    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            )
        
            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logger.info(f"Topic {topic_name} created.")
                except Exception as e:
                    logger.error(f"Failed to create topic {topic_name}: {e}")
        else:
            logger.info(f"Topic {topic_name} already exists.")
    
    except Exception as e:
        logger.error(f"Failed to create topics: {e}")


def generate_transaction():
    return dict(
        id_transacao = str(uuid.uuid4()),
        id_usuario = f'user_{random.randint(1, 1000)}',
        valor = round(random.uniform(1, 150000), 2),
        hora_transacao = int(time.time()),
        id_vendedor = random.choice(['vendedor_1', 'vendedor_2', 'vendedor_3']),
        tipo_transacao = random.choice(['compra', 'venda']),
        localizacao = f'localizacao_{random.randint(1, 50)}',
        metodo_pagamento = random.choice(['cartao', 'transferencia', 'dinheiro', 'pix']),
        compra_internacional = random.choice([True, False]),
        moeda = random.choice(['BRL', 'USD', 'EUR'])
    )


def produce_transaction(thread_id):
    while True:
            transaction = generate_transaction()

            try:
                producer.produce(
                    topic =TOPIC_NAME,
                    key = transaction['id_transacao'],
                    value = json.dumps(transaction).encode('utf-8'),
                    on_delivery = delivery_report
                )
                logger.info(f"Thread {thread_id} -> Produced message: {transaction}")
                #producer.flush()
                producer.poll(0)
            except Exception as e:
                logger.error(f"Failed to produce message: {e}")


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {msg.key()}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def producer_data_parallel(num_threads):
    threads = []
    try:
        for i in range(num_threads):
            thread = threading.Thread(target=produce_transaction, args=(i,))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
            
    except Exception as e:
        logger.error(f"Error sending transaction: {e}")
  

if __name__ == "__main__":

   create_topic(TOPIC_NAME)
   producer_data_parallel(3)

   
    
