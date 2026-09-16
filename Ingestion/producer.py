import json
import threading
import logging
import time
import random
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(levelname)-8s | %(name)-24s | %(message)s')
log = logging.getLogger('producer')

# Schema Registry Setup 
schema_registry = SchemaRegistryClient({
    'url' : 'http://localhost:8081'
})

with open('schemas/order.avsc') as f:
    schema_str = f.read()

avro_serializer = AvroSerializer(
    schema_registry,
    schema_str,
    to_dict=lambda obj, ctx: obj,
)

producer = SerializingProducer({
    'bootstrap.servers':  'localhost:9092,localhost:9093,localhost:9094',
    'acks':               'all',
    'enable.idempotence': True,
    'linger.ms':          5,
    'compression.type':   'snappy',
    'retries':            10,
    'retry.backoff.ms':   200,
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
})

ITEMS = [
    ('coffee',    4.50, 'hot-drinks'),
    ('tea',       3.00, 'hot-drinks'),
    ('latte',     5.50, 'hot-drinks'),
    ('muffin',    2.75, 'food'),
    ('bagel',     3.25, 'food'),
    ('croissant', 3.75, 'food'),
    ('juice',     3.50, 'cold-drinks'),
    ('smoothie',  6.00, 'cold-drinks'),
]

USERS = [f'user-{i:02d}' for i in range(1,11)]

stats = {'produced': 0, 'error':0}
stats_lock = threading.Lock()

def delivery_callback(err, msg):
    with stats_lock:
        if err is None:
            stats['produced'] += 1
        else:
            stats['error'] += 1
            log.error(f"delivery failed: {err}")

def generate_order(order_id: int) -> dict:
    item, amount, category = random.choice(ITEMS)

    # 5% chance of invalid order (for DLQ testing)
    if random.random() < 0.5:
        amount = -1.0  #invalid order (for DLQ testing)

    return {
        'order_id':   order_id,
        'user_id':    random.choice(USERS),
        'item':       item,
        'amount':     amount,
        'category':   category,
        'created_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'source':     'order-api',
    }

def run():
    order_id = 1
    log.info('Producer started... press ctrl+c to stop')

    try:
        while True:
            order = generate_order(order_id)
            producer.produce(
                topic='orders',
                key=order['user_id'],
                value=order,
                on_delivery=delivery_callback,
            )
            producer.poll(0)
            order_id += 1
            time.sleep(0.5)

    except KeyboardInterrupt:
        producer.flush()
        log.info(
            f"producer stopped... "
            f"produced = {stats['produced']} error = {stats['error']}"  
        )

if __name__ == '__main__':
    run()