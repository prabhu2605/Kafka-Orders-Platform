import json
import time
import logging
import threading
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s | %(levelname)-8s | %(name)-24s | %(message)s')
log = logging.getLogger('processor')

schema_registry = SchemaRegistryClient({
    'url' : 'http://localhost:8081'
})

with open('schemas/order.avsc') as f:
    schema_str = f.read()

avro_deserializer = AvroDeserializer(
    schema_registry,
    schema_str,
    from_dict=lambda obj, ctx: obj,
)

# Custom exceptions
class TransientError(Exception): pass
class PersistentError(Exception): pass

stats      = {'processed': 0, 'dlq': 0, 'retries': 0}
stats_lock = threading.Lock()

USER_PROFILES = {
    f'user-{i:02d}': {
        'name': name,
        'tier': tier,
        'email': f'{name.lower()}@example.com'
    }
    for i, (name, tier) in enumerate([
        ('Alice', 'GOLD'),    ('Bob', 'SILVER'),  ('Carol', 'GOLD'),
        ('Dave', 'BRONZE'),   ('Eve', 'SILVER'),   ('Frank', 'GOLD'),
        ('Grace', 'BRONZE'),  ('Henry', 'SILVER'), ('Iris', 'GOLD'),
        ('Jack', 'BRONZE'),
    ], start=1)
}

DISCOUNTS = {'GOLD': 0.10, 'PLATINUM': 0.20, 'SILVER': 0.0, 'BRONZE': 0.0}

def validate_order(order: dict) -> None:
    if order.get('amount', 0) <= 0:
        raise PersistentError(f"Invalid amount: {order['amount']}")
    if not order.get('item'):
        raise PersistentError("Missing item field")


def enrich_order(order: dict) -> dict:
    profile  = USER_PROFILES.get(order['user_id'], {})
    tier     = profile.get('tier', 'UNKNOWN')
    discount = DISCOUNTS.get(tier, 0.0)

    return {
        **order,
        'customer_name':       profile.get('name', 'UNKNOWN'),
        'customer_tier':       tier,
        'discount':            discount,
        'amount_after_discount': round(order['amount'] * (1 - discount), 2),
        'processed_at':        time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'processor_version':   'processor-v1',
    }


def send_to_dlq(msg, error: Exception, output_producer: Producer) -> None:
    dlq_record = {
        'original_topic':     msg.topic(),
        'original_partition': msg.partition(),
        'original_offset':    msg.offset(),
        'original_key':       msg.key().decode() if msg.key() else None,
        'error_type':         type(error).__name__,
        'error_message':      str(error),
        'failed_at':          time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    }
    output_producer.produce(
        topic='orders-dlq',
        key=msg.key(),
        value=json.dumps(dlq_record).encode(),
    )
    output_producer.flush()
    with stats_lock:
        stats['dlq'] += 1
    log.warning(f" dlq offset={msg.offset()} error={type(error).__name__}: {error}")

def run():
    consumer = Consumer({
        'bootstrap.servers':  'localhost:9092,localhost:9093,localhost:9094',
        'group.id':           'order-processor',
        'auto.offset.reset':  'earliest',
        'enable.auto.commit': False,
    })

    output_producer = Producer({
        'bootstrap.servers':  'localhost:9092,localhost:9093,localhost:9094',
        'acks':               'all',
        'enable.idempotence': True,
    })

    dlq_producer = Producer({
        'bootstrap.servers':  'localhost:9092,localhost:9093,localhost:9094',
        'acks':               'all',
        'enable.idempotence': True,
    })

    consumer.subscribe(['orders'])
    log.info("Processor started")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                log.error(f"Consumer error: {msg.error()}")
                continue

            try:
                order = avro_deserializer(
                    msg.value(),
                    SerializationContext('orders', MessageField.VALUE)
                )

                validate_order(order)

                enriched = enrich_order(order)

                output_producer.produce(
                    topic='processed-orders',
                    key=msg.key(),
                    value=json.dumps(enriched).encode(),
                )
                output_producer.flush()

                with stats_lock:
                    stats['processed'] += 1

                log.info(
                    f"#{order['order_id']} "
                    f"{enriched['customer_name']} "
                    f"({enriched['customer_tier']}) "
                    f"£{order['amount']} → £{enriched['amount_after_discount']}"
                )

            except PersistentError as e:
                send_to_dlq(msg, e, dlq_producer)

            except Exception as e:
                send_to_dlq(msg, PersistentError(str(e)), dlq_producer)

            finally:
                consumer.commit(asynchronous=True)

    except KeyboardInterrupt:
        log.info(
            f"Processor stopped -----"
            f"processed={stats['processed']} dlq={stats['dlq']}"
        )
    finally:
        consumer.close()
        output_producer.flush()


if __name__ == '__main__':
    run()