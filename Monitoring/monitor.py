import time
import logging
import requests
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-24s | %(message)s')
log = logging.getLogger('layer5.monitor')

BOOTSTRAP = 'localhost:9092,localhost:9093,localhost:9094'
KSQL_URL  = 'http://localhost:8088'

CONSUMER_GROUPS = [
    {'group': 'order-processor',   'topic': 'orders'},
    {'group': 'stream-processor',  'topic': 'processed-orders'},
]
def get_lag(group_id: str, topic: str) -> int:
    consumer = Consumer({'bootstrap.servers': BOOTSTRAP, 'group.id': group_id})
    try:
        meta  = consumer.list_topics(topic=topic, timeout=5)
        parts = [TopicPartition(topic, p)
                 for p in meta.topics[topic].partitions]
        committed = consumer.committed(parts, timeout=5)
        total = 0
        for tp in committed:
            _, high = consumer.get_watermark_offsets(tp, timeout=5)
            total  += high - (tp.offset if tp.offset >= 0 else 0)
        return total
    finally:
        consumer.close()


def get_dlq_count() -> int:
    consumer = Consumer({
        'bootstrap.servers':  BOOTSTRAP,
        'group.id':           'dlq-counter',
        'auto.offset.reset':  'earliest',
        'enable.auto.commit': False,
    })
    try:
        meta  = consumer.list_topics(topic='orders-dlq', timeout=5)
        parts = [TopicPartition('orders-dlq', p)
                 for p in meta.topics['orders-dlq'].partitions]
        total = 0
        for tp in parts:
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            total += high - low
        return total
    finally:
        consumer.close()


def pull_ksql(sql: str) -> list:
    try:
        resp = requests.post(
            f'{KSQL_URL}/query',
            headers={'Content-Type': 'application/vnd.ksql.v1+json'},
            json={'ksql': sql},
            timeout=5,
        )
        return resp.json() if resp.ok else []
    except Exception:
        return []


def run_monitor():
    log.info("Layer 5 Monitor started — reporting every 10 seconds")

    while True:
        print(f"\n{'═' * 64}")
        print(f"  System Health — {time.strftime('%H:%M:%S')}")
        print(f"{'═' * 64}")

        # ── Consumer lag ──────────────────────────────────────────
        print("\n  Consumer Lag:")
        for entry in CONSUMER_GROUPS:
            try:
                lag = get_lag(entry['group'], entry['topic'])
                status = '✓' if lag < 100 else '⚠' if lag < 1000 else '✗'
                print(f"    {status} {entry['group']:<30} lag={lag:,}")
            except Exception as e:
                print(f"    ✗ {entry['group']}: {e}")

        # ── DLQ count ─────────────────────────────────────────────
        try:
            dlq_count = get_dlq_count()
            print(f"\n  DLQ Messages: {dlq_count:,}")
        except Exception as e:
            print(f"\n  DLQ check failed: {e}")

        # ── ksqlDB user stats ──────────────────────────────────────
        print("\n  Top Users by Spend (from ksqlDB):")
        rows = pull_ksql(
            "SELECT user_id, customer_name, customer_tier, "
            "total_orders, lifetime_spend "
            "FROM user_lifetime_stats LIMIT 5;"
        )
        for row in rows:
            if 'row' in row:
                cols = row['row']['columns']
                print(
                    f"    {cols[1]:<10} ({cols[2]:<8}) "
                    f"orders={cols[3]:>3} "
                    f"spend=£{cols[4]:.2f}"
                )

        print(f"\n{'═' * 64}")
        time.sleep(10)


if __name__ == '__main__':
    try:
        run_monitor()
    except KeyboardInterrupt:
        log.info("Monitor stopped")