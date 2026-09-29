from quix_streams import Application, State
from datetime import timedelta
import time

app = Application(
    broker_address='localhost:9092,localhost:9093,localhost:9094',
    consumer_group='stream-processor',
    auto_offset_reset='earliest',
)

input_topic   = app.topic('processed-orders', value_deserializer='json')
counts_topic  = app.topic('order-counts',     value_serializer='json')
revenue_topic = app.topic('order-revenue',    value_serializer='json')
alerts_topic  = app.topic('order-alerts',     value_serializer='json')

sdf = app.dataframe(input_topic)

sdf = sdf[sdf['amount_after_discount'] > 0]

def track_customer_value(row: dict, state: State):
    total = state.get('lifetime_spend', default=0.0)
    total += row['amount_after_discount']
    state.set('lifetime_spend', total)

    if total > 50.0 and not state.get('vip_alerted', default=False):
        state.set('vip_alerted', True)
        return {
            'alert_type': 'VIP_THRESHOLD',
            'user_id':    row['user_id'],
            'name':       row.get('customer_name'),
            'lifetime':   round(total, 2),
            'detected_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        }
    return None

sdf_alerts = sdf.apply(track_customer_value, stateful=True)
sdf_alerts = sdf_alerts.filter(lambda r: r is not None)
sdf_alerts = sdf_alerts.update(
    lambda r: print(f" VIP ALERT: {r['name']} reached £{r['lifetime']} lifetime spend")
)
sdf_alerts = sdf_alerts.to_topic(alerts_topic)

sdf_counts = (
    sdf
    .tumbling_window(timedelta(seconds=60))
    .count()
    .final()
    .apply(lambda r: {
        'window_start': r['start'],
        'window_end':   r['end'],
        'order_count':  r['value'],
    })
)
sdf_counts = sdf_counts.to_topic(counts_topic)

sdf_revenue = (
    sdf
    .tumbling_window(timedelta(seconds=60))
    .sum(column='amount_after_discount')
    .final()
    .apply(lambda r: {
        'window_start':  r['start'],
        'window_end':    r['end'],
        'total_revenue': round(r['value'], 2),
    })
)
sdf_revenue = sdf_revenue.to_topic(revenue_topic)

print("Stream Processor running...")
app.run(sdf)