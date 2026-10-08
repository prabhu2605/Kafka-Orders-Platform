# Real-Time Order Processing Platform

A real-time streaming platform built on Apache Kafka and Python, processing orders end-to-end from ingestion through validation, stream processing, and SQL analytics.

---

# Stack 

Kafka (KRaft, 3-broker), Python 3.10, Avro + Schema Registry, Quix Streams, ksqlDB, Docker.

## How it works
 
```
Producer (Avro)  →  orders topic  →  Processor (validate + enrich + DLQ)
                                   →  processed-orders topic
                                   →  Quix Streams (windowed aggregations)
                                   →  ksqlDB (SQL analytics)
```
 
**Layer 1** — Avro-serialised producer with idempotence and acks=all  

**Layer 2** — Consumer with validation, enrichment, Dead Letter Queue routing  

**Layer 3** — Quix Streams: tumbling windows, stateful VIP detection

**Layer 4** — ksqlDB: persistent queries, user stats table

**Layer 5** — Consumer lag monitor, cluster health check
 
---
 
## Quick start
 
```bash
# Start infrastructure
cd docker && docker compose up -d
 
# Create topics
./setup_topics.sh
 
# Install dependencies
pip install -r requirements.txt
 
# Run each layer in a separate terminal
python Ingestion/producer.py
python Processing/processor.py
python Streams/streams.py
python Monitoring/monitor.py
```
---