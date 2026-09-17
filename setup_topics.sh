BROKER="broker-1:29092"

create_topic() {
    docker exec broker-1 kafka-topics \
    --bootstrap-server $BROKER \
    --create --if-not-exists \
    --topic $1 \
    --partitions $2 \
    --replication-factor 3 \
    --config $3 
}

create_topic "orders"            3 "min.insync.replicas=2"
create_topic "processed-orders"  3 "min.insync.replicas=2"
create_topic "orders-dlq"        3 "retention.ms=604800000"  

create_topic "enriched-orders"   3 "min.insync.replicas=2"
create_topic "order-counts"      3 "retention.ms=86400000"   
create_topic "order-revenue"     3 "retention.ms=86400000"

create_topic "user-profiles"     3 "cleanup.policy=compact"

echo "All topics created"
docker exec broker-1 kafka-topics --bootstrap-server $BROKER --list