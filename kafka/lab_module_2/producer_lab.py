import time
from confluent_kafka import Producer

# 1. Producer Configuration (Reliability + Batching)
conf = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',                    # Wait for Leader + all ISRs
    'enable.idempotence': True,       # Prevent duplicate writes on retry
    'linger.ms': 10,                  # Wait up to 10ms to batch records
    'batch.size': 16384               # 16 KB max batch size
}

producer = Producer(conf)

# Callback function to inspect RecordMetadata returned by Broker
def delivery_report(err, msg):
    if err is not None:
        print(f"[FAILED] Delivery failed: {err}")
    else:
        key_str = msg.key().decode('utf-8') if msg.key() else "None (Sticky)"
        val_str = msg.value().decode('utf-8')
        print(f"[ACK] Key: {key_str:<15} | Partition: {msg.partition()} | Offset: {msg.offset():<3} | Value: {val_str}")

print("\n--- TEST 1: Keyless Messages (Sticky Partitioning) ---")
for i in range(1, 6):
    producer.produce(
        topic='payments',
        key=None,
        value=f"Anonymous_Click_{i}",
        callback=delivery_report
    )
producer.flush()  # Force send buffered batch and wait for ACKs

time.sleep(1)

print("\n--- TEST 2: Keyed Messages (Deterministic Murmur2 Hashing) ---")
users = ["user_101", "user_202", "user_101", "user_303", "user_202", "user_101"]
for idx, user in enumerate(users, start=1):
    producer.produce(
        topic='payments',
        key=user.encode('utf-8'),
        value=f"Tx_{idx}_for_{user}",
        callback=delivery_report
    )
producer.flush()