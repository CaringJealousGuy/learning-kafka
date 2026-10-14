
import json

from kafka import KafkaConsumer
from kafka.errors import KafkaError


BOOTSTRAP_SERVERS = "localhost:9094"

TOPICS = [
    "dbserver1.public.users",
    "dbserver1.public.orders",
]


def main():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="postgres-change-reader",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print("Waiting for PostgreSQL changes...")
    print(f"Subscribed topics: {', '.join(TOPICS)}")

    try:
        for message in consumer:
            event = message.value

            print("\n" + "=" * 60)
            print(f"Topic: {message.topic}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")

            if event is None:
                print("Empty event (possibly a tombstone)")
                continue

            payload = event.get("payload", event)
            print(json.dumps(payload, ensure_ascii=False, indent=2))

    except KeyboardInterrupt:
        print("\nConsumer stopped.")

    except KafkaError as error:
        print(f"Kafka error: {error}")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
