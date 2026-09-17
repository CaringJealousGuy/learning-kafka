import logging

from kafka import KafkaConsumer

from serializer import deserialize_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


consumer = KafkaConsumer(
    "messages",
    bootstrap_servers=["kafka-1:29092", "kafka-2:29093"],
    group_id="batch-consumer-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    max_poll_records=10,
    fetch_min_bytes=1000,
    fetch_max_wait_ms=10000,
)


def run():
    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            if not records:
                continue

            messages_count = 0

            for records_from_partition in records.values():
                for message in records_from_partition:
                    try:
                        received_message = deserialize_message(message.value)

                        print(f"Received message: {received_message}")

                        # Здесь позже будет обработка сообщения.

                        messages_count += 1

                    except Exception:
                        logger.exception(
                            "Error while processing message: "
                            "partition=%s, offset=%s",
                            message.partition,
                            message.offset,
                        )

            logger.info(
                "Batch processed: %s messages. Committing offsets.",
                messages_count,
            )

            consumer.commit()

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")

    finally:
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    run()