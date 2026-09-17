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
    group_id="single-consumer-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)


def run():
    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            for records_from_partition in records.values():
                for message in records_from_partition:
                    try:
                        received_message = deserialize_message(message.value)

                        print(f"Received message: {received_message}")

                        # Здесь позже будет обработка сообщения.

                    except Exception:
                        logger.exception(
                            "Error while processing message: "
                            "partition=%s, offset=%s",
                            message.partition,
                            message.offset,
                        )

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")

    finally:
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    run()