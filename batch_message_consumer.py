import logging

from kafka import KafkaConsumer

from serializer import deserialize_message


# Настройка логирования ошибок и основных событий Consumer.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


# Consumer читает topic "messages" в отдельной consumer group.
# auto_offset_reset="earliest" — при отсутствии сохранённого offset
# начинать чтение с начала topic.
# Auto commit отключён: offset подтверждается вручную после обработки batch.
# fetch_min_bytes и fetch_max_wait_ms позволяют настроить ожидание данных
# перед получением batch.
consumer = KafkaConsumer(
    "messages",
    bootstrap_servers=["kafka-1:29092", "kafka-2:29092","kafka-3:29092"],
    group_id="batch-consumer-group",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    max_poll_records=10,
    fetch_min_bytes=1000,
    fetch_max_wait_ms=10000,
)


# Основной цикл Consumer: получает batch сообщений, обрабатывает каждое
# сообщение и после завершения batch фиксирует offsets.
def run():
    try:
        while True:
            batch = []

            # Накопление сообщений до размера batch.
            # Один вызов poll() Kafka может вернуть меньше 10 сообщений,
            # поэтому повторяем poll(), пока не накопим минимум 10.
            while len(batch) < 10:
                records = consumer.poll(timeout_ms=1000)

                for records_from_partition in records.values():
                    batch.extend(records_from_partition)

            # Обрабатываем накопленную пачку сообщений.
            for message in batch:
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

            # После обработки всей пачки один раз синхронно
            # фиксируем offsets.
            logger.info(
                "Batch processed: %s messages. Committing offsets.",
                len(batch),
            )

            consumer.commit()

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")

    finally:
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    run()