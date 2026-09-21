import logging
import time
from datetime import datetime

from kafka import KafkaProducer

from message import Message
from serializer import serialize_message


# Настройка логирования результатов отправки и ошибок.
logging.basicConfig(
    filename="producer.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


# Producer подключается к обоим брокерам Kafka.
# acks="all" и retries=5 обеспечивают At Least Once delivery:
# Kafka подтверждает запись, а при ошибке Producer повторяет отправку.
producer = KafkaProducer(
    bootstrap_servers=["kafka-1:29092", "kafka-2:29092","kafka-3:29092"],
    acks="all",
    retries=5,
)


# Основной цикл Producer: создаёт сообщения и асинхронно отправляет их
# в topic "messages", после чего ждёт 1 секунду перед следующим сообщением.
def run():
    try:
        message_number = 1

        while True:
            text = f"message {message_number}"

            message = Message(
                id=message_number,
                text=text,
                timestamp=datetime.now().isoformat(),
            )

            print(f"Sending message: {message}")

            future = producer.send(
                "messages",
                value=serialize_message(message),
            )

            # Callback вызывается после успешной отправки и записывает
            # в лог topic, partition и offset сообщения.
            def on_send_success(record_metadata):
                logger.info(
                    "Message sent: topic=%s, partition=%s, offset=%s",
                    record_metadata.topic,
                    record_metadata.partition,
                    record_metadata.offset,
                )

            # Errback вызывается при ошибке отправки.
            def on_send_error(exception):
                logger.error(
                    "Error sending message: %s",
                    exception,
                )

            future.add_callback(on_send_success)
            future.add_errback(on_send_error)

            message_number += 1
            time.sleep(1)

    # Корректно завершаем Producer при остановке приложения.
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")

    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed")


if __name__ == "__main__":
    run()