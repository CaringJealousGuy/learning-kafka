import logging
import time
from datetime import datetime

from kafka import KafkaProducer

from message import Message
from serializer import serialize_message

logging.basicConfig(
    filename="producer.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


producer = KafkaProducer(
    bootstrap_servers=["kafka-1:29092", "kafka-2:29093"],
    acks="all",
    retries=5,
)

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

            def on_send_success(record_metadata):
                logger.info(
                    "Message sent: topic=%s, partition=%s, offset=%s",
                    record_metadata.topic,
                    record_metadata.partition,
                    record_metadata.offset,
                )

            def on_send_error(exception):
                logger.error(
                    "Error sending message: %s",
                    exception,
                )

            future.add_callback(on_send_success)
            future.add_errback(on_send_error)

            message_number += 1
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")

    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed")  

if __name__ == "__main__":
    run()