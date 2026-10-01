import threading

from project_2.batch_message_consumer import run as run_batch_consumer
from project_2.producer import run as run_producer
from project_2.single_message_consumer import run as run_single_consumer


# Создаём отдельный поток для каждого компонента приложения:
# Producer, Single Message Consumer и Batch Message Consumer.
def run():
    producer_thread = threading.Thread(target=run_producer)
    single_consumer_thread = threading.Thread(target=run_single_consumer)
    batch_consumer_thread = threading.Thread(target=run_batch_consumer)

    # Запускаем все три компонента параллельно.
    producer_thread.start()
    single_consumer_thread.start()
    batch_consumer_thread.start()

    # Ждём завершения всех потоков.
    producer_thread.join()
    single_consumer_thread.join()
    batch_consumer_thread.join()


if __name__ == "__main__":
    run()