import json

from message import Message


# Сериализация Message → JSON → bytes.
# Подготавливает объект сообщения к отправке в Kafka.
def serialize_message(message: Message) -> bytes:
    try:
        data = {
            "id": message.id,
            "text": message.text,
            "timestamp": message.timestamp,
        }

        json_string = json.dumps(data)

        return json_string.encode("utf-8")

    # Ошибка сериализации: выводим причину и передаём исключение дальше.
    except (TypeError, ValueError) as error:
        print(f"Serialization error: {error}")
        raise


# Десериализация bytes → JSON → Message.
# Преобразует полученное из Kafka сообщение обратно в объект Message.
def deserialize_message(data: bytes) -> Message:
    try:
        json_string = data.decode("utf-8")
        message_data = json.loads(json_string)

        return Message(
            id=message_data["id"],
            text=message_data["text"],
            timestamp=message_data["timestamp"],
        )

    # Ошибка десериализации: выводим причину и передаём исключение дальше.
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        print(f"Deserialization error: {error}")
        raise