import json

from message import Message


def serialize_message(message: Message) -> bytes:
    try:
        data = {
            "id": message.id,
            "text": message.text,
            "timestamp": message.timestamp,
        }

        json_string = json.dumps(data)

        return json_string.encode("utf-8")

    except (TypeError, ValueError) as error:
        print(f"Serialization error: {error}")
        raise


def deserialize_message(data: bytes) -> Message:
    try:
        json_string = data.decode("utf-8")
        message_data = json.loads(json_string)

        return Message(
            id=message_data["id"],
            text=message_data["text"],
            timestamp=message_data["timestamp"],
        )

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        KeyError,
        TypeError,
        ValueError,
    ) as error:
        print(f"Deserialization error: {error}")
        raise
