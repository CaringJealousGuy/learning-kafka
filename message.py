from dataclasses import dataclass


@dataclass
class Message:
    id: int
    text: str
    timestamp: str
