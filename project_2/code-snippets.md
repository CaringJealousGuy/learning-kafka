```
app = faust.App(
    "application-name",
    broker="kafka://kafka.example.com",
    store="rocksdb://",
)
```
```
stream = app.stream(my_topic)

async for event in stream:
    # Обработка сообщений
    pass
```