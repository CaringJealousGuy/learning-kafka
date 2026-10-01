import faust


# Модель обычного пользовательского сообщения.
# sender      — отправитель сообщения
# recipient   — получатель сообщения
# content     — текст сообщения
class Message(faust.Record):
    sender: str
    recipient: str
    content: str


# Событие изменения списка заблокированных пользователей.
# action=True  — заблокировать пользователя
# action=False — разблокировать пользователя
class BlockUserEvent(faust.Record):
    user: str
    blocked_user: str
    action: bool


# Создание приложения Faust.
# broker — адрес Kafka
# store  — персистентное хранилище для Faust Tables
app = faust.App(
    "message-filter-app",
    broker="kafka://localhost:9092",
    store="rocksdb://",
)


# ============================================================
# Kafka topics
# ============================================================

# Входной topic.
# Сюда поступают все сообщения от пользователей.
messages_topic = app.topic(
    "messages",
    value_type=Message,
)

# Topic событий блокировки пользователей.
# Сюда отправляются команды /block и /unblock
# в виде структурированных событий.
blocked_users_topic = app.topic(
    "blocked_users",
    value_type=BlockUserEvent,
)

# Выходной topic.
# Сюда попадают сообщения после всех необходимых проверок
# и обработки.
filtered_messages_topic = app.topic(
    "filtered_messages",
    value_type=Message,
)


# ============================================================
# Faust Tables
# ============================================================

# Персистентный список заблокированных пользователей.
#
# Ключ   — пользователь
# Значение — список пользователей, которых он заблокировал
#
# Например:
# Alice -> ["Bob", "Charlie"]
# Bob   -> ["Alice"]
blocked_users_table = app.Table(
    "blocked_users",
    default=list,
    partitions=1,
    options={"max_open_files": 1000},
)


# Персистентный список запрещённых слов.
#
# Ключом является само запрещённое слово.
# Значение нам не важно — важно только наличие ключа.
#
# Например:
# "idiot"  -> None
# "stupid" -> None
forbidden_words_table = app.Table(
    "forbidden_words",
    default=str,
    partitions=1,
    options={"max_open_files": 1000},
)


# ============================================================
# Вспомогательные функции
# ============================================================

# Проверяет каждое слово сообщения по таблице запрещённых слов.
# Если слово запрещено, оно заменяется символами '*'.
def censor_message(text: str) -> str:
    words = text.split()
    censored_message = []

    for word in words:
        if word.lower() in forbidden_words_table:
            censored_message.append("*" * len(word))
        else:
            censored_message.append(word)

    return " ".join(censored_message)


# ============================================================
# Обработка изменений списка блокировок
# ============================================================

# Agent читает события из blocked_users topic
# и на их основе изменяет blocked_users_table.
@app.agent(blocked_users_topic)
async def process_blocked_users(stream):
    async for event in stream:

        # Получаем текущий список заблокированных пользователей.
        blocked_users = blocked_users_table[event.user]

        # action=True означает блокировку.
        if event.action:
            # Не добавляем пользователя повторно,
            # если он уже находится в списке.
            if event.blocked_user not in blocked_users:
                blocked_users.append(event.blocked_user)

            system_message = (
                f"Пользователь {event.blocked_user} был заблокирован."
            )

        # action=False означает разблокировку.
        else:
            # Удаляем пользователя только если он действительно
            # присутствует в списке.
            if event.blocked_user in blocked_users:
                blocked_users.remove(event.blocked_user)

            system_message = (
                f"Пользователь {event.blocked_user} был разблокирован."
            )

        # Сохраняем изменённый список обратно в Table.
        blocked_users_table[event.user] = blocked_users

        # Отправляем пользователю системное уведомление
        # о результате операции.
        await filtered_messages_topic.send(
            key=event.user,
            value=Message(
                sender="SYSTEM",
                recipient=event.user,
                content=system_message,
            ),
        )


# ============================================================
# Обработка входящих сообщений
# ============================================================

# Основной agent приложения.
# Он получает сообщения из messages topic и определяет,
# что с каждым сообщением нужно сделать.
@app.agent(messages_topic)
async def process_messages(stream):
    async for message in stream:

        sender = message.sender
        recipient = message.recipient
        content = message.content

        # --------------------------------------------------------
        # Команды администратора
        # --------------------------------------------------------
        #
        # Пользователь ADMIN может изменять список
        # запрещённых слов.
        if sender == "ADMIN":
            command, words = content.split(maxsplit=1)
            words = words.split()

            # Добавление запрещённых слов.
            if command == "/addword":
                for word in words:
                    forbidden_words_table[word.lower()] = None

            # Удаление запрещённых слов.
            elif command == "/removeword":
                for word in words:
                    word = word.lower()

                    if word in forbidden_words_table:
                        del forbidden_words_table[word]

            # Команда администратора полностью обработана.
            # Дальше она не должна проходить как обычное сообщение.
            continue


        # --------------------------------------------------------
        # Блокировка пользователя
        # --------------------------------------------------------

        # Команда /block отправляет событие в blocked_users topic.
        if content.startswith("/block"):
            blocked_user = content.split(maxsplit=1)[1]

            await blocked_users_topic.send(
                key=sender,
                value=BlockUserEvent(
                    user=sender,
                    blocked_user=blocked_user,
                    action=True,
                ),
            )

            # Команда обработана.
            continue


        # --------------------------------------------------------
        # Разблокировка пользователя
        # --------------------------------------------------------

        # Команда /unblock также отправляет событие
        # в blocked_users topic, но с action=False.
        if content.startswith("/unblock"):
            blocked_user = content.split(maxsplit=1)[1]

            await blocked_users_topic.send(
                key=sender,
                value=BlockUserEvent(
                    user=sender,
                    blocked_user=blocked_user,
                    action=False,
                ),
            )

            # Команда обработана.
            continue


        # --------------------------------------------------------
        # Проверка блокировки
        # --------------------------------------------------------

        # Проверяем, не находится ли отправитель
        # в списке заблокированных у получателя.
        #
        # Например:
        # sender = Alice
        # recipient = Bob
        #
        # Проверяем:
        # "Alice" in blocked_users_table["Bob"]
        if sender in blocked_users_table[recipient]:

            # Отправляем системное сообщение самому отправителю.
            await filtered_messages_topic.send(
                key=sender,
                value=Message(
                    sender="SYSTEM",
                    recipient=sender,
                    content=(
                        "Сообщение не было отправлено: "
                        "пользователь ограничил круг лиц."
                    ),
                ),
            )

            # Исходное сообщение дальше не обрабатываем.
            continue


        # --------------------------------------------------------
        # Цензура сообщения
        # --------------------------------------------------------

        # Если отправитель не заблокирован,
        # сообщение проходит цензуру.
        content = censor_message(content)


        # --------------------------------------------------------
        # Отправка обработанного сообщения
        # --------------------------------------------------------

        # После проверки блокировки и цензуры
        # сообщение отправляется в выходной topic.
        await filtered_messages_topic.send(
            key=recipient,
            value=Message(
                sender=sender,
                recipient=recipient,
                content=content,
            ),
        )