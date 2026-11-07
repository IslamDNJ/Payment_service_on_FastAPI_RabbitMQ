"""Топология RabbitMQ — единое место, общее для API (outbox relay) и consumer.

payments (direct) --payments.new--> payments.new ---(reject)--> payments.dlx --> payments.new.dlq
                                         ^
payments.retry.N (TTL, без consumer'ов) -+  по истечении TTL сообщение возвращается в payments
"""
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import settings

NEW_ROUTING_KEY = "payments.new"
DLQ_ROUTING_KEY = "payments.new.dlq"

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX_EXCHANGE.name,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)

DLQ_QUEUE = RabbitQueue("payments.new.dlq", durable=True, routing_key=DLQ_ROUTING_KEY)


def retry_delay_seconds(attempt: int) -> int:
    """Задержка перед следующей попыткой после неудачной попытки №attempt (экспоненциально)."""
    return settings.retry_base_delay_seconds * 2 ** (attempt - 1)


def retry_queue(attempt: int) -> RabbitQueue:
    return RabbitQueue(
        f"payments.retry.{attempt}",
        durable=True,
        arguments={
            "x-message-ttl": retry_delay_seconds(attempt) * 1000,
            # по истечении TTL сообщение возвращается в основную очередь
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": NEW_ROUTING_KEY,
        },
    )


async def declare_topology(broker: RabbitBroker) -> None:
    """Идемпотентно объявляет обменники, очереди и биндинги. Вызывать после broker.connect()."""
    payments_ex = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dlx_ex = await broker.declare_exchange(DLX_EXCHANGE)

    new_q = await broker.declare_queue(NEW_QUEUE)
    await new_q.bind(payments_ex, routing_key=NEW_ROUTING_KEY)

    dlq = await broker.declare_queue(DLQ_QUEUE)
    await dlq.bind(dlx_ex, routing_key=DLQ_ROUTING_KEY)

    # N попыток => N-1 очередей задержки (после последней неудачи — сразу в DLQ)
    for attempt in range(1, settings.max_attempts):
        await broker.declare_queue(retry_queue(attempt))
