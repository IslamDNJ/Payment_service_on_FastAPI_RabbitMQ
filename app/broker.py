"""Топология RabbitMQ — общая для API (outbox relay) и consumer'а.

    exchange `payments` (direct)
        └─ key `payments.new` ──► queue `payments.new`  (x-dead-letter-exchange=payments.dlx)

    queue `payments.retry.1`  (TTL = base)      ─┐ по истечении TTL сообщение
    queue `payments.retry.2`  (TTL = base * 2)  ─┘ возвращается в `payments` / `payments.new`

    exchange `payments.dlx` (direct)
        └─ key `payments.failed` ──► queue `payments.dlq`
"""
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import settings

ROUTING_KEY_NEW = "payments.new"
ROUTING_KEY_FAILED = "payments.failed"

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
dlx_exchange = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

payments_queue = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=ROUTING_KEY_NEW,
    arguments={
        # nack/reject без requeue и истёкшие сообщения уходят в DLX
        "x-dead-letter-exchange": dlx_exchange.name,
        "x-dead-letter-routing-key": ROUTING_KEY_FAILED,
    },
)

dlq_queue = RabbitQueue("payments.dlq", durable=True, routing_key=ROUTING_KEY_FAILED)


def retry_queue_name(attempt: int) -> str:
    """Очередь ожидания после неудачной попытки №attempt."""
    return f"payments.retry.{attempt}"


def retry_queues() -> list[RabbitQueue]:
    queues = []
    for attempt in range(1, settings.max_attempts):
        delay_ms = settings.retry_base_delay_sec * (2 ** (attempt - 1)) * 1000
        queues.append(
            RabbitQueue(
                retry_queue_name(attempt),
                durable=True,
                arguments={
                    "x-message-ttl": delay_ms,
                    # по истечении TTL сообщение возвращается в основной поток
                    "x-dead-letter-exchange": payments_exchange.name,
                    "x-dead-letter-routing-key": ROUTING_KEY_NEW,
                },
            )
        )
    return queues


def create_broker() -> RabbitBroker:
    return RabbitBroker(settings.rabbitmq_url)


async def declare_topology(broker: RabbitBroker) -> None:
    """Идемпотентно объявляет всё, что не создаётся подпиской consumer'а."""
    await broker.declare_exchange(payments_exchange)
    dlx = await broker.declare_exchange(dlx_exchange)

    main = await broker.declare_queue(payments_queue)
    await main.bind(await broker.declare_exchange(payments_exchange), routing_key=ROUTING_KEY_NEW)

    dlq = await broker.declare_queue(dlq_queue)
    await dlq.bind(dlx, routing_key=ROUTING_KEY_FAILED)

    for q in retry_queues():
        await broker.declare_queue(q)
