"""Consumer очереди payments.new — один обработчик, делающий всё:
получает сообщение → эмулирует шлюз → обновляет статус → шлёт webhook.

Retry / DLQ
-----------
Всего 3 попытки. Если попытка №n упала (БД недоступна, webhook вернул ошибку и т.п.),
сообщение публикуется в очередь ожидания `payments.retry.n` с TTL base * 2^(n-1) сек
(2 с, 4 с), по истечении TTL RabbitMQ сам возвращает его в `payments.new`.
После 3-й неудачи сообщение уходит в `payments.dlx` → `payments.dlq`.

Идемпотентность
---------------
Обработка безопасна при повторной доставке: статус меняется только из `pending`
(под SELECT ... FOR UPDATE), а повторная доставка для уже финального платежа
просто заново отправляет webhook.
"""
import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone

from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitMessage
from sqlalchemy import select

from app.broker import (
    ROUTING_KEY_FAILED,
    create_broker,
    declare_topology,
    dlx_exchange,
    payments_exchange,
    payments_queue,
    retry_queue_name,
)
from app.config import settings
from app.db import SessionLocal
from app.models import Payment, PaymentStatus
from app.schemas import PaymentEvent, PaymentOut
from app.webhook import send_webhook

logger = logging.getLogger("consumer")

broker = create_broker()
app = FastStream(broker)


class PaymentNotFound(Exception):
    """Платежа нет в БД — повторять бессмысленно."""


@app.on_startup
async def setup_topology() -> None:
    # on_startup выполняется до старта подписчиков: очереди/обменники/DLQ уже готовы
    await broker.connect()
    await declare_topology(broker)


async def emulate_gateway() -> bool:
    """Эмуляция внешнего платёжного шлюза: 2–5 секунд, 90% успех."""
    await asyncio.sleep(random.uniform(settings.gateway_min_sec, settings.gateway_max_sec))
    return random.random() < settings.gateway_success_rate


async def _load_payment(payment_id: uuid.UUID) -> Payment:
    async with SessionLocal() as session:
        payment = await session.get(Payment, payment_id)
    if payment is None:
        raise PaymentNotFound(str(payment_id))
    return payment


async def _finalize(payment_id: uuid.UUID, succeeded: bool) -> Payment:
    async with SessionLocal() as session, session.begin():
        payment = await session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
        if payment is None:
            raise PaymentNotFound(str(payment_id))
        if payment.status == PaymentStatus.PENDING:  # защита от гонки двух доставок
            payment.status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
            payment.processed_at = datetime.now(timezone.utc)
        return payment


async def process_payment(payment_id: uuid.UUID) -> None:
    payment = await _load_payment(payment_id)

    if payment.status == PaymentStatus.PENDING:
        # сессия закрыта — на время "обращения к шлюзу" соединение с БД не занято
        succeeded = await emulate_gateway()
        payment = await _finalize(payment_id, succeeded)
    else:
        logger.info("payment %s already %s, only re-sending webhook", payment_id, payment.status.value)

    payload = PaymentOut.model_validate(payment).model_dump(mode="json")
    payload["event"] = f"payment.{payment.status.value}"
    await send_webhook(payment.webhook_url, payload)
    logger.info("payment %s -> %s, webhook delivered", payment_id, payment.status.value)


@broker.subscriber(
    payments_queue,
    payments_exchange,
    # необработанное исключение (напр. битое тело) -> reject без requeue -> DLX -> DLQ
    ack_policy=AckPolicy.REJECT_ON_ERROR,
)
async def handle_new_payment(event: PaymentEvent, message: RabbitMessage) -> None:
    attempt = int(message.headers.get("x-attempt", 1))
    body = event.model_dump(mode="json")

    try:
        await process_payment(event.payment_id)
    except PaymentNotFound as exc:
        logger.error("payment not found, dead-lettering: %s", exc)
        await _to_dlq(body, attempt, exc)
    except Exception as exc:  # noqa: BLE001
        if attempt >= settings.max_attempts:
            logger.error("attempt %d/%d failed, giving up: %r", attempt, settings.max_attempts, exc)
            await _to_dlq(body, attempt, exc)
        else:
            logger.warning("attempt %d/%d failed: %r — scheduling retry", attempt, settings.max_attempts, exc)
            await broker.publish(
                body,
                queue=retry_queue_name(attempt),  # default exchange: routing key = имя очереди
                persist=True,
                headers={"x-attempt": attempt + 1},
            )


async def _to_dlq(body: dict, attempt: int, exc: Exception) -> None:
    await broker.publish(
        body,
        exchange=dlx_exchange,
        routing_key=ROUTING_KEY_FAILED,
        persist=True,
        headers={"x-attempt": attempt, "x-error": repr(exc)[:500]},
    )
