"""Outbox relay: переносит события из таблицы outbox в RabbitMQ.

Событие записывается в БД в той же транзакции, что и платёж, поэтому оно не может
потеряться при падении между "сохранили платёж" и "отправили в брокер".
Relay доставляет at-least-once (возможны дубли при падении между publish и commit) —
дубли безопасно гасит идемпотентный consumer.
"""
import asyncio
import logging

from faststream.rabbit import RabbitBroker
from sqlalchemy import func, select

from app.broker import ROUTING_KEY_NEW, payments_exchange
from app.config import settings
from app.db import SessionLocal
from app.models import OutboxEvent

logger = logging.getLogger("outbox")


async def relay_once(broker: RabbitBroker) -> int:
    async with SessionLocal() as session, session.begin():
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(settings.outbox_batch_size)
            # несколько инстансов API не будут брать одни и те же строки
            .with_for_update(skip_locked=True)
        )
        events = (await session.scalars(stmt)).all()
        for event in events:
            # публикация с publisher confirm: await возвращается после подтверждения брокера
            await broker.publish(
                event.payload,
                exchange=payments_exchange,
                routing_key=ROUTING_KEY_NEW,
                persist=True,
                message_id=str(event.id),
                message_type=event.event_type,
                headers={"x-attempt": 1},
            )
            event.published_at = func.now()
        return len(events)


async def run_outbox_relay(broker: RabbitBroker) -> None:
    logger.info("outbox relay started")
    while True:
        published = 0
        try:
            published = await relay_once(broker)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("outbox relay iteration failed, will retry")
        if published < settings.outbox_batch_size:
            await asyncio.sleep(settings.outbox_poll_interval)
