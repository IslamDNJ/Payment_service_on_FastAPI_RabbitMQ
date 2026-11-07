import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.models import OutboxEvent, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentEvent, PaymentOut
from app.security import verify_api_key

router = APIRouter(prefix="/api/v1", dependencies=[Depends(verify_api_key)])

SessionDep = Annotated[AsyncSession, Depends(get_session)]
IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)]


async def _find_by_key(session: AsyncSession, key: str) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.idempotency_key == key))


def _replay(existing: Payment, body: PaymentCreate) -> Payment:
    """Повторный запрос с тем же ключом: возвращаем исходный платёж.

    Если тело отличается — это ошибка клиента (ключ переиспользован), отвечаем 409.
    """
    same = (
        existing.amount == body.amount
        and existing.currency == body.currency
        and existing.description == body.description
        and existing.meta == body.metadata
        and existing.webhook_url == str(body.webhook_url)
    )
    if not same:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "Idempotency-Key was already used with a different request body",
        )
    return existing


@router.post(
    "/payments",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    summary="Создать платёж",
)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: IdempotencyKey,
    session: SessionDep,
) -> Payment:
    existing = await _find_by_key(session, idempotency_key)
    if existing:
        return _replay(existing, body)

    payment = Payment(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        meta=body.metadata,
        webhook_url=str(body.webhook_url),
        idempotency_key=idempotency_key,
    )
    session.add(payment)
    await session.flush()  # id генерируется на стороне приложения, created_at — БД

    # Платёж и событие — одна транзакция (Outbox pattern)
    session.add(
        OutboxEvent(
            event_type="payment.created",
            payload=PaymentEvent(payment_id=payment.id).model_dump(mode="json"),
        )
    )
    try:
        await session.commit()
    except IntegrityError:
        # гонка: параллельный запрос с тем же ключом успел закоммитить раньше
        await session.rollback()
        existing = await _find_by_key(session, idempotency_key)
        if existing is None:
            raise
        return _replay(existing, body)

    await session.refresh(payment)
    return payment


@router.get("/payments/{payment_id}", response_model=PaymentOut, summary="Получить платёж")
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return payment
