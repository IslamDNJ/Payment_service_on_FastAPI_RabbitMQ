# Асинхронный сервис процессинга платежей

FastAPI + Pydantic v2 · SQLAlchemy 2.0 (async) · PostgreSQL · RabbitMQ (FastStream) · Alembic · Docker Compose

## Запуск

```bash
copy .env.example .env        # при необходимости поменяйте API_KEY
docker compose up --build
```

Поднимаются: `postgres`, `rabbitmq` (UI: http://localhost:15672, guest/guest), `migrate` (одноразово применяет Alembic-миграции), `api` (http://localhost:8000, Swagger: `/docs`), `consumer`.

## Примеры

```bash
# создать платёж (202 Accepted)
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: super-secret-key" \
  -H "Idempotency-Key: order-1001" \
  -H "Content-Type: application/json" \
  -d '{"amount": "1500.00", "currency": "RUB", "description": "Заказ №1001",
       "metadata": {"order_id": 1001}, "webhook_url": "https://webhook.site/<your-id>"}'

# получить платёж
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: super-secret-key"
```

Повтор запроса с тем же `Idempotency-Key` и тем же телом вернёт тот же платёж (202) без создания дубля;
с другим телом — `409 Conflict`. Без/с неверным `X-API-Key` — `401`, без `Idempotency-Key` — `422`.

Пример webhook (POST на `webhook_url`):
```json
{"event": "payment.succeeded", "payment_id": "…", "amount": "1500.00", "currency": "RUB",
 "description": "Заказ №1001", "metadata": {"order_id": 1001}, "status": "succeeded",
 "idempotency_key": "order-1001", "webhook_url": "…", "created_at": "…", "processed_at": "…"}
```
Удобно проверять через https://webhook.site. Webhook с хоста из контейнера: `http://host.docker.internal:PORT`.

## Архитектура

```
POST /payments ─► [ payments + outbox ]  (одна транзакция в PostgreSQL)
                        │
              outbox relay (в процессе api, SELECT … FOR UPDATE SKIP LOCKED)
                        │ publish + confirm
                        ▼
   exchange `payments` ─key payments.new─► queue `payments.new` ─► consumer
                                                                     │ 1. эмуляция шлюза (2–5 с, 90% ok)
                                                                     │ 2. UPDATE status (FOR UPDATE)
                                                                     │ 3. webhook
                          ┌──────────── ошибка ─────────────────────┘
                          ▼
   попытка 1 → `payments.retry.1` (TTL 2 с) ─┐
   попытка 2 → `payments.retry.2` (TTL 4 с) ─┴─► обратно в `payments.new`
   попытка 3 → exchange `payments.dlx` ─► queue `payments.dlq`
```

### Что где
| Требование | Реализация |
|---|---|
| Модели и миграции | `app/models.py`, `alembic/versions/0001_initial.py` (таблицы `payments`, `outbox`) |
| API | `app/api.py` |
| Auth | `app/security.py` — `X-API-Key` на всех эндпоинтах |
| Outbox | запись события в той же транзакции (`app/api.py`), доставка — `app/outbox.py` |
| Consumer | `app/consumer.py` |
| Retry | 3 попытки, задержки 2 с → 4 с через очереди ожидания с TTL (`app/broker.py`); счётчик в заголовке `x-attempt` |
| DLQ | `payments.dlx` → `payments.dlq`; сюда же уходят «ядовитые» сообщения (reject без requeue) и платёж, которого нет в БД. Причина — в заголовке `x-error` |
| Идемпотентность API | `UNIQUE(idempotency_key)` + проверка до вставки + обработка гонки через `IntegrityError` |
| Идемпотентность consumer | статус меняется только из `pending` под `FOR UPDATE`; повторная доставка финального платежа лишь повторно шлёт webhook |

### Гарантии и компромиссы
- **At-least-once.** Relay публикует с подтверждением брокера и только потом помечает событие `published_at`. При падении между этими шагами событие уйдёт повторно — consumer это переживает. Webhook-получатель тоже должен быть идемпотентным (ключ — `payment_id`).
- Relay запущен внутри `api`; благодаря `SKIP LOCKED` можно масштабировать api горизонтально. Для продакшена relay удобнее вынести в отдельный процесс.
- Соединение с БД не удерживается во время «обращения к шлюзу» (sleep 2–5 с).
- Параметры (задержки, число попыток, вероятность успеха) — в `app/config.py`, переопределяются переменными окружения.

## Локальная разработка без Docker
```bash
pip install -r requirements.txt
export DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments
export RABBITMQ_URL=amqp://guest:guest@localhost:5672/
alembic upgrade head
uvicorn app.main:app --reload          # терминал 1
faststream run app.consumer:app        # терминал 2
```

## Как проверить retry и DLQ
Укажите `webhook_url`, который отвечает 500 (например, `http://host.docker.internal:9999/x` при незапущенном сервере):
в логах consumer будут попытки 1/3 и 2/3, затем сообщение окажется в `payments.dlq` (видно в RabbitMQ UI → Queues).

## Скриншоты работоспособности сервиса
![localhost:8000/docs](IMG/localhost8000.png)
![aut](IMG/aut.png)
![post](IMG/post.png)
![test1/3](IMG/test1_3.png)
![test2/3](IMG/test2_3.png)
![test3/3](IMG/test3_3.png)
![get_test1/3](IMG/test_get1_3.png)
![get_test2/3](IMG/test_get2_3.png)
![get_test3/3](IMG/test_get3_3.png)