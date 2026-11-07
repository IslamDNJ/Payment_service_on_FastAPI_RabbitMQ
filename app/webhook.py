import httpx

from app.config import settings


async def send_webhook(url: str, payload: dict) -> None:
    """Отправляет уведомление. Любой не-2xx ответ или сетевая ошибка — исключение."""
    async with httpx.AsyncClient(timeout=settings.webhook_timeout_sec) as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()
