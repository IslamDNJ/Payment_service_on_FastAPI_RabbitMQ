from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: str = "super-secret-key"
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # outbox relay
    outbox_poll_interval: float = 1.0
    outbox_batch_size: int = 100

    # retry: всего 3 попытки, задержка перед n-й повторной = base * 2^(n-1) секунд
    max_attempts: int = 3
    retry_base_delay_sec: int = 2

    # эмуляция платёжного шлюза
    gateway_min_sec: float = 2.0
    gateway_max_sec: float = 5.0
    gateway_success_rate: float = 0.9

    webhook_timeout_sec: float = 5.0


settings = Settings()
