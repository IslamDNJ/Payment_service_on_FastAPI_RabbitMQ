import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI

from app.api import router
from app.broker import create_broker, declare_topology
from app.outbox import run_outbox_relay

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    broker = create_broker()
    await broker.connect()
    await declare_topology(broker)
    relay = asyncio.create_task(run_outbox_relay(broker), name="outbox-relay")
    try:
        yield
    finally:
        relay.cancel()
        with suppress(asyncio.CancelledError):
            await relay
        await broker.stop()


app = FastAPI(title="Payment processing service", version="1.0.0", lifespan=lifespan)
app.include_router(router)
