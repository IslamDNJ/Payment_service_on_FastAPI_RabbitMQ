"""Мини-приёмник webhook'ов для локальной проверки.

POST /hook          -> 200, печатает тело в лог
POST /hook?fail=1   -> 500 (для демонстрации retry и DLQ)
"""
import logging

from fastapi import FastAPI, HTTPException, Request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
app = FastAPI(title="Webhook mock")


@app.post("/hook")
async def hook(request: Request, fail: int = 0):
    body = await request.json()
    logging.info("WEBHOOK RECEIVED (fail=%s): %s", fail, body)
    if fail:
        raise HTTPException(500, "simulated failure")
    return {"ok": True}
