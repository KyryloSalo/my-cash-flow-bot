from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from config import OPENAI_API_KEY, OPENAI_STT_MODEL

logger = logging.getLogger("mcf.stt")


@dataclass(frozen=True)
class SttResult:
    text: str


async def transcribe_ogg_bytes(ogg_bytes: bytes) -> SttResult:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
    data = {"model": OPENAI_STT_MODEL}
    files = {"file": ("voice.ogg", ogg_bytes, "audio/ogg")}

    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers=headers,
            data=data,
            files=files,
        )
        r.raise_for_status()
        payload = r.json()
        text = (payload.get("text") or "").strip()
        if not text:
            raise RuntimeError("Empty STT result")
        return SttResult(text=text)
