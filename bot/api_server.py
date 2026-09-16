from __future__ import annotations

import asyncio
import base64
import binascii
import logging
import re
from datetime import date, datetime

import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel

import config
from report_service import ReportService
from help_content import get_faq_catalog, get_faq_topic
from parsing import build_miniapp_text_parse_payload
from stt import transcribe_audio_bytes
from vision_tx import (
    AccountOption as VisionAccountOption,
    CategoryOption as VisionCategoryOption,
    OpenAIVisionConfigError,
    OpenAIVisionError,
    OpenAIVisionInvalidResponseError,
    analyze_screenshot,
    serialize_miniapp_screenshot_analysis,
)

logger = logging.getLogger("mcf.api")

MINIAPP_IMAGE_MAX_BYTES = 10 * 1024 * 1024
MINIAPP_IMAGE_MAX_BASE64_CHARS = ((MINIAPP_IMAGE_MAX_BYTES + 2) // 3) * 4
MINIAPP_IMAGE_ALLOWED_MIME_TYPES = {"image/jpeg", "image/png", "image/webp", "image/gif"}
MINIAPP_AUDIO_MAX_BYTES = 10 * 1024 * 1024
MINIAPP_AUDIO_MAX_BASE64_CHARS = ((MINIAPP_AUDIO_MAX_BYTES + 2) // 3) * 4
MINIAPP_AUDIO_ALLOWED_MIME_TYPES = {"audio/webm", "audio/ogg", "audio/mp4"}
MINIAPP_CURRENCY_CODE_PATTERN = re.compile(r"^[A-Z]{3,10}$")


async def _get_pool() -> asyncpg.Pool:
    if not config.DATABASE_URL:
        raise RuntimeError("DATABASE_URL missing")
    # The bot owns the current production schema. These API-only legacy SQL
    # migrations target a retired UUID schema and must never run at API startup.
    return await asyncpg.create_pool(dsn=config.DATABASE_URL, min_size=1, max_size=5)


async def require_admin(x_admin_token: str | None = Header(default=None)) -> None:
    if not config.ADMIN_TOKEN:
        raise HTTPException(status_code=503, detail="ADMIN_TOKEN not configured")
    if x_admin_token != config.ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


async def require_internal_token(x_internal_token: str | None = Header(default=None)) -> None:
    if not config.BILLING_INTERNAL_TOKEN:
        raise HTTPException(status_code=503, detail="BILLING_INTERNAL_TOKEN not configured")
    if x_internal_token != config.BILLING_INTERNAL_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


class MiniAppExportRequest(BaseModel):
    telegram_user_id: int
    date_from: date
    date_to_exclusive: date
    title: str


class MiniAppHelpRequest(BaseModel):
    locale: str = "uk"
    topic_id: str | None = None


class MiniAppTextParseRequest(BaseModel):
    text: str
    default_currency: str = "UAH"


class MiniAppVisionAccountRequest(BaseModel):
    id: int
    label: str


class MiniAppVisionCategoryRequest(BaseModel):
    id: int
    name: str
    aliases: list[str] = []


class MiniAppImageParseRequest(BaseModel):
    image_base64: str
    mime_type: str
    default_currency: str = "UAH"
    accounts: list[MiniAppVisionAccountRequest] = []
    expense_categories: list[MiniAppVisionCategoryRequest] = []
    income_categories: list[MiniAppVisionCategoryRequest] = []


class MiniAppAudioParseRequest(BaseModel):
    audio_base64: str
    mime_type: str
    default_currency: str = "UAH"


def _decode_miniapp_image(payload: MiniAppImageParseRequest) -> tuple[bytes, str, str]:
    mime_type = str(payload.mime_type or "").strip().lower()
    if mime_type == "image/jpg":
        mime_type = "image/jpeg"
    if mime_type not in MINIAPP_IMAGE_ALLOWED_MIME_TYPES:
        raise ValueError("Unsupported image type")

    encoded = str(payload.image_base64 or "").strip()
    if not encoded:
        raise ValueError("Image is required")
    if len(encoded) > MINIAPP_IMAGE_MAX_BASE64_CHARS:
        raise ValueError("Image is too large")
    try:
        image_bytes = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Image payload is invalid") from exc
    if not image_bytes:
        raise ValueError("Image is required")
    if len(image_bytes) > MINIAPP_IMAGE_MAX_BYTES:
        raise ValueError("Image is too large")

    default_currency = str(payload.default_currency or "").strip().upper()
    if not MINIAPP_CURRENCY_CODE_PATTERN.fullmatch(default_currency):
        default_currency = "UAH"
    return image_bytes, mime_type, default_currency


def _normalized_miniapp_audio_mime(value: object) -> str:
    mime_type = str(value or "").split(";", 1)[0].strip().lower()
    aliases = {
        "audio/oga": "audio/ogg",
        "audio/m4a": "audio/mp4",
        "audio/x-m4a": "audio/mp4",
    }
    return aliases.get(mime_type, mime_type)


def _detect_miniapp_audio_mime(audio_bytes: bytes) -> str | None:
    if audio_bytes.startswith(b"OggS"):
        return "audio/ogg"
    if audio_bytes.startswith(b"\x1aE\xdf\xa3") and b"webm" in audio_bytes[:4096].lower():
        return "audio/webm"
    if len(audio_bytes) >= 12 and audio_bytes[4:8] == b"ftyp":
        return "audio/mp4"
    return None


def _decode_miniapp_audio(payload: MiniAppAudioParseRequest) -> tuple[bytes, str, str, str]:
    declared_mime_type = _normalized_miniapp_audio_mime(payload.mime_type)
    if declared_mime_type not in MINIAPP_AUDIO_ALLOWED_MIME_TYPES:
        raise ValueError("Unsupported audio type")

    encoded = str(payload.audio_base64 or "").strip()
    if not encoded:
        raise ValueError("Audio is required")
    if len(encoded) > MINIAPP_AUDIO_MAX_BASE64_CHARS:
        raise ValueError("Audio is too large")
    try:
        audio_bytes = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Audio payload is invalid") from exc
    if not audio_bytes:
        raise ValueError("Audio is required")
    if len(audio_bytes) > MINIAPP_AUDIO_MAX_BYTES:
        raise ValueError("Audio is too large")

    detected_mime_type = _detect_miniapp_audio_mime(audio_bytes)
    if detected_mime_type is None or detected_mime_type != declared_mime_type:
        raise ValueError("Audio type does not match its contents")

    default_currency = str(payload.default_currency or "").strip().upper()
    if not MINIAPP_CURRENCY_CODE_PATTERN.fullmatch(default_currency):
        default_currency = "UAH"
    filename = {"audio/webm": "voice.webm", "audio/ogg": "voice.ogg", "audio/mp4": "voice.m4a"}[detected_mime_type]
    return audio_bytes, detected_mime_type, filename, default_currency


def _vision_accounts(items: list[MiniAppVisionAccountRequest]) -> list[VisionAccountOption]:
    options: list[VisionAccountOption] = []
    for item in items[:100]:
        label = str(item.label or "").strip()[:120]
        if item.id > 0 and label:
            options.append(VisionAccountOption(id=int(item.id), label=label))
    return options


def _vision_categories(items: list[MiniAppVisionCategoryRequest], *, kind: str) -> list[VisionCategoryOption]:
    options: list[VisionCategoryOption] = []
    for item in items[:200]:
        name = str(item.name or "").strip()[:120]
        if item.id > 0 and name:
            aliases = [str(alias).strip()[:120] for alias in item.aliases[:40] if str(alias).strip()]
            options.append(VisionCategoryOption(id=int(item.id), kind=kind, name=name, aliases=aliases))
    return options


def create_app() -> FastAPI:
    app = FastAPI(title="MCF API (v0)")

    @app.on_event("startup")
    async def _startup() -> None:
        app.state.pool = await _get_pool()
        logger.info("API pool ready")

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        pool: asyncpg.Pool | None = getattr(app.state, "pool", None)
        if pool:
            await pool.close()

    @app.get("/health")
    async def health():
        try:
            pool: asyncpg.Pool = app.state.pool
            async with asyncio.timeout(3):
                async with pool.acquire() as conn:
                    v = await conn.fetchval("SELECT 1")
            if v != 1:
                raise RuntimeError("Dependency check failed")
        except Exception:
            return JSONResponse({"status": "degraded", "db": "unavailable"}, status_code=503)
        return {"status": "ok", "db": v}

    @app.get("/live")
    async def live():
        return {"alive": True}

    # Legacy admin financial readers removed: no approved operational consumer.
    # Keep only scoped, authenticated Mini App bridges.

    @app.post("/internal/miniapp/export", dependencies=[Depends(require_internal_token)])
    async def miniapp_export(payload: MiniAppExportRequest) -> Response:
        if payload.telegram_user_id <= 0:
            raise HTTPException(status_code=400, detail="telegram_user_id must be positive")
        if payload.date_to_exclusive <= payload.date_from:
            raise HTTPException(status_code=400, detail="Invalid export period")
        if (payload.date_to_exclusive - payload.date_from).days > 366:
            raise HTTPException(status_code=400, detail="Export period is too long")

        title = str(payload.title or "").strip()[:120] or "Export"
        pool: asyncpg.Pool = app.state.pool
        async with pool.acquire() as conn:
            report_result = await ReportService(conn).build_export_xlsx(
                payload.telegram_user_id,
                payload.date_from,
                payload.date_to_exclusive,
                title,
            )
        if report_result is None:
            raise HTTPException(status_code=404, detail="No operations for the selected export period")

        return Response(
            content=report_result.content,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": 'attachment; filename="my-cash-flow-export.xlsx"',
                "X-Export-Row-Count": str(report_result.row_count),
            },
        )

    @app.post("/internal/miniapp/export-csv", dependencies=[Depends(require_internal_token)])
    async def miniapp_export_csv(payload: MiniAppExportRequest) -> Response:
        if payload.telegram_user_id <= 0:
            raise HTTPException(status_code=400, detail="telegram_user_id must be positive")
        if payload.date_to_exclusive <= payload.date_from:
            raise HTTPException(status_code=400, detail="Invalid export period")
        if (payload.date_to_exclusive - payload.date_from).days > 366:
            raise HTTPException(status_code=400, detail="Export period is too long")

        pool: asyncpg.Pool = app.state.pool
        async with pool.acquire() as conn:
            content, row_count = await ReportService(conn).build_export_csv(
                payload.telegram_user_id,
                payload.date_from,
                payload.date_to_exclusive,
            )
        if content is None or row_count <= 0:
            raise HTTPException(status_code=404, detail="No operations for the selected export period")

        return Response(
            content=content,
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": 'attachment; filename="my-cash-flow-export.csv"',
                "X-Export-Row-Count": str(row_count),
            },
        )

    @app.post("/internal/miniapp/help", dependencies=[Depends(require_internal_token)])
    async def miniapp_help(payload: MiniAppHelpRequest) -> dict:
        locale = "en" if str(payload.locale or "").strip().lower().startswith("en") else "uk"
        catalog = get_faq_catalog(locale)
        topic_id = str(payload.topic_id or "").strip()
        if not topic_id:
            return {
                "mode": "index",
                "locale": catalog.locale,
                "title": catalog.home_title,
                "intro": catalog.home_intro,
                "topics": [
                    {
                        "id": topic.id,
                        "title": topic.title,
                        "question_count": len(topic.questions),
                    }
                    for topic in catalog.topics
                ],
            }
        topic = get_faq_topic(topic_id, locale=locale)
        if topic is None:
            raise HTTPException(status_code=404, detail="Help topic not found")
        return {
            "mode": "topic",
            "locale": catalog.locale,
            "title": topic.title,
            "questions": [
                {"id": question.id, "title": question.title, "answer": question.answer}
                for question in topic.questions
            ],
        }

    @app.post("/internal/miniapp/parse-text", dependencies=[Depends(require_internal_token)])
    async def miniapp_parse_text(payload: MiniAppTextParseRequest) -> dict:
        try:
            return build_miniapp_text_parse_payload(
                text=payload.text,
                today=datetime.now().date(),
                default_currency=payload.default_currency,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/internal/miniapp/parse-audio", dependencies=[Depends(require_internal_token)])
    async def miniapp_parse_audio(payload: MiniAppAudioParseRequest) -> dict:
        try:
            audio_bytes, mime_type, filename, default_currency = _decode_miniapp_audio(payload)
            transcription = await transcribe_audio_bytes(
                audio_bytes,
                filename=filename,
                mime_type=mime_type,
            )
            transcript = transcription.text.strip()
            if len(transcript) > 500:
                raise ValueError("Voice transcript is too long")
            parsed = build_miniapp_text_parse_payload(
                text=transcript,
                today=datetime.now().date(),
                default_currency=default_currency,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            logger.warning("Mini App audio parsing failed: %s", exc)
            raise HTTPException(status_code=503, detail="Audio recognition is unavailable") from exc
        return {"transcript": transcript, "parsed": parsed}

    @app.post("/internal/miniapp/parse-image", dependencies=[Depends(require_internal_token)])
    async def miniapp_parse_image(payload: MiniAppImageParseRequest) -> dict:
        try:
            image_bytes, mime_type, default_currency = _decode_miniapp_image(payload)
            analysis = await analyze_screenshot(
                image_bytes,
                mime_type=mime_type,
                today=datetime.now().date(),
                default_currency=default_currency,
                accounts=_vision_accounts(payload.accounts),
                expense_categories=_vision_categories(payload.expense_categories, kind="expense"),
                income_categories=_vision_categories(payload.income_categories, kind="income"),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except OpenAIVisionConfigError as exc:
            logger.warning("Mini App image parsing unavailable: %s", exc)
            raise HTTPException(status_code=503, detail="Image recognition is unavailable") from exc
        except (OpenAIVisionError, OpenAIVisionInvalidResponseError) as exc:
            logger.warning("Mini App image parsing failed: %s", exc)
            raise HTTPException(status_code=503, detail="Image recognition is unavailable") from exc
        return serialize_miniapp_screenshot_analysis(analysis)

    return app
