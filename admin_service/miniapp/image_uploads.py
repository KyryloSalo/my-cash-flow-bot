from __future__ import annotations

from io import BytesIO

from PIL import Image, UnidentifiedImageError
from pi_heif import register_heif_opener


AI_IMAGE_MAX_OUTPUT_BYTES = 10 * 1024 * 1024
AI_IMAGE_MAX_PIXELS = 25_000_000
HEIF_MIME_TYPE = "image/heic"
HEIF_BRANDS = {
    b"heic",
    b"heix",
    b"hevc",
    b"hevx",
    b"heim",
    b"heis",
    b"hevm",
    b"hevs",
}

register_heif_opener(thumbnails=False)


class MiniAppImageUploadError(ValueError):
    pass


def detect_heif_mime(image_bytes: bytes) -> str | None:
    """Detect HEIC/HEIF by ISO-BMFF brands instead of trusting file metadata."""
    if len(image_bytes) < 12 or image_bytes[4:8] != b"ftyp":
        return None

    brands = {image_bytes[8:12]}
    compatible_end = min(len(image_bytes), 128)
    brands.update(image_bytes[offset : offset + 4] for offset in range(16, compatible_end - 3, 4))
    return HEIF_MIME_TYPE if brands & HEIF_BRANDS else None


def normalize_ai_image_upload(image_bytes: bytes, mime_type: str) -> tuple[bytes, str]:
    """Convert HEIC/HEIF uploads to a bounded JPEG accepted by the Vision bridge."""
    if mime_type != HEIF_MIME_TYPE:
        return image_bytes, mime_type

    try:
        with Image.open(BytesIO(image_bytes)) as source:
            width, height = source.size
            if width <= 0 or height <= 0 or width * height > AI_IMAGE_MAX_PIXELS:
                raise MiniAppImageUploadError("The HEIC image dimensions are too large.")

            # libheif applies HEIF's own irot/imir presentation transforms.
            # EXIF orientation is informational for HEIF and can double-rotate it.
            normalized = source.copy()
            normalized.load()
            if normalized.mode != "RGB":
                normalized = normalized.convert("RGB")

            output = BytesIO()
            normalized.save(output, format="JPEG", quality=90, optimize=True)
            converted = output.getvalue()
    except MiniAppImageUploadError:
        raise
    except (
        Image.DecompressionBombError,
        UnidentifiedImageError,
        OSError,
        ValueError,
        SyntaxError,
        EOFError,
        RuntimeError,
    ) as exc:
        raise MiniAppImageUploadError("The HEIC image could not be decoded.") from exc

    if not converted or len(converted) > AI_IMAGE_MAX_OUTPUT_BYTES:
        raise MiniAppImageUploadError("The converted image is too large.")
    return converted, "image/jpeg"
