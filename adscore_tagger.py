"""Claude Vision API integration for banner element tagging."""

import base64
import json
import logging
import os
from io import BytesIO
from pathlib import Path

logger = logging.getLogger(__name__)

TAGGING_PROMPT = """Проанализируй этот рекламный баннер и извлеки структурированную информацию.

Верни JSON-объект строго в такой структуре (все строковые значения на РУССКОМ языке):
{
  "visual": {
    "has_faces": boolean,
    "n_people": integer (0 если нет людей),
    "background_type": "сплошной цвет" | "градиент" | "фото" | "паттерн" | "абстрактный",
    "background_color": "#hex или null",
    "objects": ["список", "основных", "объектов на русском"],
    "color_scheme": "тёплая" | "холодная" | "нейтральная" | "яркая" | "тёмная" | "пастельная",
    "dominant_colors": ["#hex1", "#hex2", "#hex3"]
  },
  "text_elements": {
    "headline": "основной заголовок или null",
    "subtitle": "подзаголовок или null",
    "offer": "скидка/цена или null",
    "cta_text": "текст CTA-кнопки или null",
    "has_urgency_words": boolean,
    "urgency_words": ["список слов срочности, пустой если нет"]
  },
  "structural": {
    "has_cta_button": boolean,
    "cta_button_color": "#hex или null",
    "cta_position": "верх-лево" | "верх-центр" | "верх-право" | "центр" | "низ-лево" | "низ-центр" | "низ-право" | null,
    "has_logo": boolean,
    "logo_position": "верх-лево" | "верх-центр" | "верх-право" | "низ-лево" | "низ-центр" | "низ-право" | null,
    "text_image_ratio": float от 0.0 до 1.0 (доля текста от общей площади)
  },
  "emotional": {
    "tonality": "позитивная" | "нейтральная" | "тревожная" | "игривая" | "профессиональная" | "премиальная",
    "has_smiling_face": boolean,
    "energy_level": "высокая" | "средняя" | "низкая"
  }
}

Верни ТОЛЬКО JSON-объект. Без markdown-обрамления, без пояснений, без лишнего текста."""


def _get_media_type(image_path: str) -> str:
    ext = Path(image_path).suffix.lower()
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
    }.get(ext, "image/png")


def _resize_image(source, max_size: int = 1568) -> bytes:
    """Resize image so longest side is max_size pixels. Returns bytes.

    source: file path (str) or raw image bytes.
    """
    from PIL import Image
    Image.MAX_IMAGE_PIXELS = 25_000_000  # ~5000x5000 max, prevents decompression bombs

    if isinstance(source, bytes):
        img = Image.open(BytesIO(source))
    else:
        img = Image.open(source)

    with img:
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")

        w, h = img.size
        if max(w, h) > max_size:
            if w >= h:
                new_w = max_size
                new_h = int(h * max_size / w)
            else:
                new_h = max_size
                new_w = int(w * max_size / h)
            img = img.resize((new_w, new_h), Image.LANCZOS)

        buf = BytesIO()
        img.save(buf, format="JPEG", quality=85)
        return buf.getvalue()


def tag_banner(source) -> dict:
    """Send banner image to Claude Vision API and return parsed tags.

    source: file path (str) or raw image bytes.
    """
    import anthropic

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY environment variable is not set")

    logger.info("Tagging banner: %s", type(source).__name__)

    image_bytes = _resize_image(source)
    b64_data = base64.b64encode(image_bytes).decode("utf-8")
    # _resize_image always saves as JPEG regardless of original format
    media_type = "image/jpeg"

    client = anthropic.Anthropic(api_key=api_key)

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": b64_data,
                        },
                    },
                    {"type": "text", "text": TAGGING_PROMPT},
                ],
            }
        ],
    )

    response_text = message.content[0].text.strip()

    # Strip markdown fences if present
    if response_text.startswith("```"):
        lines = response_text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        response_text = "\n".join(lines)

    try:
        tags = json.loads(response_text)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse AI response: %s\nResponse: %s", e, response_text)
        raise ValueError(f"AI returned invalid JSON: {e}")

    logger.info("Successfully tagged banner: %s", type(source).__name__)
    return tags
