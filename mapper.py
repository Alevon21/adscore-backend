"""Column mapping with synonym dictionary for auto-detection."""

import re
import logging
from typing import Dict, List, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

SYNONYMS: Dict[str, List[str]] = {
    "text_id": [
        "text_id", "textid", "id", "ad_id", "adid", "creative_id",
        "идентификатор", "ид",
    ],
    "headline": [
        "headline", "заголовок", "текст", "title", "text", "ad_text",
        "объявление", "creative", "ad_title", "ad_name", "name",
        "текст объявления", "креатив",
    ],
    "campaign": [
        "campaign", "кампания", "camp", "campaign_name", "camp_id",
        "campaign_id", "рекламная кампания",
    ],
    "platform": [
        "platform", "платформа", "source", "источник", "площадка",
        "network", "канал",
    ],
    "device": [
        "device", "устройство", "os", "девайс",
    ],
    "date_from": [
        "date_from", "date", "дата", "day", "день",
        "дата начала", "start_date", "начало", "from",
        "дата_от", "period_start",
    ],
    "date_to": [
        "date_to", "дата конца", "end_date", "конец", "to",
        "дата_до", "period_end",
    ],
    "impressions": [
        "impressions", "показы", "imp", "imps", "views", "показ",
        "кол-во показов", "количество показов", "impression",
        "показы (тыс.)", "shows",
    ],
    "clicks": [
        "clicks", "клики", "ctr_abs", "click", "кликов",
        "кол-во кликов", "количество кликов", "переходы",
    ],
    "spend": [
        "spend", "расход", "cost", "затраты", "бюджет", "budget",
        "расход (руб.)", "расход (руб)", "стоимость", "costs",
    ],
    "installs": [
        "installs", "установки", "install", "инсталлы",
        "кол-во установок", "количество установок",
    ],
    # Event slots — backward compatible with old column names
    "event_1": [
        "event_1", "registrations", "регистрации", "regs", "conversions",
        "конверсии", "целевые действия", "goals", "leads",
        "лиды", "заявки", "reg",
    ],
    "event_2": [
        "event_2", "buy_events", "покупки", "purchases", "purchase",
        "события покупки", "buy",
    ],
    "event_3": [
        "event_3", "buy_checks", "оплаты", "чеки", "checks", "payments",
        "оплат",
    ],
    "event_4": [
        "event_4",
    ],
    "revenue": [
        "revenue", "доход", "выручка", "rev", "income", "rpc_sum",
        "доход (руб.)", "доход (руб)", "money",
    ],
    "banner_url": [
        "banner_url", "banner", "image_url", "image", "creative_url",
        "баннер", "ссылка на баннер", "изображение", "banner_link",
    ],
}

REQUIRED_FIELDS = {"impressions", "clicks"}

EVENT_SLOTS = ["event_1", "event_2", "event_3", "event_4"]

# Smart label detection based on original column name
EVENT_LABEL_HINTS = {
    "registrations": "Регистрации",
    "регистрации": "Регистрации",
    "regs": "Регистрации",
    "conversions": "Конверсии",
    "конверсии": "Конверсии",
    "leads": "Лиды",
    "лиды": "Лиды",
    "заявки": "Заявки",
    "goals": "Цели",
    "buy_events": "Покупки",
    "покупки": "Покупки",
    "purchases": "Покупки",
    "buy_checks": "Оплаты",
    "оплаты": "Оплаты",
    "checks": "Чеки",
    "payments": "Платежи",
}


def _normalize(s: str) -> str:
    """Normalize a column header for matching: lowercase, strip, remove special chars."""
    s = s.strip().lower()
    s = re.sub(r"[^a-zа-яё0-9_\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def detect_event_label(original_col: str) -> str:
    """Try to infer a meaningful Russian label from the original column name."""
    norm = _normalize(original_col)
    for hint, label in EVENT_LABEL_HINTS.items():
        if norm == _normalize(hint):
            return label
    return original_col


class ColumnMapper:
    """Maps arbitrary CSV/XLSX column headers to system fields via synonym dictionary."""

    def auto_map(self, columns: List[str]) -> Dict[str, str]:
        """
        Auto-map file columns to system fields using SYNONYMS.
        Returns {system_field: original_column_name}.
        """
        mapping: Dict[str, str] = {}
        used_columns: set = set()

        for col in columns:
            norm = _normalize(col)
            for sys_field, synonyms in SYNONYMS.items():
                if sys_field in mapping:
                    continue
                for syn in synonyms:
                    if norm == _normalize(syn) or norm == sys_field:
                        mapping[sys_field] = col
                        used_columns.add(col)
                        break
                if sys_field in mapping:
                    break

        logger.info("Auto-mapped %d of %d columns: %s", len(mapping), len(columns), mapping)
        return mapping

    def detect_events(self, mapping: Dict[str, str]) -> List[Dict]:
        """
        Detect which event slots were auto-mapped and return event configs
        with smart labels based on original column names.
        """
        events = []
        for slot in EVENT_SLOTS:
            if slot in mapping:
                original_col = mapping[slot]
                label = detect_event_label(original_col)
                events.append({
                    "slot": slot,
                    "label": label,
                    "is_primary": len(events) == 0,
                })
        return events

    def apply_mapping(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Rename columns in the DataFrame according to the mapping.
        mapping: {system_field: original_column_name}
        """
        rename_dict = {orig: sys for sys, orig in mapping.items()}
        df_mapped = df.rename(columns=rename_dict)

        sys_cols = list(mapping.keys())
        extra_cols = [c for c in df_mapped.columns if c not in sys_cols]
        keep_cols = sys_cols + extra_cols
        existing = [c for c in keep_cols if c in df_mapped.columns]
        return df_mapped[existing].copy()

    def validate_mapping(self, mapping: Dict[str, str]) -> Tuple[bool, List[str]]:
        """
        Check that required fields are mapped.
        Returns (is_valid, list_of_missing_required_fields).
        """
        missing = [f for f in REQUIRED_FIELDS if f not in mapping]
        return (len(missing) == 0, missing)

    def get_unmapped_columns(
        self, columns: List[str], mapping: Dict[str, str]
    ) -> List[str]:
        """Return columns that were not auto-mapped."""
        mapped_originals = set(mapping.values())
        return [c for c in columns if c not in mapped_originals]
