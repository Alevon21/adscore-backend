#!/usr/bin/env python3
"""
Generate 10-12 diverse datasets with different event types,
run them through the real scorer, and collect all results for analysis.

Event types:
1. Подписка (subscription)
2. Первая транзакция (first_transaction)
3. Покупка (purchase)
4. Бронирование номера (booking)
5. Лид (lead)
6. Качественный лид (qualified_lead)
7. Продажа (sale)
"""

import sys
import os
import json
import random
import math
import numpy as np
import pandas as pd

# Add backend to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scorer import TextScorer
from models import ScoringParams, EventConfig

random.seed(42)
np.random.seed(42)

# ─── Dataset generators ───

def _gen_texts(n=30, prefix="txt"):
    """Generate text IDs and headlines."""
    headlines = [
        "Скидка {pct}% на первый заказ — только сегодня",
        "Бесплатная доставка от {amount}₽ — закажи сейчас",
        "Попробуй бесплатно {days} дней — без карты",
        "Выгода до {amount}₽ — успей до конца недели",
        "{product} со скидкой — количество ограничено",
        "Забронируй сейчас — лучшая цена",
        "Получи подарок при первом заказе",
        "Эксклюзивное предложение для новых клиентов",
        "Цены снижены — только до {date}",
        "Кэшбэк {pct}% на все покупки",
        "Начни зарабатывать прямо сейчас",
        "Твоя мечта ближе чем кажется",
        "Лучший выбор для вашего бизнеса",
        "Узнай как сэкономить до {amount}₽",
        "Присоединяйся к {count}+ довольных клиентов",
        "Не упусти шанс — осталось {count} мест",
        "Рассрочка 0% — без переплаты",
        "Профессиональная консультация бесплатно",
        "Новая коллекция уже в продаже",
        "Гарантия результата или возврат денег",
        "Быстрый старт за 5 минут",
        "Специальные условия для бизнеса",
        "Автоматизируй рутину — экономь время",
        "Получи доступ к закрытому контенту",
        "Инвестируй в будущее — начни сейчас",
        "Сравни и выбери лучшее предложение",
        "Бонус {amount}₽ на первый депозит",
        "Подключи за 1 минуту — без документов",
        "Проверенное решение от экспертов",
        "Результат с первого дня — гарантируем",
        "Тариф Про — всё включено",
        "Горячее предложение — минус {pct}%",
        "Закажи обратный звонок — перезвоним за 30 сек",
        "Оставь заявку — получи расчёт бесплатно",
        "Первый месяц за 1₽ — попробуй сейчас",
    ]
    texts = []
    for i in range(n):
        h = headlines[i % len(headlines)].format(
            pct=random.choice([10, 15, 20, 25, 30, 50]),
            amount=random.choice([500, 1000, 2000, 3000, 5000]),
            days=random.choice([7, 14, 30]),
            product=random.choice(["Курс", "Подписка", "Пакет", "Тариф"]),
            date=random.choice(["пятницы", "воскресенья", "месяца"]),
            count=random.choice([1000, 5000, 10000, 50000]),
        )
        texts.append({
            "text_id": f"{prefix}_{i+1:03d}",
            "headline": h,
        })
    return texts


def _gen_performance(n, profile):
    """
    Generate performance data with controlled distribution.
    profile dict controls the shape of the data.
    """
    imp_mean = profile.get("imp_mean", 5000)
    imp_std = profile.get("imp_std", 3000)
    ctr_mean = profile.get("ctr_mean", 0.03)
    ctr_std = profile.get("ctr_std", 0.015)
    cr_mean = profile.get("cr_mean", 0.05)
    cr_std = profile.get("cr_std", 0.03)
    cpc_mean = profile.get("cpc_mean", 30)
    cpc_std = profile.get("cpc_std", 15)
    zero_conv_pct = profile.get("zero_conv_pct", 0.1)  # % of texts with 0 conversions
    low_data_pct = profile.get("low_data_pct", 0.1)  # % of texts with low data
    anomaly_pct = profile.get("anomaly_pct", 0.03)  # % of texts with anomalous data

    rows = []
    for i in range(n):
        # Low data texts
        if random.random() < low_data_pct:
            imp = random.randint(50, 150)
            clicks = random.randint(2, 10)
        else:
            imp = max(100, int(np.random.normal(imp_mean, imp_std)))
            ctr = max(0.005, np.random.normal(ctr_mean, ctr_std))
            clicks = max(1, int(imp * ctr))

        # Spend based on CPC
        cpc = max(5, np.random.normal(cpc_mean, cpc_std))
        spend = round(clicks * cpc, 2)

        # Conversions
        if random.random() < zero_conv_pct:
            conv = 0
        elif random.random() < anomaly_pct:
            # Anomalous: very high CR
            conv = max(1, int(clicks * random.uniform(0.5, 0.9)))
        else:
            cr = max(0, np.random.normal(cr_mean, cr_std))
            conv = max(0, int(clicks * cr))

        rows.append({
            "impressions": imp,
            "clicks": clicks,
            "spend": spend,
            "conversions": conv,
        })

    return rows


# ─── DATASET DEFINITIONS ───

DATASETS = []

# 1. SaaS подписка (subscription) — high CTR, medium CR
DATASETS.append({
    "name": "01_saas_subscription",
    "event_label": "Подписка",
    "event_type": "subscription",
    "description": "SaaS-продукт: подписка на сервис. Средний чек 990₽/мес",
    "profile": {
        "imp_mean": 8000, "imp_std": 4000,
        "ctr_mean": 0.045, "ctr_std": 0.02,
        "cr_mean": 0.08, "cr_std": 0.04,
        "cpc_mean": 25, "cpc_std": 12,
        "zero_conv_pct": 0.1,
        "low_data_pct": 0.07,
    },
    "n_texts": 30,
    "has_revenue": False,
})

# 2. E-commerce первая транзакция — medium CTR, low CR, has revenue
DATASETS.append({
    "name": "02_ecom_first_transaction",
    "event_label": "Первая транзакция",
    "event_type": "first_transaction",
    "description": "Интернет-магазин электроники. Средний чек 5000₽",
    "profile": {
        "imp_mean": 12000, "imp_std": 5000,
        "ctr_mean": 0.025, "ctr_std": 0.012,
        "cr_mean": 0.03, "cr_std": 0.02,
        "cpc_mean": 40, "cpc_std": 20,
        "zero_conv_pct": 0.15,
        "low_data_pct": 0.1,
    },
    "n_texts": 30,
    "has_revenue": True,
    "avg_order": 5000,
})

# 3. Покупка в app — high volume, variable CR
DATASETS.append({
    "name": "03_app_purchase",
    "event_label": "Покупка",
    "event_type": "purchase",
    "description": "Мобильное приложение: покупка внутри приложения",
    "profile": {
        "imp_mean": 15000, "imp_std": 8000,
        "ctr_mean": 0.035, "ctr_std": 0.015,
        "cr_mean": 0.04, "cr_std": 0.025,
        "cpc_mean": 15, "cpc_std": 8,
        "zero_conv_pct": 0.2,
        "low_data_pct": 0.05,
        "anomaly_pct": 0.05,
    },
    "n_texts": 32,
    "has_revenue": True,
    "avg_order": 300,
})

# 4. Бронирование номера — low volume, high CR, expensive
DATASETS.append({
    "name": "04_hotel_booking",
    "event_label": "Бронирование номера",
    "event_type": "booking",
    "description": "Отель: бронирование номеров. Средний чек 8000₽/ночь",
    "profile": {
        "imp_mean": 3000, "imp_std": 1500,
        "ctr_mean": 0.02, "ctr_std": 0.01,
        "cr_mean": 0.06, "cr_std": 0.035,
        "cpc_mean": 80, "cpc_std": 40,
        "zero_conv_pct": 0.25,
        "low_data_pct": 0.15,
    },
    "n_texts": 30,
    "has_revenue": True,
    "avg_order": 8000,
})

# 5. Лид (заявка) — high CTR, high CR
DATASETS.append({
    "name": "05_lead_form",
    "event_label": "Лид",
    "event_type": "lead",
    "description": "Юридические услуги: заявка на консультацию",
    "profile": {
        "imp_mean": 6000, "imp_std": 3000,
        "ctr_mean": 0.055, "ctr_std": 0.025,
        "cr_mean": 0.12, "cr_std": 0.06,
        "cpc_mean": 50, "cpc_std": 25,
        "zero_conv_pct": 0.05,
        "low_data_pct": 0.05,
    },
    "n_texts": 30,
    "has_revenue": False,
})

# 6. Качественный лид — two events: lead + qualified_lead
DATASETS.append({
    "name": "06_qualified_lead",
    "event_label": "Качественный лид",
    "event_type": "qualified_lead",
    "description": "B2B SaaS: заявка → квалифицированный лид (2 события)",
    "profile": {
        "imp_mean": 5000, "imp_std": 2500,
        "ctr_mean": 0.03, "ctr_std": 0.015,
        "cr_mean": 0.07, "cr_std": 0.04,
        "cpc_mean": 60, "cpc_std": 30,
        "zero_conv_pct": 0.1,
        "low_data_pct": 0.1,
    },
    "n_texts": 30,
    "has_revenue": False,
    "multi_event": True,  # Two events
    "qual_rate": 0.35,  # 35% of leads become qualified
})

# 7. Продажа — full funnel with revenue
DATASETS.append({
    "name": "07_sale",
    "event_label": "Продажа",
    "event_type": "sale",
    "description": "Онлайн-курсы: от клика до оплаты. Средний чек 15000₽",
    "profile": {
        "imp_mean": 10000, "imp_std": 5000,
        "ctr_mean": 0.04, "ctr_std": 0.02,
        "cr_mean": 0.025, "cr_std": 0.015,
        "cpc_mean": 35, "cpc_std": 18,
        "zero_conv_pct": 0.2,
        "low_data_pct": 0.1,
    },
    "n_texts": 30,
    "has_revenue": True,
    "avg_order": 15000,
})

# 8. Подписка с low data — many texts with insufficient data
DATASETS.append({
    "name": "08_sub_low_data",
    "event_label": "Подписка",
    "event_type": "subscription",
    "description": "Новый SaaS: мало данных, большинство текстов не набрали статистику",
    "profile": {
        "imp_mean": 2000, "imp_std": 2000,
        "ctr_mean": 0.03, "ctr_std": 0.02,
        "cr_mean": 0.04, "cr_std": 0.03,
        "cpc_mean": 20, "cpc_std": 10,
        "zero_conv_pct": 0.35,
        "low_data_pct": 0.35,
    },
    "n_texts": 30,
    "has_revenue": False,
})

# 9. Бронирование + Покупка (multi-event) — hotel with upsell
DATASETS.append({
    "name": "09_booking_purchase",
    "event_label": "Бронирование + Покупка",
    "event_type": "booking_purchase",
    "description": "Отель: бронирование → покупка допуслуг (2 события)",
    "profile": {
        "imp_mean": 4000, "imp_std": 2000,
        "ctr_mean": 0.025, "ctr_std": 0.012,
        "cr_mean": 0.05, "cr_std": 0.03,
        "cpc_mean": 70, "cpc_std": 35,
        "zero_conv_pct": 0.2,
        "low_data_pct": 0.1,
    },
    "n_texts": 30,
    "has_revenue": True,
    "avg_order": 12000,
    "multi_event": True,
    "qual_rate": 0.4,  # 40% of bookings buy extras
})

# 10. Лид с аномалиями — dataset with suspicious patterns
DATASETS.append({
    "name": "10_lead_anomalies",
    "event_label": "Лид",
    "event_type": "lead",
    "description": "Недвижимость: много аномалий (подозрительно высокая конверсия)",
    "profile": {
        "imp_mean": 7000, "imp_std": 3500,
        "ctr_mean": 0.04, "ctr_std": 0.02,
        "cr_mean": 0.10, "cr_std": 0.08,
        "cpc_mean": 90, "cpc_std": 50,
        "zero_conv_pct": 0.1,
        "low_data_pct": 0.05,
        "anomaly_pct": 0.15,
    },
    "n_texts": 30,
    "has_revenue": False,
})

# 11. Покупка с extreme spread — few winners, many losers
DATASETS.append({
    "name": "11_purchase_extreme",
    "event_label": "Покупка",
    "event_type": "purchase",
    "description": "Маркетплейс: экстремальный разброс — 3 лидера и 27 аутсайдеров",
    "profile": {
        "imp_mean": 6000, "imp_std": 4000,
        "ctr_mean": 0.02, "ctr_std": 0.015,
        "cr_mean": 0.015, "cr_std": 0.012,
        "cpc_mean": 25, "cpc_std": 15,
        "zero_conv_pct": 0.3,
        "low_data_pct": 0.1,
    },
    "n_texts": 30,
    "has_revenue": True,
    "avg_order": 2500,
    "extreme_winners": 3,  # 3 texts get 5x performance
})

# 12. Продажа без расхода — minimal mode (no spend column)
DATASETS.append({
    "name": "12_sale_no_spend",
    "event_label": "Продажа",
    "event_type": "sale",
    "description": "Органический трафик: нет расходов, только показы/клики/конверсии",
    "profile": {
        "imp_mean": 10000, "imp_std": 5000,
        "ctr_mean": 0.05, "ctr_std": 0.025,
        "cr_mean": 0.06, "cr_std": 0.035,
        "cpc_mean": 0, "cpc_std": 0,
        "zero_conv_pct": 0.15,
        "low_data_pct": 0.05,
    },
    "n_texts": 30,
    "has_revenue": False,
    "no_spend": True,
})


def generate_dataset(ds_config):
    """Generate a full DataFrame for a dataset config."""
    n = ds_config["n_texts"]
    texts = _gen_texts(n, prefix=ds_config["name"].split("_")[0])
    perf = _gen_performance(n, ds_config["profile"])

    rows = []
    for i in range(n):
        row = {
            "text_id": texts[i]["text_id"],
            "headline": texts[i]["headline"],
            "impressions": perf[i]["impressions"],
            "clicks": perf[i]["clicks"],
        }

        if not ds_config.get("no_spend"):
            row["spend"] = perf[i]["spend"]

        conv = perf[i]["conversions"]

        # Extreme winners: boost 3 top texts
        if ds_config.get("extreme_winners") and i < ds_config["extreme_winners"]:
            row["impressions"] = int(row["impressions"] * 2)
            row["clicks"] = int(row["clicks"] * 3)
            if not ds_config.get("no_spend"):
                row["spend"] = round(row["spend"] * 1.5, 2)
            conv = max(conv * 5, int(row["clicks"] * 0.15))

        if ds_config.get("multi_event"):
            row["event_1"] = conv
            qual = max(0, int(conv * ds_config.get("qual_rate", 0.3) + np.random.normal(0, 1)))
            row["event_2"] = min(qual, conv)  # Can't have more qualified than raw
        else:
            row["event_1"] = conv

        if ds_config.get("has_revenue"):
            avg_order = ds_config.get("avg_order", 5000)
            # Revenue = conversions * avg_order * variation
            primary_conv = row.get("event_2", row.get("event_1", 0))
            if primary_conv == 0:
                primary_conv = row.get("event_1", 0)
            variation = max(0.3, np.random.normal(1.0, 0.3))
            row["revenue"] = round(primary_conv * avg_order * variation, 2) if primary_conv > 0 else 0

        rows.append(row)

    df = pd.DataFrame(rows)
    return df


def run_scorer(df, ds_config):
    """Run the scorer on a DataFrame and return results."""
    events = []

    if ds_config.get("multi_event"):
        events = [
            EventConfig(
                slot="event_1",
                label=ds_config["event_label"].split("+")[0].strip() if "+" in ds_config["event_label"] else ds_config["event_label"],
                is_primary=False,
            ),
            EventConfig(
                slot="event_2",
                label=ds_config["event_label"].split("+")[1].strip() if "+" in ds_config["event_label"] else "Качественный " + ds_config["event_label"],
                is_primary=True,
            ),
        ]
    else:
        events = [
            EventConfig(
                slot="event_1",
                label=ds_config["event_label"],
                is_primary=True,
            ),
        ]

    # Choose weight mode based on dataset
    if ds_config.get("has_revenue"):
        weight_mode = "goal_revenue"
    elif ds_config["event_type"] in ("lead", "qualified_lead"):
        weight_mode = "goal_conversions"
    else:
        weight_mode = "goal_conversions"

    params = ScoringParams(
        events=events,
        weight_mode=weight_mode,
        min_impressions=100,
        min_clicks=10,
        min_conversions=0,  # Don't filter by conversions to see all texts
    )

    scorer = TextScorer(params)
    result = scorer.score(df)

    return result, params


def analyze_single_dataset(ds_config):
    """Generate, score, and analyze a single dataset."""
    df = generate_dataset(ds_config)
    result, params = run_scorer(df, ds_config)

    analysis = {
        "name": ds_config["name"],
        "description": ds_config["description"],
        "event_type": ds_config["event_type"],
        "event_label": ds_config["event_label"],
        "n_total": len(df),
        "n_scored": result.stats.get("n_scored", 0),
        "n_excluded": result.stats.get("n_excluded", 0),
        "mode": result.stats.get("mode", "unknown"),
        "weight_mode": params.weight_mode,
        "texts": [],
        "issues": [],
    }

    for r in result.results:
        text_data = {
            "text_id": r.text_id,
            "headline": r.headline,
            "composite_score": r.composite_score,
            "decision_score": r.decision_score,
            "ranking_score": r.ranking_score,
            "category": r.category,
            "alt_category": r.alt_category,
            "verdict": r.verdict.verdict if r.verdict else None,
            "verdict_reason": r.verdict.reason if r.verdict else None,
            "verdict_reason_type": r.verdict.reason_type if r.verdict else None,
            "verdict_reason_detail": r.verdict.reason_detail if r.verdict else None,
            "strengths": r.verdict.strengths if r.verdict else [],
            "weaknesses": r.verdict.weaknesses if r.verdict else [],
            "problem_type": r.problem_type,
            "metric_pattern": r.metric_pattern,
            "pattern_confidence": r.pattern_confidence,
            "anomaly_detected": r.anomaly_detected,
            "anomaly_code": r.anomaly_code,
            "n_impressions": r.n_impressions,
            "n_clicks": r.n_clicks,
            "metrics": {k: round(v, 6) if v is not None else None for k, v in r.metrics.items()},
            "z_scores": {k: round(v, 4) if v is not None else None for k, v in r.z_scores.items()},
            "decision_confidence": r.decision_confidence,
        }
        analysis["texts"].append(text_data)

    # ─── INCONSISTENCY CHECKS (v2.0) ───
    # Updated to match current verdict.py + frontend getDiagnosis() logic.
    # "Проблема QA" is a valid verdict for anomalies/insufficient data — not a problem.
    # Verdict uses ranking_score (percentile), not composite_score.
    # Frontend getDiagnosis() v2.0 handles mixed→metric-based fallback dynamically.

    for t in analysis["texts"]:
        tid = t["text_id"]
        verdict = t["verdict"]
        problem = t["problem_type"]
        score = t["composite_score"]
        rank = t["ranking_score"]
        alt_cat = t["alt_category"]
        strengths = t["strengths"]
        weaknesses = t["weaknesses"]
        z = t["z_scores"]

        # 1. SCALE verdict + problem_type that contradicts (only severe ones)
        #    auction/hook with SCALE are handled by frontend (cost_note / CTR note)
        #    traffic_quality / landing_mismatch with SCALE would be truly contradictory
        if verdict == "Масштабировать":
            if problem in ("traffic_quality", "landing_mismatch"):
                analysis["issues"].append({
                    "type": "SCALE_CONTRADICTS_PROBLEM",
                    "text_id": tid,
                    "verdict": verdict,
                    "problem_type": problem,
                    "score": score,
                    "detail": f"SCALE text has problem_type={problem} — truly contradictory (not just secondary weakness)"
                })

        # 2. EXCLUDE verdict + many strong metrics (3+) — paradoxical
        if verdict == "Исключить" and len(strengths) >= 3:
            analysis["issues"].append({
                "type": "EXCLUDE_MANY_STRENGTHS",
                "text_id": tid,
                "verdict": verdict,
                "strengths": strengths,
                "score": score,
                "detail": f"EXCLUDE text has {len(strengths)} strong metrics: {strengths}"
            })

        # 3. Low ranking_score but SCALE verdict (ranking/verdict mismatch)
        if rank is not None and rank < 0.70 and verdict == "Масштабировать":
            analysis["issues"].append({
                "type": "LOW_RANK_SCALE",
                "text_id": tid,
                "verdict": verdict,
                "ranking_score": rank,
                "score": score,
                "detail": f"Ranking {rank:.4f} < 0.70 but verdict is SCALE"
            })

        # 4. High ranking but not SCALE (excluding QA/anomaly — those are correct)
        if rank is not None and rank >= 0.85 and verdict not in ("Масштабировать", "Проблема QA"):
            analysis["issues"].append({
                "type": "HIGH_RANK_NOT_SCALE",
                "text_id": tid,
                "verdict": verdict,
                "ranking_score": rank,
                "score": score,
                "detail": f"Ranking {rank:.4f} >= 0.85 but verdict is '{verdict}'"
            })

        # 5. Verdict-alt_category mismatch
        if verdict == "Масштабировать" and alt_cat in ("Слабый -", "Сильный -"):
            analysis["issues"].append({
                "type": "SCALE_LOW_CATEGORY",
                "text_id": tid,
                "verdict": verdict,
                "alt_category": alt_cat,
                "score": score,
                "detail": f"SCALE verdict but alt_category='{alt_cat}'"
            })
        if verdict == "Исключить" and alt_cat in ("Слабый +", "Сильный +"):
            analysis["issues"].append({
                "type": "EXCLUDE_HIGH_CATEGORY",
                "text_id": tid,
                "verdict": verdict,
                "alt_category": alt_cat,
                "score": score,
                "detail": f"EXCLUDE verdict but alt_category='{alt_cat}'"
            })

        # 6. Problem type doesn't match z-score pattern
        ctr_z = z.get("CTR")
        cr_z = None
        for k, v in z.items():
            if k.startswith("CR_") and v is not None:
                cr_z = v
                break
        if cr_z is None:
            cr_z = z.get("CR")

        if problem == "hook" and ctr_z is not None and ctr_z > 0.5:
            analysis["issues"].append({
                "type": "HOOK_BUT_GOOD_CTR",
                "text_id": tid,
                "problem_type": problem,
                "ctr_z": ctr_z,
                "detail": f"problem_type=hook but CTR z-score={ctr_z:.3f} > 0.5"
            })

        if problem == "traffic_quality" and ctr_z is not None and ctr_z < 0.5:
            analysis["issues"].append({
                "type": "TRAFFIC_QUALITY_BUT_LOW_CTR",
                "text_id": tid,
                "problem_type": problem,
                "ctr_z": ctr_z,
                "detail": f"problem_type=traffic_quality but CTR z-score={ctr_z:.3f} < 0.5"
            })

        # 7. NaN z-scores causing invisible scoring (exclude QA — expected to have NaNs)
        n_nan = sum(1 for v in z.values() if v is None)
        n_total_z = len(z)
        if n_nan > n_total_z / 2 and verdict not in ("Мало данных", "Проблема QA"):
            analysis["issues"].append({
                "type": "MANY_NAN_ZSCORES",
                "text_id": tid,
                "n_nan": n_nan,
                "n_total": n_total_z,
                "verdict": verdict,
                "score": score,
                "detail": f"{n_nan}/{n_total_z} z-scores are NaN but verdict is '{verdict}'"
            })

        # 8. Anomaly detected but verdict is NOT "Проблема QA" (should always be QA)
        if t["anomaly_detected"] and verdict != "Проблема QA":
            analysis["issues"].append({
                "type": "ANOMALY_WRONG_VERDICT",
                "text_id": tid,
                "anomaly_code": t["anomaly_code"],
                "verdict": verdict,
                "detail": f"Anomaly detected ({t['anomaly_code']}) but verdict is '{verdict}' (expected 'Проблема QA')"
            })

        # 9. OPTIMIZE+mixed — check frontend would still give specific advice via weakest metric
        if verdict == "Оптимизировать" and problem in ("mixed", None) and len(weaknesses) == 0:
            # No weaknesses AND no specific problem → truly generic recommendation
            analysis["issues"].append({
                "type": "OPTIMIZE_NO_ACTIONABLE_INFO",
                "text_id": tid,
                "verdict": verdict,
                "problem_type": problem,
                "detail": "OPTIMIZE with no problem_type AND no weak metrics — frontend has no info for specific advice"
            })

        # 10. EXCLUDE with 0 weaknesses — unclear why excluded
        if verdict == "Исключить" and len(weaknesses) == 0:
            analysis["issues"].append({
                "type": "EXCLUDE_NO_WEAKNESSES",
                "text_id": tid,
                "verdict": verdict,
                "score": score,
                "ranking_score": rank,
                "detail": f"EXCLUDE verdict but no weak metrics identified"
            })

    return analysis


def main():
    print("=" * 80)
    print("DATASET GENERATION & SCORING ANALYSIS")
    print("=" * 80)

    all_analyses = []
    all_issues = []

    for i, ds_config in enumerate(DATASETS):
        print(f"\n{'─' * 60}")
        print(f"Dataset {i+1}/{len(DATASETS)}: {ds_config['name']}")
        print(f"  {ds_config['description']}")
        print(f"  Event: {ds_config['event_label']} ({ds_config['event_type']})")

        try:
            analysis = analyze_single_dataset(ds_config)
            all_analyses.append(analysis)
            all_issues.extend([(ds_config['name'], issue) for issue in analysis["issues"]])

            # Print summary
            print(f"  Mode: {analysis['mode']}")
            print(f"  Scored: {analysis['n_scored']}/{analysis['n_total']} (excluded: {analysis['n_excluded']})")

            # Verdict distribution
            verdict_counts = {}
            for t in analysis["texts"]:
                v = t["verdict"] or "None"
                verdict_counts[v] = verdict_counts.get(v, 0) + 1
            print(f"  Verdicts: {dict(sorted(verdict_counts.items()))}")

            # Problem type distribution
            problem_counts = {}
            for t in analysis["texts"]:
                p = t["problem_type"] or "None"
                problem_counts[p] = problem_counts.get(p, 0) + 1
            print(f"  Problems: {dict(sorted(problem_counts.items()))}")

            # Score range
            scores = [t["composite_score"] for t in analysis["texts"]]
            if scores:
                print(f"  Score range: {min(scores):.4f} — {max(scores):.4f} (median: {sorted(scores)[len(scores)//2]:.4f})")

            # Issues
            if analysis["issues"]:
                print(f"  ⚠ Issues found: {len(analysis['issues'])}")
                for issue in analysis["issues"][:5]:
                    print(f"    - [{issue['type']}] {issue['detail'][:100]}")
                if len(analysis["issues"]) > 5:
                    print(f"    ... and {len(analysis['issues']) - 5} more")
            else:
                print(f"  ✓ No issues found")

        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            import traceback
            traceback.print_exc()

    # ─── CROSS-DATASET ANALYSIS ───
    print("\n" + "=" * 80)
    print("CROSS-DATASET ISSUE SUMMARY")
    print("=" * 80)

    # Group issues by type
    issue_types = {}
    for ds_name, issue in all_issues:
        t = issue["type"]
        if t not in issue_types:
            issue_types[t] = []
        issue_types[t].append((ds_name, issue))

    for issue_type, instances in sorted(issue_types.items(), key=lambda x: -len(x[1])):
        print(f"\n🔴 {issue_type}: {len(instances)} occurrences")
        for ds_name, issue in instances[:3]:
            print(f"   [{ds_name}] {issue['detail'][:120]}")
        if len(instances) > 3:
            print(f"   ... +{len(instances) - 3} more")

    # ─── SAVE FULL RESULTS ───
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dataset_analysis_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({
            "total_datasets": len(all_analyses),
            "total_texts_scored": sum(a["n_scored"] for a in all_analyses),
            "total_issues": len(all_issues),
            "issue_summary": {k: len(v) for k, v in issue_types.items()},
            "datasets": all_analyses,
        }, f, ensure_ascii=False, indent=2)

    print(f"\n\nFull results saved to: {output_path}")
    print(f"Total: {len(all_analyses)} datasets, {sum(a['n_scored'] for a in all_analyses)} texts scored, {len(all_issues)} issues found")

    return all_analyses, all_issues


if __name__ == "__main__":
    main()
