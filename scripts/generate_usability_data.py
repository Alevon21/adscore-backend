"""
Generate 12 CSV datasets for UX Trust Testing.

Each CSV is designed to produce specific verdict distributions
when run through the real scoring pipeline.

Thresholds (from backend):
- SCALE: composite_score >= 0.68
- EXCLUDE: composite_score <= 0.30
- OPTIMIZE: mixed signals (good CTR + bad CR, etc.)
- OK: default middle ground
- INSUFFICIENT_DATA: impressions < 100 or clicks < 10 or conversions < 5

Min thresholds: impressions>=100, clicks>=10, conversions>=5
"""

import csv
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "usability_csvs")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def write_csv(filename, rows, fieldnames):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓ {filename} ({len(rows)} rows)")


STANDARD_FIELDS = ["text_id", "headline", "impressions", "clicks", "spend", "event_1"]


def scenario_1_best_performer():
    """6 ads: one has best composite (high CR + low CPA), but not highest CTR."""
    rows = [
        # ad_1: High CTR, low CR → OPTIMIZE (trap)
        {"text_id": "ad_1", "headline": "Летняя распродажа -50%",
         "impressions": 12400, "clicks": 744, "spend": 1116, "event_1": 22},
        # ad_2: Moderate CTR, high CR, low CPA → SCALE (answer)
        {"text_id": "ad_2", "headline": "Бесплатная доставка сегодня",
         "impressions": 9800, "clicks": 490, "spend": 931, "event_1": 34},
        # ad_3: Low CTR, low CR → EXCLUDE
        {"text_id": "ad_3", "headline": "Новая коллекция 2026",
         "impressions": 15200, "clicks": 608, "spend": 1277, "event_1": 11},
        # ad_4: Highest CTR, medium CR → OK
        {"text_id": "ad_4", "headline": "Только 3 дня: скидки до 70%",
         "impressions": 8500, "clicks": 680, "spend": 952, "event_1": 29},
        # ad_5: Low CTR, highest CR → SCALE candidate
        {"text_id": "ad_5", "headline": "Персональное предложение",
         "impressions": 11000, "clicks": 385, "spend": 847, "event_1": 31},
        # ad_6: Good CTR, OK CR → OK
        {"text_id": "ad_6", "headline": "Эксклюзив для подписчиков",
         "impressions": 7200, "clicks": 504, "spend": 756, "event_1": 25},
    ]
    write_csv("01_best_performer.csv", rows, STANDARD_FIELDS)


def scenario_2_budget_cut():
    """5 ads: one clearly worst (highest CPA, lowest CR)."""
    rows = [
        # ad_1: High spend, ok CR → OK
        {"text_id": "ad_1", "headline": "Весенний sale",
         "impressions": 18000, "clicks": 720, "spend": 2160, "event_1": 36},
        # ad_2: Low impressions, decent CTR → OK
        {"text_id": "ad_2", "headline": "Новинки сезона",
         "impressions": 6500, "clicks": 390, "spend": 585, "event_1": 15},
        # ad_3: High CPA $120, low CR 2.5% → EXCLUDE (answer)
        {"text_id": "ad_3", "headline": "Выбор покупателей",
         "impressions": 14000, "clicks": 560, "spend": 1680, "event_1": 8},
        # ad_4: Good CPA, good CR → SCALE
        {"text_id": "ad_4", "headline": "Бестселлер месяца",
         "impressions": 9200, "clicks": 552, "spend": 828, "event_1": 33},
        # ad_5: High spend, mediocre → OPTIMIZE
        {"text_id": "ad_5", "headline": "Премиум качество",
         "impressions": 11500, "clicks": 460, "spend": 1380, "event_1": 14},
    ]
    write_csv("02_budget_cut.csv", rows, STANDARD_FIELDS)


def scenario_3_creative_fatigue():
    """5 ads + days_active: one ad old (28 days) with declining performance."""
    fields = STANDARD_FIELDS + ["days_active"]
    rows = [
        # ad_1: Stable, 20 days
        {"text_id": "ad_1", "headline": "Проверенное качество",
         "impressions": 10000, "clicks": 500, "spend": 750, "event_1": 25, "days_active": 20},
        # ad_2: Old (28 days), metrics degraded → fatigue penalty (answer)
        {"text_id": "ad_2", "headline": "Умное решение",
         "impressions": 12000, "clicks": 480, "spend": 1020, "event_1": 19, "days_active": 28},
        # ad_3: Fresh, good metrics
        {"text_id": "ad_3", "headline": "Лёгкий выбор",
         "impressions": 8000, "clicks": 480, "spend": 672, "event_1": 24, "days_active": 10},
        # ad_4: Moderate age, low CTR but stable
        {"text_id": "ad_4", "headline": "Для ценителей",
         "impressions": 7500, "clicks": 338, "spend": 608, "event_1": 17, "days_active": 18},
        # ad_5: Fresh, high CTR
        {"text_id": "ad_5", "headline": "Хит продаж",
         "impressions": 9500, "clicks": 570, "spend": 855, "event_1": 23, "days_active": 8},
    ]
    write_csv("03_creative_fatigue.csv", rows, fields)


def scenario_4_anomaly_detection():
    """6 ads: one with only 48 impressions (incredible metrics but insufficient data)."""
    rows = [
        # ad_1: Solid performer
        {"text_id": "ad_1", "headline": "Стабильный продукт",
         "impressions": 15000, "clicks": 750, "spend": 1125, "event_1": 38},
        # ad_2: Only 48 impressions → INSUFFICIENT_DATA (trap)
        {"text_id": "ad_2", "headline": "Супер-оффер",
         "impressions": 48, "clicks": 7, "spend": 14, "event_1": 3},
        # ad_3: Average
        {"text_id": "ad_3", "headline": "Проверено временем",
         "impressions": 12000, "clicks": 480, "spend": 960, "event_1": 24},
        # ad_4: Good metrics, reliable data → SCALE (answer)
        {"text_id": "ad_4", "headline": "Надёжный выбор",
         "impressions": 9500, "clicks": 570, "spend": 798, "event_1": 34},
        # ad_5: Average
        {"text_id": "ad_5", "headline": "Рекомендуем!",
         "impressions": 11200, "clicks": 560, "spend": 952, "event_1": 22},
        # ad_6: Decent
        {"text_id": "ad_6", "headline": "Топ рейтинг",
         "impressions": 8800, "clicks": 528, "spend": 739, "event_1": 27},
    ]
    write_csv("04_anomaly_detection.csv", rows, STANDARD_FIELDS)


def scenario_5_campaign_verdict():
    """15 ads in 3 campaigns (5 each). Test campaign-level analysis."""
    fields = STANDARD_FIELDS + ["campaign"]
    rows = []

    # Campaign A: Brand Awareness — moderate, high volume, mixed quality
    camp_a = [
        {"text_id": "ca_1", "headline": "Узнайте о нас", "impressions": 18000, "clicks": 720, "spend": 1080, "event_1": 22, "campaign": "Brand Awareness"},
        {"text_id": "ca_2", "headline": "Мы рядом с вами", "impressions": 15000, "clicks": 600, "spend": 900, "event_1": 18, "campaign": "Brand Awareness"},
        {"text_id": "ca_3", "headline": "Доверие миллионов", "impressions": 20000, "clicks": 800, "spend": 1200, "event_1": 20, "campaign": "Brand Awareness"},
        {"text_id": "ca_4", "headline": "Ваш надёжный партнёр", "impressions": 12000, "clicks": 480, "spend": 720, "event_1": 10, "campaign": "Brand Awareness"},
        {"text_id": "ca_5", "headline": "Открой новое", "impressions": 16000, "clicks": 640, "spend": 960, "event_1": 16, "campaign": "Brand Awareness"},
    ]

    # Campaign B: Retargeting — high CR, low CPA, smaller volume → SCALE (answer)
    camp_b = [
        {"text_id": "cb_1", "headline": "Вернитесь за скидкой", "impressions": 6000, "clicks": 420, "spend": 714, "event_1": 34, "campaign": "Retargeting"},
        {"text_id": "cb_2", "headline": "Ваша корзина ждёт", "impressions": 7000, "clicks": 490, "spend": 833, "event_1": 39, "campaign": "Retargeting"},
        {"text_id": "cb_3", "headline": "Товар заканчивается", "impressions": 5500, "clicks": 385, "spend": 655, "event_1": 31, "campaign": "Retargeting"},
        {"text_id": "cb_4", "headline": "Персональная скидка 15%", "impressions": 8000, "clicks": 560, "spend": 952, "event_1": 45, "campaign": "Retargeting"},
        {"text_id": "cb_5", "headline": "Только для вас", "impressions": 5500, "clicks": 385, "spend": 655, "event_1": 30, "campaign": "Retargeting"},
    ]

    # Campaign C: Prospecting — high volume, low CR, high CPA
    camp_c = [
        {"text_id": "cc_1", "headline": "Попробуйте бесплатно", "impressions": 25000, "clicks": 1250, "spend": 2250, "event_1": 25, "campaign": "Prospecting"},
        {"text_id": "cc_2", "headline": "Первый заказ со скидкой", "impressions": 22000, "clicks": 1100, "spend": 1980, "event_1": 33, "campaign": "Prospecting"},
        {"text_id": "cc_3", "headline": "Новым клиентам -30%", "impressions": 28000, "clicks": 1400, "spend": 2520, "event_1": 28, "campaign": "Prospecting"},
        {"text_id": "cc_4", "headline": "Регистрируйтесь сейчас", "impressions": 20000, "clicks": 1000, "spend": 1800, "event_1": 15, "campaign": "Prospecting"},
        {"text_id": "cc_5", "headline": "Начните сегодня", "impressions": 24000, "clicks": 1200, "spend": 2160, "event_1": 24, "campaign": "Prospecting"},
    ]

    rows = camp_a + camp_b + camp_c
    write_csv("05_campaign_verdict.csv", rows, fields)


def scenario_6_high_ctr_trap():
    """6 ads: one with highest CTR but very low CR (clickbait trap)."""
    rows = [
        # ad_1: Very high CTR 8%, but CR only 1.5% → clickbait (trap)
        {"text_id": "ad_1", "headline": "ШОК! Цены рухнули",
         "impressions": 10000, "clicks": 800, "spend": 1200, "event_1": 12},
        # ad_2: Moderate CTR 5%, high CR 7% → best overall (answer)
        {"text_id": "ad_2", "headline": "Качество по доступной цене",
         "impressions": 11000, "clicks": 550, "spend": 935, "event_1": 39},
        # ad_3: Good CTR, moderate CR
        {"text_id": "ad_3", "headline": "Выгодное предложение",
         "impressions": 9000, "clicks": 540, "spend": 810, "event_1": 22},
        # ad_4: Low CTR, moderate CR → OPTIMIZE
        {"text_id": "ad_4", "headline": "Товары для дома",
         "impressions": 13000, "clicks": 520, "spend": 1040, "event_1": 21},
        # ad_5: Medium everything → OK
        {"text_id": "ad_5", "headline": "Широкий ассортимент",
         "impressions": 8500, "clicks": 425, "spend": 680, "event_1": 17},
        # ad_6: High CTR 7%, low CR → similar trap
        {"text_id": "ad_6", "headline": "МЕГА-СКИДКИ каждый день",
         "impressions": 9500, "clicks": 665, "spend": 998, "event_1": 13},
    ]
    write_csv("06_high_ctr_trap.csv", rows, STANDARD_FIELDS)


def scenario_7_roi_vs_volume():
    """5 ads with revenue: one has amazing ROI on tiny volume, another has decent ROI on big volume."""
    fields = STANDARD_FIELDS + ["revenue"]
    rows = [
        # ad_1: Small volume, huge ROI 8x → trap (not scalable)
        {"text_id": "ad_1", "headline": "Премиум-сегмент",
         "impressions": 3500, "clicks": 210, "spend": 630, "event_1": 15, "revenue": 5040},
        # ad_2: Big volume, good ROI 3.5x → SCALE (answer)
        {"text_id": "ad_2", "headline": "Для всей семьи",
         "impressions": 18000, "clicks": 900, "spend": 1800, "event_1": 54, "revenue": 6300},
        # ad_3: Medium volume, low ROI
        {"text_id": "ad_3", "headline": "Сезонные товары",
         "impressions": 12000, "clicks": 600, "spend": 1200, "event_1": 24, "revenue": 1680},
        # ad_4: Medium, break-even ROI
        {"text_id": "ad_4", "headline": "Популярные модели",
         "impressions": 10000, "clicks": 500, "spend": 1000, "event_1": 20, "revenue": 1400},
        # ad_5: Small, negative ROI
        {"text_id": "ad_5", "headline": "Эксклюзивная линейка",
         "impressions": 5000, "clicks": 250, "spend": 750, "event_1": 10, "revenue": 600},
    ]
    write_csv("07_roi_vs_volume.csv", rows, fields)


def scenario_8_borderline_decision():
    """6 ads: several hover around OK/Optimize boundary (score ~0.45-0.55)."""
    rows = [
        # ad_1: Clearly good → SCALE
        {"text_id": "ad_1", "headline": "Проверенный лидер",
         "impressions": 14000, "clicks": 700, "spend": 980, "event_1": 42},
        # ad_2: Borderline — decent CTR, meh CR → OK or OPTIMIZE
        {"text_id": "ad_2", "headline": "Новая формула",
         "impressions": 9000, "clicks": 450, "spend": 765, "event_1": 16},
        # ad_3: Borderline — ok metrics all around → OK
        {"text_id": "ad_3", "headline": "Классический выбор",
         "impressions": 10500, "clicks": 420, "spend": 756, "event_1": 17},
        # ad_4: Borderline — high CPC, but decent CR → depends on weights
        {"text_id": "ad_4", "headline": "Продвинутая версия",
         "impressions": 8000, "clicks": 320, "spend": 800, "event_1": 16},
        # ad_5: Clearly bad → EXCLUDE
        {"text_id": "ad_5", "headline": "Стандартный набор",
         "impressions": 11000, "clicks": 440, "spend": 1320, "event_1": 9},
        # ad_6: Borderline — good CTR but expensive
        {"text_id": "ad_6", "headline": "Быстрый результат",
         "impressions": 7500, "clicks": 450, "spend": 900, "event_1": 18},
    ]
    write_csv("08_borderline_decision.csv", rows, STANDARD_FIELDS)


def scenario_9_new_vs_proven():
    """5 ads + days_active: new ad (3 days) with promising metrics vs proven ad (30 days) with average metrics."""
    fields = STANDARD_FIELDS + ["days_active"]
    rows = [
        # ad_1: New (3 days), small data, great metrics → promising but unreliable
        {"text_id": "ad_1", "headline": "Инновация 2026",
         "impressions": 1200, "clicks": 84, "spend": 126, "event_1": 7, "days_active": 3},
        # ad_2: Proven (30 days), large data, average metrics → reliable
        {"text_id": "ad_2", "headline": "Испытано временем",
         "impressions": 22000, "clicks": 880, "spend": 1760, "event_1": 35, "days_active": 30},
        # ad_3: Medium age, good metrics → SCALE (answer)
        {"text_id": "ad_3", "headline": "Оптимальный баланс",
         "impressions": 14000, "clicks": 700, "spend": 1050, "event_1": 42, "days_active": 16},
        # ad_4: Old, declining
        {"text_id": "ad_4", "headline": "Классика жанра",
         "impressions": 16000, "clicks": 640, "spend": 1280, "event_1": 19, "days_active": 25},
        # ad_5: Medium, moderate
        {"text_id": "ad_5", "headline": "Свежий взгляд",
         "impressions": 8000, "clicks": 400, "spend": 680, "event_1": 20, "days_active": 12},
    ]
    write_csv("09_new_vs_proven.csv", rows, fields)


def scenario_10_platform_split():
    """8 ads across 2 platforms (4 each). Same headlines, different performance by platform."""
    fields = STANDARD_FIELDS + ["platform"]
    rows = [
        # Google ads
        {"text_id": "g_1", "headline": "Лучшая цена онлайн", "impressions": 12000, "clicks": 600, "spend": 900, "event_1": 30, "platform": "Google"},
        {"text_id": "g_2", "headline": "Доставка за 1 день", "impressions": 10000, "clicks": 500, "spend": 850, "event_1": 25, "platform": "Google"},
        {"text_id": "g_3", "headline": "Гарантия качества", "impressions": 8000, "clicks": 320, "spend": 640, "event_1": 13, "platform": "Google"},
        {"text_id": "g_4", "headline": "Скидка новичкам", "impressions": 15000, "clicks": 750, "spend": 1125, "event_1": 38, "platform": "Google"},
        # Meta ads — same concepts but different performance
        {"text_id": "m_1", "headline": "Лучшая цена онлайн", "impressions": 20000, "clicks": 1400, "spend": 2100, "event_1": 28, "platform": "Meta"},
        {"text_id": "m_2", "headline": "Доставка за 1 день", "impressions": 18000, "clicks": 1080, "spend": 1620, "event_1": 22, "platform": "Meta"},
        {"text_id": "m_3", "headline": "Гарантия качества", "impressions": 14000, "clicks": 840, "spend": 1260, "event_1": 17, "platform": "Meta"},
        {"text_id": "m_4", "headline": "Скидка новичкам", "impressions": 22000, "clicks": 1540, "spend": 2310, "event_1": 31, "platform": "Meta"},
    ]
    write_csv("10_platform_split.csv", rows, fields)


def scenario_11_cost_outlier():
    """5 ads: one has 2x CPC but outstanding CR and ROI."""
    fields = STANDARD_FIELDS + ["revenue"]
    rows = [
        # ad_1: Expensive CPC $4, but amazing CR 10% and ROI → SCALE (answer)
        {"text_id": "ad_1", "headline": "Премиум-консультация",
         "impressions": 8000, "clicks": 400, "spend": 1600, "event_1": 40, "revenue": 6400},
        # ad_2: Cheap CPC $1.5, OK CR → OK
        {"text_id": "ad_2", "headline": "Быстрый старт",
         "impressions": 12000, "clicks": 600, "spend": 900, "event_1": 24, "revenue": 1680},
        # ad_3: Medium CPC, medium CR
        {"text_id": "ad_3", "headline": "Простое решение",
         "impressions": 10000, "clicks": 500, "spend": 1000, "event_1": 20, "revenue": 1600},
        # ad_4: Low CPC, low CR → poor
        {"text_id": "ad_4", "headline": "Экономный вариант",
         "impressions": 14000, "clicks": 700, "spend": 840, "event_1": 14, "revenue": 840},
        # ad_5: Medium, negative ROI
        {"text_id": "ad_5", "headline": "Стандартный пакет",
         "impressions": 9000, "clicks": 360, "spend": 900, "event_1": 11, "revenue": 660},
    ]
    write_csv("11_cost_outlier.csv", rows, fields)


def scenario_12_mixed_signals():
    """6 ads: each has one great metric and one terrible → genuinely conflicting signals."""
    rows = [
        # ad_1: Best CTR 7%, worst CR 1.5%
        {"text_id": "ad_1", "headline": "Кликни сейчас!",
         "impressions": 10000, "clicks": 700, "spend": 1050, "event_1": 11},
        # ad_2: Best CR 8%, worst CTR 2.5%
        {"text_id": "ad_2", "headline": "Для профессионалов",
         "impressions": 12000, "clicks": 300, "spend": 750, "event_1": 24},
        # ad_3: Low CPA, low volume
        {"text_id": "ad_3", "headline": "Точечное решение",
         "impressions": 5000, "clicks": 300, "spend": 450, "event_1": 21},
        # ad_4: High volume, high CPA
        {"text_id": "ad_4", "headline": "Масштабный проект",
         "impressions": 20000, "clicks": 800, "spend": 2000, "event_1": 20},
        # ad_5: Balanced but mediocre
        {"text_id": "ad_5", "headline": "Золотая середина",
         "impressions": 9000, "clicks": 450, "spend": 810, "event_1": 18},
        # ad_6: Good metrics overall → likely SCALE
        {"text_id": "ad_6", "headline": "Умный выбор",
         "impressions": 11000, "clicks": 550, "spend": 825, "event_1": 33},
    ]
    write_csv("12_mixed_signals.csv", rows, STANDARD_FIELDS)


if __name__ == "__main__":
    print("Generating 12 CSV datasets for UX Trust Testing...\n")
    scenario_1_best_performer()
    scenario_2_budget_cut()
    scenario_3_creative_fatigue()
    scenario_4_anomaly_detection()
    scenario_5_campaign_verdict()
    scenario_6_high_ctr_trap()
    scenario_7_roi_vs_volume()
    scenario_8_borderline_decision()
    scenario_9_new_vs_proven()
    scenario_10_platform_split()
    scenario_11_cost_outlier()
    scenario_12_mixed_signals()
    print(f"\nDone! CSVs saved to: {OUTPUT_DIR}")
