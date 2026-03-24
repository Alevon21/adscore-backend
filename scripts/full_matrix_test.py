#!/usr/bin/env python3
"""
Full matrix verification: Generate 12 datasets, score them, check verdicts,
problem_types, action plan recommendations, and text elements logic.
Runs standalone (no web server needed).
"""
import sys, os, json, random, math
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from scorer import TextScorer
from models import EventConfig, ScoringParams, TextResult, ScoringResult
from verdict import generate_verdicts
from problem_type import classify_problem_types

# ── Dataset configs ──
DATASETS = [
    {"name": "SaaS Подписка", "events": [{"slot": "event_1", "label": "Подписка"}], "n": 30},
    {"name": "E-com Транзакция", "events": [{"slot": "event_1", "label": "Первая транзакция"}], "n": 30},
    {"name": "App Покупка", "events": [{"slot": "event_1", "label": "Покупка"}], "n": 30},
    {"name": "Отель Бронирование", "events": [{"slot": "event_1", "label": "Бронирование номера"}], "n": 30},
    {"name": "Лид-форма", "events": [{"slot": "event_1", "label": "Лид"}], "n": 30},
    {"name": "Качественный лид (2 события)", "events": [
        {"slot": "event_1", "label": "Лид"},
        {"slot": "event_2", "label": "Качественный лид", "is_primary": True}
    ], "n": 30},
    {"name": "Продажа с revenue", "events": [{"slot": "event_1", "label": "Продажа"}], "has_revenue": True, "n": 30},
    {"name": "Мало данных подписка", "events": [{"slot": "event_1", "label": "Подписка"}], "n": 30, "low_data": True},
    {"name": "Бронь+Покупка (2 события)", "events": [
        {"slot": "event_1", "label": "Бронирование"},
        {"slot": "event_2", "label": "Покупка", "is_primary": True}
    ], "n": 30},
    {"name": "Лид с аномалиями", "events": [{"slot": "event_1", "label": "Лид"}], "n": 30, "anomalies": True},
    {"name": "Маркетплейс экстрим", "events": [{"slot": "event_1", "label": "Продажа"}], "n": 35, "extreme_spread": True},
    {"name": "Organic (без расхода)", "events": [{"slot": "event_1", "label": "Регистрация"}], "n": 25, "no_spend": True},
]


def generate_texts(config):
    """Generate realistic ad text data for scoring."""
    n = config["n"]
    texts = []
    random.seed(hash(config["name"]) % 2**32)
    np.random.seed(hash(config["name"]) % 2**31)

    for i in range(n):
        impressions = int(np.random.lognormal(10, 1.5)) if not config.get("low_data") else int(np.random.lognormal(5, 1))
        impressions = max(impressions, 100)
        ctr = np.random.beta(2, 50) if not config.get("extreme_spread") else np.random.beta(0.5, 10)
        clicks = max(int(impressions * ctr), 1)
        spend = 0 if config.get("no_spend") else round(clicks * np.random.lognormal(2, 0.8), 2)

        row = {
            "text_id": f"T{i+1:03d}",
            "headline": f"{config['name']} text {i+1}",
            "n_impressions": impressions,
            "n_clicks": clicks,
            "spend": spend,
        }

        for ev in config["events"]:
            slot = ev["slot"]
            cr = np.random.beta(2, 30)
            convs = max(0, int(clicks * cr))
            if config.get("low_data") and random.random() < 0.4:
                convs = 0
            row[f"n_{slot}"] = convs

        if config.get("has_revenue"):
            total_convs = sum(row.get(f"n_{ev['slot']}", 0) for ev in config["events"])
            row["revenue"] = round(total_convs * np.random.lognormal(5, 1), 2) if total_convs > 0 else 0

        if config.get("anomalies") and i < 3:
            row["n_clicks"] = int(row["n_impressions"] * 0.8)  # 80% CTR = anomaly
            row[f"n_{config['events'][0]['slot']}"] = row["n_clicks"] // 2  # 50% CR

        texts.append(row)

    return texts


def score_dataset(config):
    """Score a single dataset and return results."""
    texts = generate_texts(config)
    events = [EventConfig(**ev) for ev in config["events"]]

    df = pd.DataFrame(texts)
    params = ScoringParams(events=events)
    scorer = TextScorer(params=params)
    scoring_result = scorer.score(df)
    results = scoring_result.results

    # Apply verdicts and problem types
    generate_verdicts(results, events, params=params)
    classify_problem_types(results, events)

    return results, texts


# ── Frontend simulation: getDiagnosis() ──
VERDICT_RU_TO_KEY = {
    'Масштабировать': 'SCALE', 'ОК': 'HOLD', 'Подождать': 'HOLD',
    'Оптимизировать': 'OPTIMIZE', 'Оптимизировать (усталость)': 'OPTIMIZE_FATIGUE',
    'Исключить': 'EXCLUDE', 'Мало данных': 'INSUFFICIENT_DATA',
    'Проблема QA': 'QA_ISSUE',
}

METRIC_RECOMMENDATIONS = {
    'CTR': 'Низкая кликабельность → Переписать заголовок',
    'CR': 'Слабая конверсия → Проверить посадочную',
    'CPA': 'Дорогие конверсии → Оптимизировать воронку',
    'CPC': 'Дорогие клики → Расширить аудиторию',
    'CPM': 'Дорогие показы → Тестировать площадки',
    'ROI': 'Низкая окупаемость → Пересмотреть оффер',
}

SPECIFIC_PROBLEMS = {'hook', 'traffic_quality', 'landing_mismatch', 'auction', 'activation', 'monetization', 'microsegment'}


def normalize_metric(m):
    if m.startswith('CR_event_') or m.startswith('CR_install'): return 'CR'
    if m.startswith('CPA_event_'): return 'CPA'
    return m


def find_weakest(z_scores):
    weakest, weakest_z = None, float('inf')
    for m, z in z_scores.items():
        if z is None: continue
        if z < weakest_z: weakest_z, weakest = z, m
    return weakest if weakest and weakest_z < 0.42 else None


def simulate_getDiagnosis(result):
    """Simulate the frontend getDiagnosis() logic."""
    v = result.verdict
    if not v: return 'unknown', 'no verdict'

    verdict_text = v.verdict
    verdict_key = VERDICT_RU_TO_KEY.get(verdict_text, verdict_text)

    if result.anomaly_detected:
        return 'anomaly', 'Аномалия в данных'

    problem_type = result.problem_type
    weaknesses = v.weaknesses or []
    z_scores = result.z_scores or {}

    if verdict_key == 'SCALE':
        cost_weaks = [w for w in weaknesses if w in ('CPC', 'CPM', 'CPI')]
        if cost_weaks: return 'SCALE+cost_note', 'Лидер + оптимизировать стоимость'
        return 'SCALE', 'Лидер рейтинга'

    if verdict_key == 'HOLD':
        if problem_type and problem_type not in ('mixed', 'insufficient_data') and problem_type in SPECIFIC_PROBLEMS:
            return f'HOLD+{problem_type}', f'Стабильный ({problem_type})'
        weakest = weaknesses[0] if weaknesses else find_weakest(z_scores)
        if weakest:
            base = normalize_metric(weakest)
            if base in METRIC_RECOMMENDATIONS:
                return f'HOLD+metric_{base}', f'Стабильный ({METRIC_RECOMMENDATIONS[base]})'
        return 'HOLD', 'Стабильный результат'

    if verdict_key == 'OPTIMIZE':
        if problem_type and problem_type not in ('mixed',) and problem_type in SPECIFIC_PROBLEMS:
            return f'OPTIMIZE+{problem_type}', f'Оптимизировать ({problem_type})'
        weakest = weaknesses[0] if weaknesses else find_weakest(z_scores)
        if weakest:
            base = normalize_metric(weakest)
            if base in METRIC_RECOMMENDATIONS:
                return f'OPTIMIZE+metric_{base}', f'{METRIC_RECOMMENDATIONS[base]}'
        return 'OPTIMIZE', 'Требует доработки (generic)'

    if verdict_key == 'EXCLUDE':
        strengths = v.strengths or []
        if problem_type and problem_type not in ('mixed',) and problem_type in SPECIFIC_PROBLEMS:
            return f'EXCLUDE+{problem_type}', f'Отключить ({problem_type})'
        if len(strengths) > 1:
            return 'EXCLUDE+strengths', 'Неэффективный несмотря на сильные стороны'
        weakest = weaknesses[0] if weaknesses else find_weakest(z_scores)
        if weakest:
            base = normalize_metric(weakest)
            if base in METRIC_RECOMMENDATIONS:
                return f'EXCLUDE+metric_{base}', f'Отключить: {METRIC_RECOMMENDATIONS[base]}'
        return 'EXCLUDE', 'Неэффективный текст'

    if verdict_key in ('QA_ISSUE', 'INSUFFICIENT_DATA'):
        return verdict_key, 'Проблема данных'

    return verdict_key or 'unknown', 'fallback'


# ── Inconsistency checks ──
def check_inconsistencies(results, config_name):
    issues = []

    for r in results:
        v = r.verdict
        if not v: continue
        verdict_key = VERDICT_RU_TO_KEY.get(v.verdict, v.verdict)
        z = r.z_scores or {}
        valid_z = {k: val for k, val in z.items() if val is not None}
        rank = getattr(r, 'ranking_score', None)
        problem = r.problem_type
        strengths = v.strengths or []
        weaknesses = v.weaknesses or []

        # 1. SCALE with problem_type that suggests issues
        if verdict_key == 'SCALE' and problem in SPECIFIC_PROBLEMS and problem != 'microsegment':
            issues.append({
                'type': 'SCALE_WITH_PROBLEM',
                'text_id': r.text_id,
                'detail': f'SCALE + problem_type={problem}',
                'severity': 'medium',
            })

        # 2. SCALE with cost weaknesses
        if verdict_key == 'SCALE' and weaknesses:
            cost_weaks = [w for w in weaknesses if w in ('CPC', 'CPM', 'CPI')]
            if cost_weaks:
                issues.append({
                    'type': 'SCALE_WITH_COST_WEAKNESS',
                    'text_id': r.text_id,
                    'detail': f'SCALE + weak cost metrics: {cost_weaks}',
                    'severity': 'low',
                })

        # 3. High score but not SCALE
        if rank is not None and rank >= 0.75 and verdict_key not in ('SCALE', 'QA_ISSUE'):
            issues.append({
                'type': 'HIGH_SCORE_NOT_SCALE',
                'text_id': r.text_id,
                'detail': f'rank={rank:.2f}, verdict={verdict_key}',
                'severity': 'medium',
            })

        # 4. OPTIMIZE without clear recommendation
        if verdict_key == 'OPTIMIZE':
            rec_type, rec_text = simulate_getDiagnosis(r)
            if rec_type == 'OPTIMIZE':
                issues.append({
                    'type': 'OPTIMIZE_GENERIC',
                    'text_id': r.text_id,
                    'detail': f'No specific recommendation, problem={problem}, weaknesses={weaknesses}',
                    'severity': 'high',
                })

        # 5. EXCLUDE with multiple strengths
        if verdict_key == 'EXCLUDE' and len(strengths) >= 2:
            issues.append({
                'type': 'EXCLUDE_WITH_STRENGTHS',
                'text_id': r.text_id,
                'detail': f'EXCLUDE + strengths: {strengths}',
                'severity': 'medium',
            })

        # 6. HOLD with specific problem but generic recommendation
        if verdict_key == 'HOLD':
            rec_type, rec_text = simulate_getDiagnosis(r)
            if rec_type == 'HOLD':
                issues.append({
                    'type': 'HOLD_GENERIC',
                    'text_id': r.text_id,
                    'detail': f'No specific hint, problem={problem}',
                    'severity': 'low',
                })

        # 7. Verdict-problem mismatch: hook but good CTR
        if problem == 'hook' and z.get('CTR') is not None and z['CTR'] >= 0.55:
            issues.append({
                'type': 'HOOK_BUT_GOOD_CTR',
                'text_id': r.text_id,
                'detail': f'problem=hook but CTR z={z["CTR"]:.2f}',
                'severity': 'high',
            })

        # 8. traffic_quality but CR is OK
        if problem == 'traffic_quality' and any(
            z.get(k) is not None and z[k] >= 0.45
            for k in z if k.startswith('CR')
        ):
            issues.append({
                'type': 'TRAFFIC_QUALITY_BUT_CR_OK',
                'text_id': r.text_id,
                'detail': f'problem=traffic_quality but CR z-scores OK',
                'severity': 'high',
            })

        # 9. Many NaN z-scores but not flagged insufficient
        nan_count = sum(1 for val in z.values() if val is None)
        if nan_count >= 3 and verdict_key not in ('INSUFFICIENT_DATA', 'QA_ISSUE'):
            issues.append({
                'type': 'MANY_NAN_NOT_FLAGGED',
                'text_id': r.text_id,
                'detail': f'{nan_count} NaN z-scores, verdict={verdict_key}',
                'severity': 'medium',
            })

        # 10. Action plan: check buildChecklist logic
        if verdict_key == 'EXCLUDE' and not weaknesses:
            issues.append({
                'type': 'EXCLUDE_NO_WEAKNESSES',
                'text_id': r.text_id,
                'detail': 'EXCLUDE but no weaknesses for action plan',
                'severity': 'medium',
            })

        # 11. Verdict strengths/weaknesses alignment with z-scores
        for s in strengths:
            if s in z and z[s] is not None and z[s] < 0.42:
                issues.append({
                    'type': 'STRENGTH_BUT_LOW_Z',
                    'text_id': r.text_id,
                    'detail': f'strength={s} but z={z[s]:.2f}',
                    'severity': 'high',
                })
        for w in weaknesses:
            if w in z and z[w] is not None and z[w] > 0.58:
                issues.append({
                    'type': 'WEAKNESS_BUT_HIGH_Z',
                    'text_id': r.text_id,
                    'detail': f'weakness={w} but z={z[w]:.2f}',
                    'severity': 'high',
                })

    return issues


# ── Main ──
def main():
    all_issues = []
    all_stats = []

    for ds_config in DATASETS:
        name = ds_config["name"]
        print(f"\n{'='*60}")
        print(f"Dataset: {name}")
        print(f"{'='*60}")

        try:
            results, texts = score_dataset(ds_config)
        except Exception as e:
            print(f"  ERROR scoring: {e}")
            continue

        # Verdict distribution
        verdicts = {}
        problems = {}
        rec_types = {}
        for r in results:
            v = r.verdict
            if not v: continue
            vk = VERDICT_RU_TO_KEY.get(v.verdict, v.verdict)
            verdicts[vk] = verdicts.get(vk, 0) + 1
            pt = r.problem_type or 'none'
            problems[pt] = problems.get(pt, 0) + 1
            rt, _ = simulate_getDiagnosis(r)
            bucket = rt.split('+')[0] if '+' in rt else rt
            rec_types[rt] = rec_types.get(rt, 0) + 1

        print(f"  Texts: {len(results)}")
        print(f"  Verdicts: {dict(sorted(verdicts.items()))}")
        print(f"  Problems: {dict(sorted(problems.items()))}")
        print(f"  Recs: {dict(sorted(rec_types.items(), key=lambda x: -x[1]))}")

        # Check inconsistencies
        issues = check_inconsistencies(results, name)
        if issues:
            high = [i for i in issues if i['severity'] == 'high']
            med = [i for i in issues if i['severity'] == 'medium']
            low = [i for i in issues if i['severity'] == 'low']
            print(f"  Issues: {len(issues)} (high={len(high)}, med={len(med)}, low={len(low)})")
            for i in high[:5]:
                print(f"    🔴 {i['type']}: {i['text_id']} — {i['detail']}")
            for i in med[:3]:
                print(f"    🟡 {i['type']}: {i['text_id']} — {i['detail']}")
        else:
            print(f"  ✅ No issues found")

        all_issues.extend([{**i, 'dataset': name} for i in issues])
        all_stats.append({
            'name': name,
            'n_texts': len(results),
            'verdicts': verdicts,
            'problems': problems,
            'rec_types': rec_types,
            'n_issues': len(issues),
        })

    # ── Summary ──
    print(f"\n{'='*60}")
    print(f"SUMMARY ACROSS ALL DATASETS")
    print(f"{'='*60}")

    total_texts = sum(s['n_texts'] for s in all_stats)
    total_issues = len(all_issues)
    high_issues = [i for i in all_issues if i['severity'] == 'high']
    med_issues = [i for i in all_issues if i['severity'] == 'medium']

    print(f"Total texts scored: {total_texts}")
    print(f"Total issues: {total_issues}")
    print(f"  High severity: {len(high_issues)}")
    print(f"  Medium severity: {len(med_issues)}")
    print(f"  Low severity: {total_issues - len(high_issues) - len(med_issues)}")

    # Issue type distribution
    issue_counts = {}
    for i in all_issues:
        issue_counts[i['type']] = issue_counts.get(i['type'], 0) + 1
    print(f"\nIssue distribution:")
    for t, c in sorted(issue_counts.items(), key=lambda x: -x[1]):
        print(f"  {t}: {c}")

    # Verdict distribution across all
    total_verdicts = {}
    for s in all_stats:
        for v, c in s['verdicts'].items():
            total_verdicts[v] = total_verdicts.get(v, 0) + c
    print(f"\nOverall verdict distribution:")
    for v, c in sorted(total_verdicts.items(), key=lambda x: -x[1]):
        pct = c / total_texts * 100
        print(f"  {v}: {c} ({pct:.1f}%)")

    # Problem type distribution
    total_problems = {}
    for s in all_stats:
        for p, c in s['problems'].items():
            total_problems[p] = total_problems.get(p, 0) + c
    print(f"\nOverall problem distribution:")
    for p, c in sorted(total_problems.items(), key=lambda x: -x[1]):
        pct = c / total_texts * 100
        print(f"  {p}: {c} ({pct:.1f}%)")

    # Recommendation specificity
    total_recs = {}
    for s in all_stats:
        for r, c in s['rec_types'].items():
            total_recs[r] = total_recs.get(r, 0) + c
    generic_recs = sum(c for r, c in total_recs.items() if '+' not in r and r not in ('SCALE', 'anomaly', 'QA_ISSUE', 'INSUFFICIENT_DATA'))
    specific_recs = sum(c for r, c in total_recs.items() if '+' in r)
    print(f"\nRecommendation specificity:")
    print(f"  Specific (with +detail): {specific_recs}")
    print(f"  Generic (no detail): {generic_recs}")
    if specific_recs + generic_recs > 0:
        print(f"  Specificity rate: {specific_recs / (specific_recs + generic_recs) * 100:.1f}%")

    # Save results
    output = {
        'total_texts': total_texts,
        'total_issues': total_issues,
        'datasets': all_stats,
        'all_issues': all_issues[:200],
    }
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'matrix_test_results.json')
    with open(output_path, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nResults saved to {output_path}")


if __name__ == '__main__':
    main()
