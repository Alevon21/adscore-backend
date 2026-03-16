"""
Text part analysis module.
Detects elements/parts in ad text headlines and analyzes their impact on metrics.
Finds best combinations of text elements.
"""

import logging
import re
from itertools import combinations
from typing import Dict, List, Optional

import numpy as np
from scipy.stats import norm, t as t_dist

from models import (
    AnalysisSummary,
    CombinationResult,
    ExcludedPartInfo,
    ExtractedWord,
    PartImpact,
    ScoringResult,
    TextPartAnalysisResult,
)

logger = logging.getLogger(__name__)

# Common offer patterns to auto-detect
OFFER_PATTERNS = [
    (r"\d+\s*руб", "N рублей"),
    (r"\d+\s*₽", "N₽"),
    (r"\d+\s*дн", "N дней"),
    (r"\d+\s*мес", "N месяцев"),
    (r"\bбесплатн", "бесплатно"),
    (r"\bскидк", "скидка"),
    (r"\bакци", "акция"),
    (r"\bподарок", "подарок"),
    (r"\bбонус", "бонус"),
    (r"\bбез\s+%|без\s+процент", "без процентов"),
    (r"\bгарант", "гарантия"),
    (r"\bдоставк", "доставка"),
    (r"\bкэшбэк|cashback", "кэшбэк"),
]

# Russian stopwords — prepositions, conjunctions, particles, pronouns
STOPWORDS_RU = {
    # prepositions
    "в", "во", "на", "с", "со", "за", "от", "по", "из", "к", "ко",
    "у", "о", "об", "обо", "до", "для", "при", "без", "про", "через",
    "между", "над", "под", "перед",
    # conjunctions
    "и", "а", "но", "или", "да", "ни", "то", "что", "как", "когда",
    "если", "хотя", "чтобы", "потому", "поэтому",
    # particles
    "не", "бы", "же", "ли", "вот", "вон", "ведь", "уж",
    # pronouns
    "я", "ты", "он", "она", "оно", "мы", "вы", "они",
    "его", "её", "их", "ей", "ему", "им",
    "мой", "твой", "наш", "ваш", "свой",
    # misc short words
    "это", "все", "так", "ещё", "еще", "уже", "тут", "там",
    "где", "кто", "чем", "чего", "кого",
    # punctuation as "words"
    "-", "—", "–", "|", "/", "\\", ".", ",", "!", "?", ":", ";",
}

# Characters to strip from n-gram boundaries
STRIP_CHARS = "-—–|/\\.,!?:;()[]{}\"'«»"


class TextPartAnalyzer:
    """Analyzes individual parts of ad texts and finds best combinations."""

    def _clean_word(self, word: str) -> str:
        """Strip punctuation from word boundaries."""
        return word.strip(STRIP_CHARS)

    def _extract_ngrams(self, texts: List[str], n_range=(1, 3)) -> Dict[str, int]:
        """Extract n-gram frequencies from texts, filtering stopwords."""
        ngram_counts: Dict[str, int] = {}
        for text in texts:
            raw_words = text.lower().split()
            words = [self._clean_word(w) for w in raw_words]
            words = [w for w in words if w]  # remove empty after stripping

            for n in range(n_range[0], n_range[1] + 1):
                for i in range(len(words) - n + 1):
                    gram_words = words[i:i + n]

                    # Skip if all words are stopwords
                    if all(w in STOPWORDS_RU for w in gram_words):
                        continue

                    # For unigrams: skip stopwords entirely
                    if n == 1 and gram_words[0] in STOPWORDS_RU:
                        continue

                    # For multi-word n-grams: skip if any word is a stopword or pure digit
                    if n > 1 and any(w in STOPWORDS_RU or w.isdigit() for w in gram_words):
                        continue

                    # For unigrams: skip pure digits
                    if n == 1 and gram_words[0].isdigit():
                        continue

                    ngram = " ".join(gram_words)
                    if len(ngram) < 3:
                        continue

                    ngram_counts[ngram] = ngram_counts.get(ngram, 0) + 1

        return ngram_counts

    def _auto_detect_parts(self, headlines: List[str]) -> List[str]:
        """
        Auto-detect text elements using:
        1. Regex patterns for common offers
        2. N-gram frequency analysis (5%-80% of texts must contain the part)
        """
        n_texts = len(headlines)
        if n_texts < 3:
            return []

        detected = set()

        # 1. Regex pattern detection
        for pattern, label in OFFER_PATTERNS:
            count = sum(1 for h in headlines if re.search(pattern, h.lower()))
            freq = count / n_texts
            if 0.05 <= freq <= 0.80 and count >= 2:
                detected.add(label)

        # 2. N-gram frequency analysis
        ngram_counts = self._extract_ngrams(headlines)
        min_freq = max(2, int(n_texts * 0.05))
        max_freq = int(n_texts * 0.80)

        # Sort by frequency descending, take top candidates
        sorted_ngrams = sorted(
            ngram_counts.items(), key=lambda x: x[1], reverse=True
        )

        for ngram, count in sorted_ngrams[:50]:
            if min_freq <= count <= max_freq:
                if len(ngram) < 4:
                    continue
                # Skip if it's a substring of an already detected part
                is_sub = any(ngram in d for d in detected if ngram != d)
                if not is_sub:
                    detected.add(ngram)

            if len(detected) >= 15:
                break

        return sorted(detected)

    def _build_flags(
        self, headlines: List[str], parts: List[str]
    ) -> Dict[str, List[bool]]:
        """Build boolean flags: for each part, True/False per text."""
        flags: Dict[str, List[bool]] = {}
        for part in parts:
            # Check if part is a regex pattern label
            pattern = None
            for pat, label in OFFER_PATTERNS:
                if label == part:
                    pattern = pat
                    break

            if pattern:
                flags[part] = [
                    bool(re.search(pattern, h.lower())) for h in headlines
                ]
            else:
                flags[part] = [
                    part.lower() in h.lower() for h in headlines
                ]
        return flags

    def _test_two_groups(
        self, values_with: List[float], values_without: List[float]
    ) -> Dict:
        """Compare two groups. Uses Welch's t-test for n<30, z-test otherwise."""
        n1 = len(values_with)
        n2 = len(values_without)

        if n1 < 2 or n2 < 2:
            return {"z_stat": 0.0, "p_value": 1.0, "se": 0.0}

        m1 = np.mean(values_with)
        m2 = np.mean(values_without)
        s1 = np.std(values_with, ddof=1)
        s2 = np.std(values_without, ddof=1)

        se = np.sqrt(s1 ** 2 / n1 + s2 ** 2 / n2)
        if se < 1e-12:
            return {"z_stat": 0.0, "p_value": 1.0, "se": 0.0}

        stat = float((m1 - m2) / se)

        if n1 < 30 or n2 < 30:
            # Welch-Satterthwaite degrees of freedom
            num = (s1**2 / n1 + s2**2 / n2) ** 2
            den = (s1**2 / n1) ** 2 / (n1 - 1) + (s2**2 / n2) ** 2 / (n2 - 1)
            df = float(num / den) if den > 0 else max(n1, n2) - 1
            p_value = float(2 * (1 - t_dist.cdf(abs(stat), df)))
        else:
            p_value = float(2 * (1 - norm.cdf(abs(stat))))

        return {"z_stat": stat, "p_value": p_value, "se": float(se)}

    @staticmethod
    def _cohens_d(with_vals: List[float], without_vals: List[float]) -> float:
        """Compute Cohen's d effect size."""
        n1, n2 = len(with_vals), len(without_vals)
        if n1 < 2 or n2 < 2:
            return 0.0
        s1 = np.std(with_vals, ddof=1)
        s2 = np.std(without_vals, ddof=1)
        pooled = np.sqrt(((n1 - 1) * s1 ** 2 + (n2 - 1) * s2 ** 2) / (n1 + n2 - 2))
        if pooled < 1e-12:
            return 0.0
        return float((np.mean(with_vals) - np.mean(without_vals)) / pooled)

    @staticmethod
    def _classify_confidence(p_value: float, effect_size: float) -> str:
        """Classify reliability level based on p-value and Cohen's d.

        Uses AND for high/medium (need both significance & effect),
        but OR for low (either signal is enough to distinguish from noise).
        This prevents normalized metrics like ranking_score (0–1 scale)
        from always showing "noise" due to naturally small Cohen's d.
        """
        d = abs(effect_size)
        if p_value < 0.05 and d >= 0.5:
            return "high"
        if p_value < 0.10 and d >= 0.3:
            return "medium"
        if p_value < 0.10 or d >= 0.3:
            return "low"
        if p_value < 0.20 or d >= 0.15:
            return "low"
        return "noise"

    def _analyze_impact(
        self,
        headlines: List[str],
        metric_values: List[float],
        part_flags: Dict[str, List[bool]],
        metric_name: str,
    ) -> List[PartImpact]:
        """Analyze impact of each part on a metric with Cohen's d and 95% CI."""
        impacts = []

        for part, flags in part_flags.items():
            with_vals = [v for v, f in zip(metric_values, flags) if f]
            without_vals = [v for v, f in zip(metric_values, flags) if not f]

            n_with = len(with_vals)
            n_without = len(without_vals)

            if n_with < 2 or n_without < 2:
                continue

            m_with = float(np.mean(with_vals))
            m_without = float(np.mean(without_vals))
            delta = m_with - m_without
            delta_pct = (delta / abs(m_without) * 100) if m_without != 0 else 0.0

            test = self._test_two_groups(with_vals, without_vals)
            se = test["se"]
            p_value = test["p_value"]

            # Cohen's d
            d = self._cohens_d(with_vals, without_vals)

            # 95% CI for delta
            ci_lower = delta - 1.96 * se if se > 0 else delta
            ci_upper = delta + 1.96 * se if se > 0 else delta

            # Confidence classification
            confidence = self._classify_confidence(p_value, d)

            impacts.append(
                PartImpact(
                    part_name=part,
                    n_with=n_with,
                    n_without=n_without,
                    metric_with=round(m_with, 6),
                    metric_without=round(m_without, 6),
                    delta=round(delta, 6),
                    delta_pct=round(delta_pct, 2),
                    p_value=round(p_value, 6),
                    significant=confidence in ("high", "medium"),
                    effect_size=round(d, 4),
                    confidence=confidence,
                    ci_lower=round(ci_lower, 6),
                    ci_upper=round(ci_upper, 6),
                )
            )

        # Sort by absolute delta descending
        impacts.sort(key=lambda x: abs(x.delta), reverse=True)
        return impacts

    def _find_combinations(
        self,
        headlines: List[str],
        metric_values: List[float],
        part_flags: Dict[str, List[bool]],
        max_size: int = 3,
    ) -> List[CombinationResult]:
        """Find best combinations of text parts by average metric.
        Deduplicates combinations that match the exact same set of texts."""
        parts = list(part_flags.keys())
        if not parts:
            return []

        results = []
        seen_text_sets = set()
        n_texts = len(headlines)

        # Process smaller combos first — simpler explanations win
        for size in range(1, min(max_size + 1, len(parts) + 1)):
            for combo in combinations(parts, size):
                mask = [True] * n_texts
                for part in combo:
                    flags = part_flags[part]
                    mask = [m and f for m, f in zip(mask, flags)]

                matching_indices = frozenset(i for i, m in enumerate(mask) if m)
                if len(matching_indices) < 2:
                    continue

                # Skip if same text set already seen (simpler combo already registered)
                if matching_indices in seen_text_sets:
                    continue
                seen_text_sets.add(matching_indices)

                matching_values = [metric_values[i] for i in matching_indices]
                avg_metric = float(np.mean(matching_values))
                n = len(matching_values)
                if n >= 2:
                    std = float(np.std(matching_values, ddof=1))
                    se = std / np.sqrt(n)
                    ci_lo = avg_metric - 1.96 * se
                    ci_hi = avg_metric + 1.96 * se
                else:
                    ci_lo = avg_metric
                    ci_hi = avg_metric
                results.append(
                    CombinationResult(
                        parts=list(combo),
                        n_texts=n,
                        avg_metric=round(avg_metric, 6),
                        rank=0,
                        ci_lower=round(ci_lo, 6),
                        ci_upper=round(ci_hi, 6),
                    )
                )

        # Sort by average metric descending and assign ranks
        results.sort(key=lambda x: x.avg_metric, reverse=True)
        for i, r in enumerate(results):
            r.rank = i + 1

        return results[:20]  # Top 20 combinations

    def _generate_summary(
        self,
        impacts: List[PartImpact],
        n_texts: int,
        metric_name: str,
        is_cost_metric: bool = False,
    ) -> AnalysisSummary:
        """Generate human-readable summary and recommendations."""
        positive = []
        negative = []
        neutral = []

        for imp in impacts:
            # For cost metrics, negative delta = good (lower cost)
            is_good = (imp.delta < 0) if is_cost_metric else (imp.delta > 0)
            is_bad = not is_good and imp.delta != 0

            if imp.confidence in ("high", "medium"):
                if is_good:
                    positive.append(imp.part_name)
                elif is_bad:
                    negative.append(imp.part_name)
                else:
                    neutral.append(imp.part_name)
            else:
                neutral.append(imp.part_name)

        # Build recommendation text
        parts = []
        if positive:
            names = ", ".join(f"«{n}»" for n in positive)
            parts.append(
                f"Элементы {names} ассоциируются с улучшением метрики. "
                f"Рекомендуем чаще использовать их в текстах и протестировать в A/B тесте."
            )
        if negative:
            names = ", ".join(f"«{n}»" for n in negative)
            parts.append(
                f"Элементы {names} ассоциируются с ухудшением метрики. "
                f"Рекомендуем сократить их использование или проверить контекст."
            )
        if neutral and not positive and not negative:
            parts.append("Ни один из выбранных элементов не показал заметного влияния на метрику.")
        elif neutral:
            names = ", ".join(f"«{n}»" for n in neutral[:3])
            rest = f" и ещё {len(neutral) - 3}" if len(neutral) > 3 else ""
            parts.append(f"Элементы {names}{rest} не оказывают заметного влияния.")

        recommendation = " ".join(parts) if parts else ""

        # Sample size warning
        sample_warning = ""
        if n_texts < 10:
            sample_warning = (
                f"⚠ Анализ на {n_texts} текстах — выводы крайне ненадёжные. "
                f"Для минимально значимых результатов нужно 20+ текстов."
            )
        elif n_texts < 20:
            sample_warning = (
                f"⚠ Анализ на {n_texts} текстах — выводы ориентировочные. "
                f"Для надёжных результатов рекомендуется 30+ текстов."
            )

        return AnalysisSummary(
            positive_elements=positive,
            negative_elements=negative,
            neutral_elements=neutral,
            recommendation=recommendation,
            sample_warning=sample_warning,
        )

    def analyze(
        self,
        scoring_result: ScoringResult,
        custom_parts: Optional[List[str]] = None,
        primary_metric: str = "composite_score",
        max_combination_size: int = 3,
        all_headlines: Optional[List[str]] = None,
        fdr_level: float = 0.05,
    ) -> TextPartAnalysisResult:
        """
        Full text part analysis pipeline.
        1. Extract/detect text parts (using all_headlines if provided)
        2. Build flags (on scored texts only)
        3. Analyze impact on each available metric (with confidence classification)
        4. Find best combinations for primary metric (deduplicated)
        5. Generate summary and recommendations
        """
        texts = scoring_result.results
        n_scored = len(texts)

        if n_scored < 3:
            return TextPartAnalysisResult(
                parts_detected=[],
                part_impacts={},
                best_combinations=[],
                part_flags={},
                n_texts_analyzed=n_scored,
                n_texts_total=len(all_headlines) if all_headlines else n_scored,
            )

        scored_headlines = [t.headline for t in texts]

        # Use all headlines for auto-detection if available
        detection_headlines = all_headlines if all_headlines else scored_headlines

        # Step 1: Detect parts
        if custom_parts:
            parts = custom_parts
        else:
            parts = self._auto_detect_parts(detection_headlines)

        if not parts:
            return TextPartAnalysisResult(
                parts_detected=[],
                part_impacts={},
                best_combinations=[],
                part_flags={},
                n_texts_analyzed=n_scored,
                n_texts_total=len(detection_headlines),
            )

        # Step 2: Build flags on SCORED headlines (for metric analysis)
        part_flags = self._build_flags(scored_headlines, parts)

        # Remove parts that match too few or too many texts, track reasons
        filtered_parts = []
        filtered_flags = {}
        excluded_parts = []
        for part in parts:
            n_true = sum(part_flags[part])
            n_false = len(part_flags[part]) - n_true
            n_total = len(part_flags[part])

            if n_true >= 2 and n_false >= 2:
                filtered_parts.append(part)
                filtered_flags[part] = part_flags[part]
            else:
                if n_true == 0:
                    reason = "none_match"
                    message = f"Не найден ни в одном из {n_total} текстов"
                elif n_true == n_total:
                    reason = "all_match"
                    message = f"Присутствует во всех {n_total} текстах — нет текстов без элемента для сравнения"
                elif n_false < 2:
                    reason = "too_many"
                    message = f"Присутствует в {n_true} из {n_total} — недостаточно текстов БЕЗ элемента (нужно мин. 2)"
                else:
                    reason = "too_few"
                    message = f"Найден только в {n_true} из {n_total} текстов — нужно минимум 2"
                excluded_parts.append(ExcludedPartInfo(
                    part_name=part, reason=reason,
                    n_with=n_true, n_total=n_total, message=message,
                ))

        parts = filtered_parts
        part_flags = filtered_flags

        # Step 3: Analyze impact per metric (with FDR)
        available_metrics = set()
        for t in texts:
            available_metrics.update(t.metrics.keys())
        available_metrics.add("composite_score")

        all_impacts: Dict[str, List[PartImpact]] = {}
        for metric in sorted(available_metrics):
            if metric == "composite_score":
                values = [t.composite_score for t in texts]
                valid_mask = [True] * len(texts)
            else:
                values = []
                valid_mask = []
                for t in texts:
                    v = t.metrics.get(metric)
                    if v is not None:
                        values.append(v)
                        valid_mask.append(True)
                    else:
                        values.append(0.0)
                        valid_mask.append(False)

            filtered_values = [v for v, vm in zip(values, valid_mask) if vm]
            filtered_headlines = [h for h, vm in zip(scored_headlines, valid_mask) if vm]
            filtered_part_flags = {}
            for part, flags in part_flags.items():
                filtered_part_flags[part] = [
                    f for f, vm in zip(flags, valid_mask) if vm
                ]

            if len(filtered_values) < 4:
                continue

            impacts = self._analyze_impact(
                filtered_headlines, filtered_values, filtered_part_flags,
                metric,
            )
            if impacts:
                all_impacts[metric] = impacts

        # Step 3b: Apply FDR correction across ALL p-values (all metrics × all parts)
        all_pvalues: List[float] = []
        all_refs: List[tuple] = []  # (metric_key, impact_index)
        for metric_key, impacts_list in all_impacts.items():
            for idx, imp in enumerate(impacts_list):
                all_pvalues.append(imp.p_value)
                all_refs.append((metric_key, idx))

        if len(all_pvalues) > 1:
            k = len(all_pvalues)
            # Compute BH-adjusted p-values
            sorted_indices = sorted(range(k), key=lambda i: all_pvalues[i])
            adjusted_p = [0.0] * k
            for rank_idx, orig_idx in enumerate(sorted_indices):
                rank = rank_idx + 1
                adjusted_p[orig_idx] = min(1.0, all_pvalues[orig_idx] * k / rank)
            # Enforce monotonicity (step-down)
            for i in range(len(sorted_indices) - 2, -1, -1):
                idx_curr = sorted_indices[i]
                idx_next = sorted_indices[i + 1]
                adjusted_p[idx_curr] = min(adjusted_p[idx_curr], adjusted_p[idx_next])

            # Re-classify confidence with adjusted p-values
            for i, (metric_key, impact_idx) in enumerate(all_refs):
                imp = all_impacts[metric_key][impact_idx]
                adj_p = adjusted_p[i]
                new_conf = self._classify_confidence(adj_p, imp.effect_size)
                all_impacts[metric_key][impact_idx] = imp.model_copy(update={
                    "p_value": round(adj_p, 6),
                    "confidence": new_conf,
                    "significant": new_conf in ("high", "medium"),
                })

        # Step 4: Find best combinations for primary metric (deduplicated)
        headlines = scored_headlines
        if primary_metric == "composite_score":
            primary_values = [t.composite_score for t in texts]
        else:
            primary_values = []
            primary_valid = []
            for t in texts:
                v = t.metrics.get(primary_metric)
                if v is not None:
                    primary_values.append(v)
                    primary_valid.append(True)
                else:
                    primary_values.append(0.0)
                    primary_valid.append(False)
            primary_values = [v for v, vm in zip(primary_values, primary_valid) if vm]
            headlines = [h for h, vm in zip(headlines, primary_valid) if vm]
            combo_flags = {}
            for part, flags in part_flags.items():
                combo_flags[part] = [f for f, vm in zip(flags, primary_valid) if vm]
            part_flags = combo_flags

        best_combos = self._find_combinations(
            headlines, primary_values, part_flags, max_combination_size
        )

        flags_serializable = {k: v for k, v in part_flags.items()}

        # Step 5: Generate summary for primary metric
        is_cost = primary_metric.startswith("CPA") or primary_metric.startswith("CPC") or primary_metric.startswith("CPM")
        primary_impacts = all_impacts.get(primary_metric, [])
        summary = self._generate_summary(primary_impacts, n_scored, primary_metric, is_cost)

        return TextPartAnalysisResult(
            parts_detected=parts,
            part_impacts=all_impacts,
            best_combinations=best_combos,
            part_flags=flags_serializable,
            n_texts_analyzed=n_scored,
            n_texts_total=len(detection_headlines),
            excluded_parts=excluded_parts,
            summary=summary,
        )

    def extract_all_words(
        self,
        headlines: List[str],
        min_length: int = 3,
        include_bigrams: bool = True,
    ) -> List[ExtractedWord]:
        """Extract ALL unique meaningful words/n-grams from headlines with per-text frequency."""
        n_texts = len(headlines)
        if n_texts == 0:
            return []

        n_range = (1, 2) if include_bigrams else (1, 1)

        # Collect unique ngrams per text to count texts (not occurrences)
        text_counts: Dict[str, int] = {}
        for headline in headlines:
            raw_words = headline.lower().split()
            words = [self._clean_word(w) for w in raw_words]
            words = [w for w in words if w]

            text_ngrams = set()
            for n in range(n_range[0], n_range[1] + 1):
                for i in range(len(words) - n + 1):
                    gram_words = words[i:i + n]

                    if all(w in STOPWORDS_RU for w in gram_words):
                        continue
                    if n == 1 and gram_words[0] in STOPWORDS_RU:
                        continue
                    if n == 1 and gram_words[0].isdigit():
                        continue
                    if n > 1 and any(w in STOPWORDS_RU or w.isdigit() for w in gram_words):
                        continue

                    ngram = " ".join(gram_words)
                    if len(ngram) >= min_length:
                        text_ngrams.add(ngram)

            for ngram in text_ngrams:
                text_counts[ngram] = text_counts.get(ngram, 0) + 1

        # Filter: at least 2 texts
        result = []
        for word, count in sorted(text_counts.items(), key=lambda x: x[1], reverse=True):
            if count >= 2:
                result.append(ExtractedWord(
                    word=word,
                    count=count,
                    frequency=round(count / n_texts, 4),
                ))

        return result
