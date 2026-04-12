"""Video scoring service — aggregate CQS from keyframe analysis."""

import logging
from typing import List, Optional

from adscore_tagger import tag_banner
from video_processor import (
    KeyFrame, VideoMeta,
    compute_video_specific_score,
)

logger = logging.getLogger(__name__)

# Weights for aggregating keyframe CQS into video CQS
FRAME_WEIGHTS = {
    "hook": 0.35,
    "cta": 0.25,
    "scene_change": 0.15,  # split among all scene_change frames
    "interval": 0.15,      # split among all interval frames
}
VIDEO_SPECIFIC_WEIGHT = 0.25
FRAME_TOTAL_WEIGHT = 1 - VIDEO_SPECIFIC_WEIGHT  # 0.75


def _compute_frame_cqs(tags: dict) -> float:
    """
    Compute CQS for a single frame.

    Server-side simplified scoring that mirrors the frontend
    CreativeScoreGauge 6-dimension weighted model.
    """
    return _fallback_cqs(tags)


def _fallback_cqs(tags: dict) -> float:
    """Simple CQS estimation when the full scorer is unavailable."""
    score = 50.0  # baseline

    visual = tags.get("visual", {})
    text = tags.get("text_elements", {})
    structural = tags.get("structural", {})
    emotional = tags.get("emotional", {})

    # Visual quality
    dq = visual.get("design_quality", "средний")
    if dq == "профессиональный":
        score += 15
    elif dq == "средний":
        score += 5
    elif dq in ("любительский", "примитивный"):
        score -= 10

    # Thumb-stop potential
    pf = tags.get("platform_fit", {})
    ts = pf.get("thumb_stop_potential", "средний")
    if ts == "высокий":
        score += 10
    elif ts == "низкий":
        score -= 10

    # CTA presence
    if structural.get("has_cta_button"):
        score += 8

    # Product visible
    if structural.get("product_visible"):
        score += 5

    # Emotional triggers
    triggers = emotional.get("emotional_triggers", [])
    if triggers and triggers != ["нет"]:
        score += len(triggers) * 3

    return max(0, min(100, score))


def score_video_keyframes(
    keyframes: List[KeyFrame],
    meta: VideoMeta,
) -> dict:
    """
    Score all keyframes via Claude Vision and aggregate into video CQS.

    Returns:
    {
        "keyframes": [
            {"index": 0, "timestamp": 0.0, "frame_type": "hook",
             "tags": {...}, "cqs_score": 72},
            ...
        ],
        "video_cqs": 68,
        "video_specific_score": 75,
        "hook_cqs": 72,
        "cta_cqs": 65,
        "avg_scene_cqs": 60,
        "scene_count": 5,
    }
    """
    scored_frames = []
    hook_cqs = None
    cta_cqs = None
    scene_scores = []
    interval_scores = []

    for kf in keyframes:
        logger.info("Tagging keyframe %d (%.1fs, type=%s)", kf.index, kf.timestamp, kf.frame_type)
        try:
            tags = tag_banner(kf.image_path)
            cqs = _compute_frame_cqs(tags)
        except Exception as e:
            logger.error("Failed to tag keyframe %d: %s", kf.index, e)
            tags = {}
            cqs = 0

        scored_frames.append({
            "index": kf.index,
            "timestamp": kf.timestamp,
            "frame_type": kf.frame_type,
            "tags": tags,
            "cqs_score": round(cqs),
        })

        if kf.frame_type == "hook":
            hook_cqs = cqs
        elif kf.frame_type == "cta":
            cta_cqs = cqs
        elif kf.frame_type == "scene_change":
            scene_scores.append(cqs)
        elif kf.frame_type == "interval":
            interval_scores.append(cqs)

    # Aggregate CQS
    # Determine what hook/cta frame text elements say
    hook_tags = next((f["tags"] for f in scored_frames if f["frame_type"] == "hook"), {})
    cta_tags = next((f["tags"] for f in scored_frames if f["frame_type"] == "cta"), {})

    hook_has_text = bool(
        hook_tags.get("text_elements", {}).get("headline")
        or hook_tags.get("text_elements", {}).get("cta_text")
    )
    cta_has_cta = bool(
        cta_tags.get("structural", {}).get("has_cta_button")
        or cta_tags.get("text_elements", {}).get("cta_text")
    )

    scene_count = len(scene_scores) + len(interval_scores)
    video_specific = compute_video_specific_score(meta, scene_count, hook_has_text, cta_has_cta)

    # Weighted average of frame scores
    total_frame_score = 0.0
    total_weight = 0.0

    if hook_cqs is not None:
        total_frame_score += hook_cqs * FRAME_WEIGHTS["hook"]
        total_weight += FRAME_WEIGHTS["hook"]

    if cta_cqs is not None:
        total_frame_score += cta_cqs * FRAME_WEIGHTS["cta"]
        total_weight += FRAME_WEIGHTS["cta"]

    middle_scores = scene_scores + interval_scores
    if middle_scores:
        avg_middle = sum(middle_scores) / len(middle_scores)
        middle_weight = FRAME_WEIGHTS["scene_change"] + FRAME_WEIGHTS["interval"]
        total_frame_score += avg_middle * middle_weight
        total_weight += middle_weight

    # Normalize frame score
    frame_cqs = total_frame_score / total_weight if total_weight > 0 else 50

    # Final video CQS
    video_cqs = FRAME_TOTAL_WEIGHT * frame_cqs + VIDEO_SPECIFIC_WEIGHT * video_specific

    avg_scene = (sum(scene_scores + interval_scores) / len(scene_scores + interval_scores)) if (scene_scores + interval_scores) else None

    return {
        "keyframes": scored_frames,
        "video_cqs": round(video_cqs),
        "video_specific_score": round(video_specific),
        "hook_cqs": round(hook_cqs) if hook_cqs is not None else None,
        "cta_cqs": round(cta_cqs) if cta_cqs is not None else None,
        "avg_scene_cqs": round(avg_scene) if avg_scene is not None else None,
        "scene_count": scene_count,
    }
