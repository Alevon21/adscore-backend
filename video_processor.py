"""Video processing service — keyframe extraction via FFmpeg."""

import json
import logging
import os
import subprocess
import tempfile
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Optional

import imageio_ffmpeg

logger = logging.getLogger(__name__)

# Get FFmpeg binary path from imageio-ffmpeg package
FFMPEG_EXE = imageio_ffmpeg.get_ffmpeg_exe()
# imageio-ffmpeg doesn't ship ffprobe, so we use ffmpeg -i for probing

# Scene detection threshold (0.0-1.0, lower = more sensitive)
SCENE_THRESHOLD = 0.3
# Max keyframes to extract
MAX_KEYFRAMES = 8
# Min keyframes (if scene detection finds fewer, use interval fallback)
MIN_KEYFRAMES = 3
# Optimal video duration range for mobile ads (seconds)
OPTIMAL_DURATION_MIN = 15
OPTIMAL_DURATION_MAX = 30


@dataclass
class VideoMeta:
    duration: float  # seconds
    width: int
    height: int
    fps: float
    codec: str
    bitrate: Optional[int] = None


@dataclass
class KeyFrame:
    index: int
    timestamp: float  # seconds from start
    frame_type: str  # 'hook' | 'scene_change' | 'cta' | 'interval'
    image_path: str  # local path to extracted PNG


def get_video_metadata(video_path: str) -> VideoMeta:
    """Extract video metadata using ffmpeg -i (no ffprobe needed)."""
    import re

    cmd = [FFMPEG_EXE, "-i", video_path, "-hide_banner"]
    # ffmpeg -i exits with code 1 when no output is specified, but still prints info to stderr
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    info = result.stderr

    # Parse duration: "Duration: 00:00:15.03"
    dur_match = re.search(r"Duration:\s*(\d+):(\d+):(\d[\d.]*)", info)
    if dur_match:
        h, m, s = dur_match.groups()
        duration = int(h) * 3600 + int(m) * 60 + float(s)
    else:
        duration = 0.0

    # Parse video stream: "Stream #0:0... Video: h264 ... 1080x1920 ... 30 fps"
    stream_match = re.search(
        r"Stream.*Video:\s*(\w+).*?(\d{2,5})x(\d{2,5})", info
    )
    if not stream_match:
        raise ValueError("No video stream found in file")

    codec = stream_match.group(1)
    width = int(stream_match.group(2))
    height = int(stream_match.group(3))

    # Parse fps: "30 fps" or "29.97 fps"
    fps_match = re.search(r"([\d.]+)\s*fps", info)
    fps = float(fps_match.group(1)) if fps_match else 30.0

    # Parse bitrate: "bitrate: 1234 kb/s"
    br_match = re.search(r"bitrate:\s*(\d+)\s*kb/s", info)
    bitrate = int(br_match.group(1)) * 1000 if br_match else None

    return VideoMeta(
        duration=duration,
        width=width,
        height=height,
        fps=round(fps, 2),
        codec=codec,
        bitrate=bitrate,
    )


def _extract_frame_at(video_path: str, timestamp: float, output_path: str) -> bool:
    """Extract a single frame at the given timestamp."""
    cmd = [
        FFMPEG_EXE, "-y", "-v", "quiet",
        "-ss", str(timestamp),
        "-i", video_path,
        "-vframes", "1",
        "-q:v", "2",  # high quality JPEG
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, timeout=30)
    return result.returncode == 0 and os.path.exists(output_path)


def _detect_scene_changes(video_path: str, threshold: float = SCENE_THRESHOLD) -> List[float]:
    """Detect scene change timestamps using FFmpeg scene filter."""
    cmd = [
        FFMPEG_EXE, "-v", "quiet",
        "-i", video_path,
        "-vf", f"select='gt(scene,{threshold})',showinfo",
        "-vsync", "vfr",
        "-f", "null", "-",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

    timestamps = []
    for line in result.stderr.split("\n"):
        if "pts_time:" in line:
            try:
                pts_part = line.split("pts_time:")[1].split()[0]
                ts = float(pts_part)
                timestamps.append(ts)
            except (IndexError, ValueError):
                continue

    return timestamps


def extract_keyframes(video_path: str, output_dir: str = None) -> tuple[VideoMeta, List[KeyFrame]]:
    """
    Extract keyframes from a video file.

    Strategy:
    1. Always extract first frame (hook) and last-3-sec frame (CTA)
    2. Use scene detection for middle frames
    3. If scene detection finds < MIN_KEYFRAMES, add evenly-spaced interval frames

    Returns (VideoMeta, list of KeyFrame).
    """
    meta = get_video_metadata(video_path)

    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="adscore_keyframes_")
    os.makedirs(output_dir, exist_ok=True)

    keyframes: List[KeyFrame] = []
    frame_idx = 0

    # 1. First frame (hook)
    hook_path = os.path.join(output_dir, f"frame_{frame_idx:03d}_hook.jpg")
    if _extract_frame_at(video_path, 0.0, hook_path):
        keyframes.append(KeyFrame(
            index=frame_idx, timestamp=0.0,
            frame_type="hook", image_path=hook_path,
        ))
        frame_idx += 1

    # 2. Scene changes (middle frames)
    scene_timestamps = _detect_scene_changes(video_path)
    # Filter: skip timestamps too close to start/end (within 1 sec)
    scene_timestamps = [
        ts for ts in scene_timestamps
        if 1.0 < ts < meta.duration - 1.0
    ]
    # Limit to MAX_KEYFRAMES - 2 (hook + CTA reserved)
    max_scenes = MAX_KEYFRAMES - 2
    if len(scene_timestamps) > max_scenes:
        # Keep evenly distributed subset
        step = len(scene_timestamps) / max_scenes
        scene_timestamps = [scene_timestamps[int(i * step)] for i in range(max_scenes)]

    for ts in scene_timestamps:
        scene_path = os.path.join(output_dir, f"frame_{frame_idx:03d}_scene.jpg")
        if _extract_frame_at(video_path, ts, scene_path):
            keyframes.append(KeyFrame(
                index=frame_idx, timestamp=round(ts, 2),
                frame_type="scene_change", image_path=scene_path,
            ))
            frame_idx += 1

    # 3. Interval fallback: if fewer than MIN_KEYFRAMES middle frames, add evenly spaced
    middle_count = len(keyframes) - 1  # excluding hook
    if middle_count < MIN_KEYFRAMES - 1 and meta.duration > 3:
        needed = MIN_KEYFRAMES - 1 - middle_count
        existing_ts = {kf.timestamp for kf in keyframes}
        interval = meta.duration / (needed + 2)  # +2 to skip start/end
        for i in range(1, needed + 1):
            ts = round(interval * i, 2)
            if ts not in existing_ts and 0.5 < ts < meta.duration - 0.5:
                interval_path = os.path.join(output_dir, f"frame_{frame_idx:03d}_interval.jpg")
                if _extract_frame_at(video_path, ts, interval_path):
                    keyframes.append(KeyFrame(
                        index=frame_idx, timestamp=ts,
                        frame_type="interval", image_path=interval_path,
                    ))
                    frame_idx += 1

    # 4. CTA frame (last 3 seconds or last frame)
    cta_ts = max(0.0, meta.duration - 2.0)
    cta_path = os.path.join(output_dir, f"frame_{frame_idx:03d}_cta.jpg")
    if _extract_frame_at(video_path, cta_ts, cta_path):
        keyframes.append(KeyFrame(
            index=frame_idx, timestamp=round(cta_ts, 2),
            frame_type="cta", image_path=cta_path,
        ))

    # Sort by timestamp
    keyframes.sort(key=lambda kf: kf.timestamp)
    for i, kf in enumerate(keyframes):
        kf.index = i

    logger.info(
        "Extracted %d keyframes from %s (duration: %.1fs, scenes: %d)",
        len(keyframes), video_path, meta.duration, len(scene_timestamps),
    )

    return meta, keyframes


def compute_video_specific_score(meta: VideoMeta, scene_count: int,
                                  hook_has_text: bool, cta_has_cta: bool) -> float:
    """
    Compute video-specific quality score (0-100) based on:
    - Duration (optimal 15-30s for mobile)
    - Scene rhythm (2-6 scenes per 15s is good)
    - Hook has text overlay
    - CTA endcard present
    """
    score = 0.0

    # Duration score (0-30 points)
    if OPTIMAL_DURATION_MIN <= meta.duration <= OPTIMAL_DURATION_MAX:
        score += 30.0
    elif 10 <= meta.duration <= 60:
        # Partial score for acceptable range
        if meta.duration < OPTIMAL_DURATION_MIN:
            score += 30.0 * (meta.duration / OPTIMAL_DURATION_MIN)
        else:
            score += 30.0 * max(0, 1 - (meta.duration - OPTIMAL_DURATION_MAX) / 30)
    else:
        score += 5.0  # too short or too long

    # Scene rhythm (0-30 points)
    scenes_per_15s = scene_count / max(meta.duration / 15, 1)
    if 2 <= scenes_per_15s <= 6:
        score += 30.0
    elif 1 <= scenes_per_15s <= 8:
        score += 20.0
    else:
        score += 10.0

    # Hook has text (0-20 points)
    if hook_has_text:
        score += 20.0
    else:
        score += 5.0

    # CTA endcard (0-20 points)
    if cta_has_cta:
        score += 20.0
    else:
        score += 5.0

    return min(100.0, score)


def keyframe_to_dict(kf: KeyFrame) -> dict:
    """Convert KeyFrame to dict for JSON storage (without local image_path)."""
    return {
        "index": kf.index,
        "timestamp": kf.timestamp,
        "frame_type": kf.frame_type,
    }


def video_meta_to_dict(meta: VideoMeta) -> dict:
    return asdict(meta)
