"""
Full platform ad specs reference — 53 platforms, CIS + Global.
Source: mobile_ad_formats_all_platforms.md (April 2025)
Used by creative_analytics.py to populate creative briefs.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Each platform entry: id, platform name, region, formats list
# Each format: name, type (image/video/playable/html5/native/text),
#   sizes, file_formats, max_file_size, notes (optional)
# ---------------------------------------------------------------------------

PLATFORMS: list[dict] = [
    # ===== РАЗДЕЛ I: СНГ =====
    {
        "id": "yandex_direct",
        "platform": "Яндекс Директ",
        "region": "СНГ",
        "url": "https://direct.yandex.ru/",
        "model": "CPC, CPA, oCPM",
        "formats": [
            {
                "name": "ТГО (текстово-графические)",
                "type": "image",
                "sizes": ["1080x1080 (1:1)", "1920x1080 (16:9)", "1080x1920 (9:16)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "10 МБ",
                "notes": "450-5000px min сторона. До 5 заголовков + 3 текста + 5 изображений",
            },
            {
                "name": "Графические баннеры (РСЯ)",
                "type": "image",
                "sizes": ["320x50", "320x100", "320x480", "300x250", "240x400", "728x90", "970x250"],
                "file_formats": ["JPG", "PNG", "GIF"],
                "max_file_size": "512 КБ",
                "notes": "Retina 2x/3x рекомендуется",
            },
            {
                "name": "Видеореклама",
                "type": "video",
                "sizes": ["16:9", "1:1", "9:16"],
                "file_formats": ["MP4", "WebM", "MOV"],
                "max_file_size": "100 МБ",
                "notes": "5-60 сек; мин. 360p, рек. 1080p; H.264/VP8",
            },
            {
                "name": "Playable Ads (HTML5)",
                "type": "playable",
                "sizes": ["320x50", "320x100", "320x480", "480x320", "300x250"],
                "file_formats": ["ZIP (HTML5)"],
                "max_file_size": "3 МБ",
                "notes": "index.html ≤150 КБ, ≤20 файлов",
            },
        ],
    },
    {
        "id": "vk_ads",
        "platform": "VK Реклама",
        "region": "СНГ",
        "url": "https://ads.vk.com/",
        "model": "oCPM, CPC",
        "formats": [
            {
                "name": "Универсальные объявления",
                "type": "image",
                "sizes": ["600x600 (1:1)", "1080x1350 (4:5)", "1080x607 (16:9)", "607x1080 (9:16)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "5 МБ",
                "notes": "Текст ≤20% площади. Безопасная зона 10% сверху",
            },
            {
                "name": "Видео",
                "type": "video",
                "sizes": ["1:1", "4:5", "16:9", "9:16"],
                "file_formats": ["MP4", "MOV", "AVI"],
                "max_file_size": "90 МБ",
                "notes": "Мин. 600px ширина. Рек. 1280x720+. Безопасные зоны: 10% сверху, 20% снизу",
            },
            {
                "name": "Карусель",
                "type": "image",
                "sizes": ["600x600 (1:1, каждый слайд)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "5 МБ",
                "notes": "3-6 слайдов. Заголовок до 25 симв.",
            },
            {
                "name": "Rewarded Video",
                "type": "video",
                "sizes": ["640x360", "1280x720", "1920x1080"],
                "file_formats": ["MP4 (H.264+AAC)"],
                "max_file_size": "10 МБ",
                "notes": "До 30 сек. 400-450 кбит/с видео, до 25 fps",
            },
            {
                "name": "Playable Ads",
                "type": "playable",
                "sizes": ["адаптивный"],
                "file_formats": ["ZIP (HTML5)"],
                "max_file_size": "2 МБ",
                "notes": "15-60 сек",
            },
        ],
    },
    {
        "id": "rustore",
        "platform": "RuStore Ads",
        "region": "СНГ",
        "url": "https://ads.rustore.ru/",
        "model": "CPC, CPM",
        "formats": [
            {
                "name": "Баннер рекомендаций",
                "type": "image",
                "sizes": ["1440x720 (2:1)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "2 МБ",
            },
            {
                "name": "Баннеры категорий",
                "type": "image",
                "sizes": ["1200x300 (4:1)", "600x600 (1:1)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "1 МБ",
            },
        ],
    },
    {
        "id": "telegram",
        "platform": "Telegram Ads",
        "region": "СНГ",
        "url": "https://ads.telegram.org/",
        "model": "CPM, CPC",
        "formats": [
            {
                "name": "Sponsored Messages",
                "type": "text",
                "sizes": [],
                "file_formats": [],
                "max_file_size": "—",
                "notes": "До 160 символов. CTA до 30 символов. Только Telegram-ссылки",
            },
            {
                "name": "Медиа Sponsored (через партнёров)",
                "type": "image",
                "sizes": ["1280x720", "800x800"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "—",
                "notes": "Видео: MP4, до 10 МБ, до 15 сек",
            },
            {
                "name": "Mini Apps баннеры",
                "type": "image",
                "sizes": ["320x50", "300x250", "320x480"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "500 КБ",
                "notes": "Через партнёрские сети (RichAds, PropellerAds)",
            },
        ],
    },
    {
        "id": "mts_adtech",
        "platform": "MTS AdTech",
        "region": "СНГ",
        "url": "https://adtech.mts.ru/",
        "model": "CPM, CPC, CPA",
        "formats": [
            {
                "name": "Мобильный баннер",
                "type": "image",
                "sizes": ["320x50", "300x250", "320x100"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "150 КБ",
            },
            {
                "name": "Полноэкранный баннер",
                "type": "image",
                "sizes": ["320x480", "480x320"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "300 КБ",
            },
            {
                "name": "Видео In-stream",
                "type": "video",
                "sizes": ["16:9, 1280x720"],
                "file_formats": ["MP4"],
                "max_file_size": "50 МБ",
                "notes": "15-30 сек",
            },
        ],
    },
    {
        "id": "sber_ads",
        "platform": "Сбер Ads",
        "region": "СНГ",
        "url": "https://sberads.ru/",
        "model": "CPM, CPC, CPA",
        "formats": [
            {
                "name": "Баннер in-app",
                "type": "image",
                "sizes": ["320x50", "300x250", "320x480"],
                "file_formats": ["JPG", "PNG", "GIF"],
                "max_file_size": "150 КБ",
            },
            {
                "name": "Видео in-stream",
                "type": "video",
                "sizes": ["16:9 / 9:16, 720-1080p"],
                "file_formats": ["MP4"],
                "max_file_size": "100 МБ",
                "notes": "6-30 сек",
            },
            {
                "name": "Нативный",
                "type": "native",
                "sizes": ["1200x627", "600x600"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "—",
            },
        ],
    },
    {
        "id": "dzen",
        "platform": "Дзен (VK Реклама)",
        "region": "СНГ",
        "url": "https://ads.vk.com/",
        "model": "CPM, oCPM",
        "formats": [
            {
                "name": "Нативная карточка",
                "type": "image",
                "sizes": ["658x370 (16:9)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "1 МБ",
                "notes": "Заголовок до 60 симв., тизер до 90 симв.",
            },
            {
                "name": "Видео в ленте",
                "type": "video",
                "sizes": ["16:9, 1280x720+", "9:16, 1080x1920"],
                "file_formats": ["MP4"],
                "max_file_size": "1 ГБ",
                "notes": "До 30 сек",
            },
        ],
    },
    # ===== РАЗДЕЛ II: ГЛОБАЛЬНЫЕ WALLED GARDENS =====
    {
        "id": "apple_search_ads",
        "platform": "Apple Search Ads",
        "region": "Глобальный",
        "url": "https://ads.apple.com/",
        "model": "CPT (Cost Per Tap)",
        "formats": [
            {
                "name": "Search Results",
                "type": "native",
                "sizes": ["Автоматически из App Store Connect"],
                "file_formats": [],
                "max_file_size": "—",
                "notes": "Иконка 1024x1024, до 3 скриншотов, превью до 30 сек",
            },
            {
                "name": "Today Tab",
                "type": "image",
                "sizes": ["960x1600 (iPhone)", "2048x2732 (iPad)"],
                "file_formats": ["через App Store Connect"],
                "max_file_size": "—",
                "notes": "Только через Apple Sales",
            },
        ],
    },
    {
        "id": "google_uac",
        "platform": "Google App Campaigns",
        "region": "Глобальный",
        "url": "https://ads.google.com/",
        "model": "tCPA, tROAS",
        "formats": [
            {
                "name": "Изображения",
                "type": "image",
                "sizes": ["1200x1200 (1:1)", "1200x628 (16:9)", "628x1200 (9:16)", "1200x628 (1.91:1)"],
                "file_formats": ["JPG", "PNG", "GIF"],
                "max_file_size": "5 МБ",
                "notes": "Мин. 300x300 (1:1), 600x314 (16:9). Система сама комбинирует",
            },
            {
                "name": "Видео (YouTube)",
                "type": "video",
                "sizes": ["16:9", "1:1", "9:16 (Shorts)"],
                "file_formats": ["YouTube ссылки"],
                "max_file_size": "—",
                "notes": "Рек. 15-30 сек. Skippable/Non-Skippable/Shorts",
            },
            {
                "name": "HTML5",
                "type": "html5",
                "sizes": ["300x250", "320x50", "320x480", "480x320", "728x90"],
                "file_formats": ["HTML5"],
                "max_file_size": "1 МБ",
            },
        ],
    },
    {
        "id": "meta_ads",
        "platform": "Meta (Facebook / Instagram)",
        "region": "Глобальный",
        "url": "https://www.facebook.com/business/ads/",
        "model": "CPI, CPA, oCPM",
        "formats": [
            {
                "name": "Single Image",
                "type": "image",
                "sizes": ["1080x1080 (1:1, Feed)", "1080x1920 (9:16, Stories/Reels)", "1200x628 (AN)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "30 МБ",
                "notes": "Текст ≤20% площади",
            },
            {
                "name": "Single Video",
                "type": "video",
                "sizes": ["1080x1080 (1:1)", "1080x1350 (4:5)", "1080x1920 (9:16)"],
                "file_formats": ["MP4", "MOV", "GIF"],
                "max_file_size": "4 ГБ",
                "notes": "Рек. 15 сек. Feed: до 240 мин. Stories: до 60 сек",
            },
            {
                "name": "Карусель",
                "type": "image",
                "sizes": ["1080x1080 (1:1, каждый слайд)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "30 МБ",
                "notes": "2-10 слайдов. Заголовок до 40 симв.",
            },
        ],
    },
    {
        "id": "tiktok",
        "platform": "TikTok for Business",
        "region": "Глобальный",
        "url": "https://ads.tiktok.com/",
        "model": "oCPI, CPA, CPM",
        "formats": [
            {
                "name": "In-Feed Video",
                "type": "video",
                "sizes": ["1080x1920 (9:16, обязательно)"],
                "file_formats": ["MP4", "MOV", "AVI", "WebM"],
                "max_file_size": "500 МБ",
                "notes": "5-60 сек (рек. 9-15 сек). Мин. 540x960. Аудио обязательно. Первые 3 сек критичны",
            },
            {
                "name": "Image Ads (News Feed)",
                "type": "image",
                "sizes": ["1200x628 (16:9)", "640x640 (1:1)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "100 КБ",
                "notes": "Заголовок до 40 симв.",
            },
        ],
    },
    {
        "id": "snapchat",
        "platform": "Snapchat Ads",
        "region": "Глобальный",
        "url": "https://forbusiness.snapchat.com/",
        "model": "CPI, CPA, CPM",
        "formats": [
            {
                "name": "Snap Ads",
                "type": "video",
                "sizes": ["1080x1920 (9:16, обязательно)"],
                "file_formats": ["MP4", "MOV"],
                "max_file_size": "1 ГБ",
                "notes": "3-180 сек (рек. 3-10). Безопасная зона: 120px сверху, 250px снизу",
            },
            {
                "name": "Snap Ads (Image)",
                "type": "image",
                "sizes": ["1080x1920 (9:16)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "5 МБ",
            },
        ],
    },
    {
        "id": "pinterest",
        "platform": "Pinterest Ads",
        "region": "Глобальный",
        "url": "https://ads.pinterest.com/",
        "model": "CPC, CPM",
        "formats": [
            {
                "name": "Standard Pin",
                "type": "image",
                "sizes": ["1000x1500 (2:3, рек.)", "1080x1080 (1:1)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "32 МБ",
            },
            {
                "name": "Video Pin",
                "type": "video",
                "sizes": ["1:1", "2:3", "9:16 (рек.)"],
                "file_formats": ["MP4", "MOV"],
                "max_file_size": "2 ГБ",
                "notes": "4 сек — 15 мин (рек. 6-15 сек)",
            },
        ],
    },
    {
        "id": "reddit",
        "platform": "Reddit Ads",
        "region": "Глобальный",
        "url": "https://business.reddit.com/",
        "model": "CPI, CPM",
        "formats": [
            {
                "name": "Promoted Posts",
                "type": "image",
                "sizes": ["1200x628 (16:9)", "1080x1080 (1:1)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "20 МБ",
                "notes": "Видео: 16:9/1:1, 5-30 мин, MP4/MOV, ≤1 ГБ",
            },
        ],
    },
    {
        "id": "x_twitter",
        "platform": "X (Twitter)",
        "region": "Глобальный",
        "url": "https://business.x.com/",
        "model": "CPI, CPM",
        "formats": [
            {
                "name": "App Cards",
                "type": "image",
                "sizes": ["800x418 (1.91:1)", "800x800 (1:1)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "20 МБ",
                "notes": "Видео: 1280x720, до 2 мин 20 сек, MP4/MOV, ≤1 ГБ",
            },
        ],
    },
    # ===== РАЗДЕЛ III: IN-APP СЕТИ И DSP =====
    {
        "id": "unity_ads",
        "platform": "Unity Ads",
        "region": "Глобальный",
        "url": "https://unity.com/solutions/unity-ads/",
        "model": "CPI, CPA, CPM",
        "formats": [
            {
                "name": "Rewarded Video",
                "type": "video",
                "sizes": ["1280x720 (L)", "720x1280 (P)"],
                "file_formats": ["MP4 (H.264+AAC)"],
                "max_file_size": "50 МБ",
                "notes": "15-30 сек. Endcard: 1200x628, JPG, ≤1 МБ",
            },
            {
                "name": "Banner",
                "type": "image",
                "sizes": ["320x50", "728x90"],
                "file_formats": ["JPG", "PNG", "GIF"],
                "max_file_size": "150 КБ",
            },
            {
                "name": "Playable",
                "type": "playable",
                "sizes": ["адаптивный"],
                "file_formats": ["HTML5"],
                "max_file_size": "5 МБ",
            },
        ],
    },
    {
        "id": "ironsource",
        "platform": "IronSource (LevelPlay)",
        "region": "Глобальный",
        "url": "https://www.is.com/",
        "model": "CPI, CPM",
        "formats": [
            {
                "name": "Rewarded Video",
                "type": "video",
                "sizes": ["1920x1080 (L)", "1080x1920 (P)"],
                "file_formats": ["MP4 (H.264)"],
                "max_file_size": "50 МБ",
                "notes": "15-30 сек. Endcard обязателен: 1200x628/628x1200, JPG, ≤1 МБ",
            },
            {
                "name": "Banner",
                "type": "image",
                "sizes": ["320x50", "300x250", "728x90"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "150 КБ",
            },
        ],
    },
    {
        "id": "applovin",
        "platform": "AppLovin (MAX)",
        "region": "Глобальный",
        "url": "https://www.applovin.com/",
        "model": "CPI, CPA, CPM, ROAS",
        "formats": [
            {
                "name": "Rewarded / Interstitial Video",
                "type": "video",
                "sizes": ["1920x1080 (L)", "1080x1920 (P)"],
                "file_formats": ["MP4 (H.264+AAC)"],
                "max_file_size": "—",
                "notes": "15-30 сек. Endcard: 1200x628/628x1200, JPG, ≤1 МБ",
            },
            {
                "name": "Banner",
                "type": "image",
                "sizes": ["320x50", "728x90", "300x250"],
                "file_formats": ["JPG", "PNG", "GIF"],
                "max_file_size": "150 КБ",
            },
            {
                "name": "Playable",
                "type": "playable",
                "sizes": ["адаптивный"],
                "file_formats": ["HTML5"],
                "max_file_size": "5 МБ",
            },
        ],
    },
    {
        "id": "mintegral",
        "platform": "Mintegral",
        "region": "Глобальный",
        "url": "https://www.mintegral.com/",
        "model": "CPI, CPM, tROAS",
        "formats": [
            {
                "name": "Rewarded / Interstitial Video",
                "type": "video",
                "sizes": ["1280x720", "720x1280", "1920x1080", "1080x1920"],
                "file_formats": ["MP4"],
                "max_file_size": "200 МБ",
                "notes": "Рек. 6/15/30/60 сек. Endcard 1200x627 обязателен. Иконка 512x512 обязательна",
            },
            {
                "name": "Banner",
                "type": "image",
                "sizes": ["1200x627", "720x1280", "320x50", "600x600", "728x90", "300x250"],
                "file_formats": ["JPG", "PNG", "GIF"],
                "max_file_size": "5 МБ",
            },
            {
                "name": "Playable",
                "type": "playable",
                "sizes": ["адаптивный"],
                "file_formats": ["HTML5"],
                "max_file_size": "5 МБ",
                "notes": "+ 1200x627 image + 512x512 icon обязательны",
            },
        ],
    },
    {
        "id": "moloco",
        "platform": "Moloco",
        "region": "Глобальный",
        "url": "https://www.moloco.com/",
        "model": "CPI, CPA, CPM, tROAS",
        "formats": [
            {
                "name": "Image (все размеры)",
                "type": "image",
                "sizes": ["300x250", "320x480", "320x50", "728x90", "480x320", "768x1024"],
                "file_formats": ["JPG", "PNG", "GIF"],
                "max_file_size": "—",
                "notes": "Шрифт мин. 10pt. Загружайте ВСЕ размеры для макс. охвата",
            },
            {
                "name": "Video",
                "type": "video",
                "sizes": ["1280x720 (16:9)", "720x1280 (9:16)"],
                "file_formats": ["MP4"],
                "max_file_size": "—",
                "notes": "16-30 / 31-60 / 61-120 сек",
            },
            {
                "name": "Native",
                "type": "native",
                "sizes": ["1200x627 (обязательно)", "627x627 (обязательно)"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "—",
                "notes": "1200x627 + 627x627 критичны для японских бирж",
            },
        ],
    },
    {
        "id": "liftoff",
        "platform": "Liftoff (Vungle)",
        "region": "Глобальный",
        "url": "https://liftoff.io/",
        "model": "CPA, CPM",
        "formats": [
            {
                "name": "Rewarded / Interstitial Video",
                "type": "video",
                "sizes": ["1920x1080", "1080x1920"],
                "file_formats": ["MP4"],
                "max_file_size": "200 МБ",
                "notes": "15-60 сек",
            },
            {
                "name": "Banner",
                "type": "image",
                "sizes": ["320x50", "300x250"],
                "file_formats": ["JPG", "PNG"],
                "max_file_size": "150 КБ",
            },
            {
                "name": "Playable",
                "type": "playable",
                "sizes": ["адаптивный"],
                "file_formats": ["HTML5"],
                "max_file_size": "5 МБ",
            },
        ],
    },
]

# ---------------------------------------------------------------------------
# Universal format matrix (covers all platforms)
# ---------------------------------------------------------------------------

UNIVERSAL_FORMATS: list[dict] = [
    {"name": "Видео вертикальное", "size": "1080x1920", "ar": "9:16", "format": "MP4", "max_size": "100-200 МБ",
     "platforms": "Яндекс, VK, Meta, TikTok, Snap, BIGO, Mintegral, Moloco, Kayzen, Liftoff, все in-app"},
    {"name": "Видео горизонтальное", "size": "1920x1080", "ar": "16:9", "format": "MP4", "max_size": "100-200 МБ",
     "platforms": "Яндекс, VK, Meta, Google, Unity, Mintegral, Moloco, все in-app"},
    {"name": "Видео квадратное", "size": "1080x1080", "ar": "1:1", "format": "MP4", "max_size": "50-100 МБ",
     "platforms": "Meta, Google, VK"},
    {"name": "Изображение горизонтальное", "size": "1200x628", "ar": "1.91:1", "format": "JPG/PNG", "max_size": "5 МБ",
     "platforms": "Google, Meta, Mintegral, Moloco, все native-форматы"},
    {"name": "Изображение квадратное", "size": "1080x1080", "ar": "1:1", "format": "JPG/PNG", "max_size": "5 МБ",
     "platforms": "Meta, VK, TikTok, Snap"},
    {"name": "Изображение вертикальное", "size": "1080x1350 / 1080x1920", "ar": "4:5 / 9:16", "format": "JPG/PNG", "max_size": "5 МБ",
     "platforms": "Meta, VK, TikTok, Snap, Pinterest"},
    {"name": "Иконка приложения", "size": "512x512", "ar": "1:1", "format": "JPG/PNG", "max_size": "1 МБ",
     "platforms": "Mintegral, Moloco, Unity, Pangle, Huawei, все native"},
    {"name": "MREC", "size": "300x250", "ar": "6:5", "format": "JPG/PNG/GIF", "max_size": "150 КБ",
     "platforms": "Яндекс, Moloco, Mintegral, Kayzen, все DSP"},
    {"name": "Мобильный баннер", "size": "320x50", "ar": "—", "format": "JPG/PNG/GIF", "max_size": "150 КБ",
     "platforms": "Яндекс, VK, Moloco, Kayzen, все DSP"},
    {"name": "Interstitial portrait", "size": "320x480", "ar": "2:3", "format": "JPG/PNG", "max_size": "500 КБ",
     "platforms": "Яндекс, Moloco, Kayzen, все DSP"},
    {"name": "Playable (HTML5)", "size": "Адаптивный", "ar": "—", "format": "HTML5/ZIP", "max_size": "2-5 МБ",
     "platforms": "Яндекс (3 МБ), VK (2 МБ), Mintegral (5 МБ), AppLovin, Unity, Kayzen"},
    {"name": "Endcard", "size": "1200x628 (L) / 628x1200 (P)", "ar": "1.91:1", "format": "JPG", "max_size": "1 МБ",
     "platforms": "Mintegral (обязательно), Unity, IronSource, Pangle, Moloco"},
]

# ---------------------------------------------------------------------------
# Key technical constants
# ---------------------------------------------------------------------------

TECH_CONSTANTS: dict[str, str] = {
    "Мин. разрешение видео": "720p (1280x720 или 720x1280)",
    "Рек. разрешение видео": "1080p (1920x1080 или 1080x1920)",
    "Видеокодек": "H.264 (обязателен везде)",
    "Аудиокодек": "AAC",
    "fps": "24-30 (мин. 20)",
    "Рек. длина видео UA": "15-30 сек",
    "Рек. длина rewarded": "30 сек",
    "Иконка приложения": "512x512 px, JPG/PNG",
    "Основной native-образ": "1200x628 px (1.91:1)",
    "Текст на изображении": "≤20% площади (Facebook, VK, TikTok)",
    "Безопасная зона Stories/Reels": "120 px сверху + 250 px снизу",
}


def get_platforms_by_region(region: str | None = None) -> list[dict]:
    """Filter platforms by region: 'СНГ', 'Глобальный', or None for all."""
    if not region:
        return PLATFORMS
    return [p for p in PLATFORMS if p["region"] == region]


def get_platform_by_id(platform_id: str) -> dict | None:
    """Get a single platform by its ID."""
    for p in PLATFORMS:
        if p["id"] == platform_id:
            return p
    return None
