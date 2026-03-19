# Security Audit: adscore-backend

**Date:** 2026-03-19
**Scope:** Full codebase security review (OWASP Top 10, infrastructure, auth, data protection)

---

## CRITICAL Vulnerabilities

### S1. SSRF (Server-Side Request Forgery) via Banner URL Upload

**Files:** `adscore.py:115-135`, `adscore.py:363-402`, `main.py:440-545`
**OWASP:** A10:2021 — Server-Side Request Forgery
**CVSS estimate:** 8.5 (High)

The `/adscore/upload-url` endpoint and `create_banner_from_url()` accept arbitrary URLs from users and make server-side HTTP requests without any validation:

```python
async with httpx_lib.AsyncClient(follow_redirects=True, timeout=30.0) as client:
    resp = await client.get(image_url)  # user-controlled URL!
```

An attacker can:
- **Scan internal network:** `http://169.254.169.254/latest/meta-data/` (AWS metadata), `http://10.0.0.1:5432/` (internal PostgreSQL)
- **Access cloud metadata endpoints:** Steal IAM credentials, service account tokens
- **Port scan internal hosts:** Determine open ports on internal infrastructure
- **Read internal services:** Access Redis, Elasticsearch, admin panels on internal IPs
- `follow_redirects=True` amplifies the attack — attacker can redirect to internal URLs

**The same issue exists in `main.py:440-545`** via the `/process-banners` endpoint which processes `banner_url` column from CSV files, making the attack scalable (hundreds of SSRF probes per CSV upload).

**Fix:**
```python
from urllib.parse import urlparse
import ipaddress

BLOCKED_HOSTS = {'localhost', '127.0.0.1', '0.0.0.0', '169.254.169.254', 'metadata.google.internal'}

def validate_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https'):
        return False
    hostname = parsed.hostname
    if not hostname or hostname in BLOCKED_HOSTS:
        return False
    try:
        ip = ipaddress.ip_address(hostname)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            return False
    except ValueError:
        pass  # hostname is a domain, not IP
    return True
```

---

### S2. Tenant Isolation Bypass — Cross-Tenant Data Access

**File:** `main.py:129-135`
**OWASP:** A01:2021 — Broken Access Control
**CVSS estimate:** 9.0 (Critical)

Already documented in database review. All in-memory session endpoints (`/score`, `/map`, `/abtest`, `/text-parts`, `/extract-words`, `/campaign-analysis`, `/export/{id}`) lack tenant verification. Any authenticated user can access another tenant's uploaded data.

**Additionally:** The `/process-banners` endpoint at `main.py:451` accesses `SESSION_STORE.get(session_id)` directly without even the `_session_lock`, creating a TOCTOU race condition on top of the missing tenant check.

---

### S3. JWT Token Expiry Not Verified

**File:** `auth.py:88-93`
**OWASP:** A07:2021 — Identification and Authentication Failures
**CVSS estimate:** 7.5 (High)

JWT validation disables audience verification and does not check token expiration:

```python
payload = jwt.decode(
    token,
    signing_key,
    algorithms=[alg],
    options={"verify_aud": False},  # audience check disabled
)
```

`python-jose` by default verifies `exp` claim, but if the token doesn't have an `exp` claim, it passes silently. There's no explicit check for:
- Token expiration (`exp`)
- Token issuer (`iss` — should be Supabase URL)
- Token not-before (`nbf`)

An expired or stolen token remains valid forever. Combined with the auto-reactivation bug (S4), a deactivated user with an old token has permanent access.

**Fix:**
```python
payload = jwt.decode(
    token,
    signing_key,
    algorithms=[alg],
    options={
        "verify_aud": False,
        "verify_exp": True,    # explicitly enforce
        "require_exp": True,   # reject tokens without exp
        "require_sub": True,   # reject tokens without sub
    },
    issuer=f"{SUPABASE_URL}/auth/v1",  # verify issuer
)
```

---

### S4. Auto-Reactivation Bypasses Account Deactivation

**File:** `auth.py:108-119`
**OWASP:** A01:2021 — Broken Access Control
**CVSS estimate:** 8.0 (High)

Already documented in database review. A deactivated user automatically regains access on next API call.

---

## HIGH Vulnerabilities

### S5. Image Processing Denial of Service (Decompression Bomb)

**Files:** `adscore.py:104-112`, `adscore_tagger.py:62-91`
**OWASP:** A05:2021 — Security Misconfiguration
**CVSS estimate:** 6.5 (Medium-High)

`_get_image_dimensions()` and `_resize_image()` use Pillow's `Image.open()` without decompression bomb protection:

```python
with Image.open(BytesIO(content)) as img:  # can decompress to GBs in memory
    return img.size
```

A crafted PNG can be ~1KB compressed but decompress to multiple GB in memory (a "zip bomb" for images). The 10MB file size check (`MAX_IMAGE_SIZE`) doesn't prevent this because the compressed file is small.

**Fix:**
```python
from PIL import Image
Image.MAX_IMAGE_PIXELS = 25_000_000  # ~5000x5000 max
```

---

### S6. Information Disclosure in Error Messages

**Files:** Multiple
**OWASP:** A04:2021 — Insecure Design

Several endpoints expose internal error details to clients:

1. `main.py:188` — CSV parse errors leak internal paths/structures:
   ```python
   raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}")
   ```

2. `adscore.py:1029` — Anthropic API errors leak to client:
   ```python
   raise HTTPException(status_code=500, detail=f"Ошибка генерации объяснения: {e}")
   ```

3. `adscore.py:312` — CSV parse errors leak pandas internals:
   ```python
   raise HTTPException(status_code=400, detail=f"Failed to parse CSV: {e}")
   ```

4. `adscore_tagger.py:146` — AI response parsing errors leak raw AI output:
   ```python
   raise ValueError(f"AI returned invalid JSON: {e}")
   ```

5. `auth.py:95` — JWT errors leak token validation details:
   ```python
   raise HTTPException(status_code=401, detail=f"Invalid token: {e}")
   ```

**Fix:** Return generic error messages; log details server-side.

---

### S7. CORS Allows Wildcard Methods and Headers

**File:** `main.py:74-88`
**OWASP:** A05:2021 — Security Misconfiguration

```python
app.add_middleware(
    CORSMiddleware,
    allow_methods=["*"],   # allows all HTTP methods including PATCH, DELETE
    allow_headers=["*"],   # allows all headers including custom ones
)
```

Combined with `allow_credentials=True`, this is overly permissive. Though `allow_origins` is restricted to specific domains, `os.getenv("FRONTEND_URL", "")` defaults to empty string — if the env var is empty, an empty origin is allowed which some browsers may treat unexpectedly.

**Fix:** Explicitly list allowed methods (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `OPTIONS`) and headers (`Authorization`, `Content-Type`). Remove the empty string default.

---

### S8. No Rate Limiting on Any Endpoint

**OWASP:** A04:2021 — Insecure Design

No rate limiting exists anywhere:
- `/auth/register` — unlimited account creation
- `/upload` — unlimited file uploads consuming memory and storage
- `/score` — CPU-intensive scoring with no throttle
- `/adscore/tag-all` — unlimited Anthropic API calls ($$$)
- `/adscore/upload-url` — unlimited SSRF probes
- `/adscore/banner/{id}/explain` — unlimited Anthropic API calls ($$$)

**Fix:** Add `slowapi` or similar rate limiter:
```python
from slowapi import Limiter
limiter = Limiter(key_func=get_remote_address)
```

---

### S9. Synchronous JWKS Fetch Blocks Event Loop

**File:** `auth.py:27-45`
**OWASP:** A05:2021 — Security Misconfiguration

Already documented in database review. `httpx.get()` (synchronous) blocks all concurrent requests for up to 10 seconds. An attacker can trigger cache invalidation (by using a token with an unknown `kid`) to force repeated blocking fetches, creating a DoS condition.

---

## MEDIUM Vulnerabilities

### S10. Storage Path Injection in Supabase Storage Key

**Files:** `adscore.py:158`, `adscore.py:229`, `main.py:249`

The `original_name` from user-provided filenames is used directly in storage paths:

```python
storage_key = f"{tenant_id}/banners/{banner_id}/{original_name}"
```

While Supabase Storage may handle this safely, the filename could contain `../` or URL-encoded traversal sequences that might bypass controls depending on the storage backend. In `create_banner_from_url`, the filename comes from the URL path:

```python
original_name = unquote(Path(url_path).name) or "image_from_url"
```

`unquote()` + user-controlled URL could produce filenames with special characters.

**Fix:** Sanitize filenames — strip path separators, limit to alphanumeric + dots + dashes:
```python
import re
safe_name = re.sub(r'[^\w.\-]', '_', original_name)[:200]
```

---

### S11. Unbounded Banner Loading — Memory Exhaustion

**File:** `adscore.py:476-477`, `adscore.py:781-787`

```python
result = await db.execute(q)
all_banners = list(result.scalars().all())  # loads ALL into memory
```

`list_banners` loads ALL matching banners into memory before applying JSONB filters and pagination. A tenant with 100K+ banners would consume significant RAM. Same issue in `_load_tenant_banners_data()` which loads ALL tenant banners for analytics.

**Fix:** Push filters to SQL where possible; add hard limit (e.g., max 10000 banners per query).

---

### S12. CSV Parsing Without Row Limit — Pandas DoS

**File:** `main.py:182-185`, `adscore.py:310`

`pd.read_csv()` and `pd.read_excel()` parse the entire file into memory. A 50MB CSV with small rows could contain millions of rows, consuming significant memory during scoring.

**Fix:** Add `nrows` parameter limit:
```python
df = pd.read_csv(buf, nrows=500_000)
```

---

### S13. `tag_banner` is Synchronous — Blocks Event Loop

**File:** `adscore_tagger.py:93-149`

`tag_banner()` is a synchronous function that calls the synchronous Anthropic SDK. When called from async endpoints (`tag_banner_endpoint`, `tag_all_banners`), it blocks the event loop for the duration of the API call (potentially seconds).

The `tag_all_banners` endpoint at `adscore.py:614-652` calls `tag_banner` **in a sequential loop** for ALL pending banners — a tenant with 100 pending banners would block the server for minutes.

**Fix:** Use async Anthropic client (`anthropic.AsyncAnthropic`) or `asyncio.to_thread(tag_banner, ...)`.

---

### S14. No Content-Type Validation on Upload

**File:** `main.py:146-188`

File type is validated only by extension:
```python
ext = filename.rsplit(".", 1)[-1].lower()
if ext not in ("csv", "xlsx", "xls"):
```

An attacker can upload any content with a `.csv` extension. While `pd.read_csv()` would fail on non-CSV content (limited risk), a malicious CSV could contain formulas (CSV injection): `=cmd|' /C calc'!A1`.

For images in `adscore.py:209-211`, only extension is checked — no magic byte validation.

**Fix:** Validate magic bytes for images:
```python
import imghdr
img_type = imghdr.what(None, h=content[:32])
if img_type not in ('png', 'jpeg', 'gif', 'webp'):
    raise HTTPException(...)
```

---

### S15. Hardcoded Database Credentials in Default Config

**File:** `database.py:8-11`, `docker-compose.yml:4-6`

```python
"postgresql+asyncpg://adscore:adscore_local@localhost:5432/adscore"
```

Default credentials `adscore:adscore_local` are hardcoded. While intended for local development, if `DATABASE_URL` env var is missing in production, the app silently uses these defaults and may connect to an unintended database.

---

### S16. `process_banners` Shared AsyncSession in Concurrent Tasks

**File:** `main.py:440-545`

Already documented. `asyncio.gather()` shares one `AsyncSession` across concurrent coroutines — this can corrupt session state and lead to data inconsistency.

---

### S17. Unprotected Endpoints

**File:** `main.py:141-143`, `main.py:799-866`

- `GET /health` — exposes session count (`sessions: len(SESSION_STORE)`), which leaks information about server load
- `GET /template` — no authentication required, minor but inconsistent with auth requirements elsewhere
- `GET /adscore/csv-template` — no authentication required

While templates are public data, the health endpoint's session count can be used for reconnaissance.

---

### S18. `python-jose` Library Has Known CVEs

**File:** `requirements.txt:16`

`python-jose` has had multiple CVEs and is no longer actively maintained. The last release was in 2021. Known issues include:
- CVE-2024-33663 — ECDSA signature validation bypass
- General algorithm confusion attacks

**Fix:** Migrate to `PyJWT` or `joserfc` which are actively maintained:
```
PyJWT[crypto]>=2.8.0
```

---

## LOW Vulnerabilities

### S19. Logging Potentially Sensitive Information

**Files:** Multiple

- `auth.py:122` — logs `supabase_uid` on auth failure
- `adscore.py:134` — logs full banner download URLs (may contain tokens/API keys in query params)
- `adscore.py:168` — logs Supabase Storage error responses (may contain auth tokens)
- `main.py:279` — logs full exception details including DB connection info

---

### S20. Docker Runs as Root

**File:** `Dockerfile`

The Dockerfile doesn't specify a non-root user:
```dockerfile
FROM python:3.11-slim
WORKDIR /app
```

Container processes run as root by default, increasing blast radius of any container escape.

**Fix:**
```dockerfile
RUN useradd -m appuser
USER appuser
```

---

### S21. Missing Security Headers

**File:** `main.py`

No security headers are set:
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `Strict-Transport-Security` (HSTS)
- `X-XSS-Protection: 0` (modern approach)
- `Content-Security-Policy`
- `Referrer-Policy`

---

### S22. `start.sh` Continues on Migration Failure

**File:** `start.sh:48`

```bash
alembic upgrade head || echo "WARNING: Migration failed, continuing with app startup..."
```

If migrations fail, the app starts with a potentially inconsistent schema. This could lead to runtime errors or data corruption.

---

### S23. No Input Length Validation on API Bodies

**Files:** `users.py`, `models.py`

Pydantic models don't enforce string length limits:
- `RegisterRequest.company_name` — no max length
- `RegisterRequest.email` — no format validation
- `InviteByEmailRequest.email` — no format validation

An attacker could send extremely long strings that get stored in the database (varchar limits help, but processing overhead remains).

---

## Summary Table

| # | Severity | Category | Issue | File |
|---|----------|----------|-------|------|
| S1 | CRITICAL | SSRF | Unrestricted server-side URL fetch | `adscore.py` |
| S2 | CRITICAL | AuthZ | Cross-tenant data access via in-memory sessions | `main.py` |
| S3 | CRITICAL | AuthN | JWT expiry/issuer not verified | `auth.py` |
| S4 | CRITICAL | AuthZ | Auto-reactivation bypasses deactivation | `auth.py` |
| S5 | HIGH | DoS | Image decompression bomb | `adscore.py`, `adscore_tagger.py` |
| S6 | HIGH | InfoLeak | Internal errors exposed to clients | Multiple |
| S7 | HIGH | Config | CORS overly permissive | `main.py` |
| S8 | HIGH | DoS/Abuse | No rate limiting anywhere | All endpoints |
| S9 | HIGH | DoS | Synchronous JWKS fetch blocks event loop | `auth.py` |
| S10 | MEDIUM | PathTraversal | Unsanitized filenames in storage paths | `adscore.py` |
| S11 | MEDIUM | DoS | Unbounded query loads all banners | `adscore.py` |
| S12 | MEDIUM | DoS | Unbounded CSV row parsing | `main.py` |
| S13 | MEDIUM | DoS | Synchronous `tag_banner` blocks event loop | `adscore_tagger.py` |
| S14 | MEDIUM | Validation | Extension-only file type validation | `main.py`, `adscore.py` |
| S15 | MEDIUM | Config | Hardcoded default DB credentials | `database.py` |
| S16 | MEDIUM | Concurrency | Shared AsyncSession in `process_banners` | `main.py` |
| S17 | LOW | InfoLeak | Health endpoint leaks session count | `main.py` |
| S18 | MEDIUM | Dependency | `python-jose` has known CVEs | `requirements.txt` |
| S19 | LOW | Logging | PII/URLs in logs | Multiple |
| S20 | LOW | Container | Docker runs as root | `Dockerfile` |
| S21 | LOW | Headers | Missing security response headers | `main.py` |
| S22 | LOW | Deploy | App starts on migration failure | `start.sh` |
| S23 | LOW | Validation | No input length limits on API bodies | `users.py` |

---

## Priority Remediation Order

1. **Immediate (before next deploy):**
   - S1 — SSRF URL validation (blocks cloud metadata theft)
   - S2 — Add tenant_id check to `_get_session()`
   - S3 — Enforce JWT expiry and issuer verification
   - S4 — Remove auto-reactivation

2. **This sprint:**
   - S5 — Set `Image.MAX_IMAGE_PIXELS`
   - S8 — Add rate limiting (`slowapi`)
   - S18 — Replace `python-jose` with `PyJWT`
   - S9 — Make JWKS fetch async

3. **Next sprint:**
   - S6 — Sanitize error messages
   - S7 — Tighten CORS config
   - S10-S14 — Input validation improvements
   - S20-S21 — Hardening (Docker user, security headers)
