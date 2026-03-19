# Code Review: Database & Business Logic — adscore-backend

**Date:** 2026-03-19
**Scope:** Database schema, ORM models, migrations, tenant isolation, session management, auth, API endpoints

---

## 1. CRITICAL: Tenant Isolation Bypass in In-Memory Sessions

**Files:** `main.py:129-135`, all endpoints using `_get_session()`
**Severity:** Critical (security)

`_get_session()` retrieves session data by `session_id` without any tenant check:

```python
def _get_session(session_id: str) -> Dict[str, Any]:
    with _session_lock:
        if session_id not in SESSION_STORE:
            raise HTTPException(status_code=404, ...)
        return SESSION_STORE[session_id]
```

Any authenticated user who knows (or brute-forces) a session UUID can call `/score`, `/map`, `/abtest`, `/text-parts`, `/extract-words`, `/campaign-analysis`, `/export/{id}` on another tenant's data. The `current_user` dependency is present but never checked against the session owner.

**Fix:** Store `tenant_id` in the session dict at upload time and verify it in `_get_session()`:
```python
def _get_session(session_id: str, tenant_id: uuid.UUID) -> Dict[str, Any]:
    with _session_lock:
        data = SESSION_STORE.get(session_id)
        if not data or data.get("tenant_id") != tenant_id:
            raise HTTPException(status_code=404, ...)
        return data
```

---

## 2. CRITICAL: Auto-Reactivation Defeats User Deactivation

**Files:** `auth.py:108-119`
**Severity:** Critical (security)

When an owner deactivates a user via `DELETE /tenants/{id}/users/{id}`, the user's `is_active` is set to `False`. However, `get_current_user()` automatically reactivates any inactive user on next login:

```python
if inactive_user:
    inactive_user.is_active = True  # bypasses owner's deactivation!
    db.add(inactive_user)
    await db.commit()
    user = inactive_user
```

A deactivated user can simply call any endpoint and get reactivated immediately.

**Fix:** Remove auto-reactivation. If a user is inactive, return 403:
```python
if inactive_user:
    raise HTTPException(status_code=403, detail="Account deactivated. Contact your admin.")
```

---

## 3. CRITICAL: Concurrent SQLAlchemy Session Misuse in `process_banners`

**File:** `main.py:440-545`
**Severity:** Critical (data corruption)

`process_banners` uses `asyncio.gather()` to process banners concurrently, but all coroutines share the same `db: AsyncSession` from dependency injection. SQLAlchemy's `AsyncSession` is **not safe for concurrent use** — interleaved `await` calls corrupt internal session state:

```python
async def _process_one(text_id, url, metrics):
    async with sem:
        bid = await create_banner_from_url(db, tid, uid, url, metrics)  # shared db!
```

**Fix:** Create a separate session per concurrent task, or process sequentially.

---

## 4. HIGH: Dual Session Management — Manual `async_session()` vs DI

**File:** `main.py` (lines 229, 338, 402, 662, 750, 784, 883)
**Severity:** High (architectural / data consistency)

Seven places in `main.py` create ad-hoc sessions via `from database import async_session; async with async_session() as db:` instead of using FastAPI's `Depends(get_db)`. Problems:

1. **Transaction isolation:** The endpoint's DI session and the manual session are separate transactions. A failure in one doesn't roll back the other.
2. **Lifecycle:** DI sessions get proper cleanup via `get_db()`'s generator. Manual sessions bypass any future middleware/hooks.
3. **Context loss:** `tenant_context` set in `get_current_user` applies to the DI session's context but may not propagate to manually created sessions.

**Fix:** Inject `db: AsyncSession = Depends(get_db)` in all endpoints that need DB access. Use a single session per request.

---

## 5. HIGH: Soft Delete Reuses `failed` Status

**File:** `main.py:888`
**Severity:** High (data integrity)

```python
.values(status=SessionStatus.failed)  # reuse 'failed' as deleted indicator
```

Genuinely failed scoring sessions and deliberately deleted sessions are indistinguishable. The `list_sessions` endpoint hides `failed` sessions (`where status != failed`), meaning real failures are also hidden from users.

**Fix:** Add a `deleted` status to `SessionStatus` enum (requires a new migration).

---

## 6. HIGH: Synchronous HTTP Call Blocks Event Loop

**File:** `auth.py:33`
**Severity:** High (performance)

```python
resp = httpx.get(JWKS_URL, timeout=10)  # synchronous!
```

`_fetch_jwks()` uses synchronous `httpx.get()` inside an async application. This blocks the entire event loop for up to 10 seconds on every cache miss, stalling all concurrent requests.

**Fix:** Use `httpx.AsyncClient` and make `_fetch_jwks` async, or use `asyncio.to_thread()`.

---

## 7. MEDIUM: `threading.Timer` in Async App

**File:** `main.py:101-127`
**Severity:** Medium (architecture / scaling)

Using `threading.Timer` for session TTL cleanup in an async application:
- Multiple uvicorn workers = multiple `SESSION_STORE` dicts = sessions split across workers.
- Timers don't survive process restarts.
- Thread-based cleanup can interfere with the async event loop.

**Fix:** For production, move sessions to Redis with built-in TTL. For single-worker mode, use `asyncio`-based timers.

---

## 8. MEDIUM: `log_audit` Commits Independently

**File:** `users.py:109-121`
**Severity:** Medium (data consistency)

```python
async def log_audit(db, ...):
    db.add(log)
    await db.commit()  # commits everything in the session!
```

`log_audit` calls `commit()` on the shared session, which commits ALL pending changes, not just the audit log. If called mid-transaction, it prematurely commits partial state. In `register()`, this is called *after* the main commit, so it works — but if the audit commit fails, the response still succeeds, hiding the error.

**Fix:** Don't commit inside `log_audit`. Let the caller manage the transaction:
```python
async def log_audit(db, ...):
    db.add(log)
    # caller calls db.commit()
```

---

## 9. MEDIUM: `list_sessions` Count Ignores Status Filter

**File:** `sessions.py:46-52`
**Severity:** Medium (correctness)

The count query doesn't apply the optional `status` filter:
```python
count_q = (
    select(func.count())
    .select_from(ScoringSession)
    .where(ScoringSession.tenant_id == tid)
    .where(ScoringSession.status != SessionStatus.failed)
    # missing: status filter that's applied to the main query
)
```

When a user filters by status, `total` is wrong (shows total unfiltered count).

---

## 10. MEDIUM: No `ON DELETE CASCADE` on Foreign Keys

**Files:** `db_models.py`, all migrations
**Severity:** Medium (operational)

None of the FK constraints specify `ondelete="CASCADE"` or `ondelete="SET NULL"`. Deleting a tenant requires manually deleting all related rows in dependency order, or the delete fails with FK violation.

Affected chains:
- `tenants` ← `users` ← `audit_logs`
- `tenants` ← `projects` ← `datasets` ← `stored_files`/`scoring_sessions`
- `scoring_sessions` ← `scoring_results`

---

## 11. MEDIUM: No Unique Constraint on `(tenant_id, email)` for Invites

**File:** `db_models.py:252-267`
**Severity:** Medium (data integrity)

The duplicate invite check in `users.py:421-428` is done in application code, but there's no database-level unique constraint. Concurrent requests can create duplicate invites for the same email+tenant.

**Fix:** Add `UniqueConstraint("tenant_id", "email")` to `PendingInvite.__table_args__`.

---

## 12. MEDIUM: `tenant_query` Helper Is Dead Code

**File:** `database.py:48-58`
**Severity:** Low (dead code)

`tenant_query()` is defined but never imported or used anywhere in the codebase. Either remove it or actually use it for consistent tenant-scoped queries.

---

## 13. MEDIUM: `get_me` Writes on Every Call

**File:** `users.py:219-223`
**Severity:** Medium (performance)

```python
user.last_login = datetime.now(timezone.utc)
db.add(user)
await db.commit()
```

Every call to `GET /auth/me` triggers a DB write. If the frontend polls this endpoint, it creates unnecessary write pressure. Consider throttling updates (e.g., only update if > 5 minutes since last login).

---

## 14. LOW: `User.email` Not Unique Within Tenant

**File:** `db_models.py:82`
**Severity:** Low (data integrity)

`email` has no unique constraint at all (only `supabase_uid` is unique). Depending on business rules, the same email could exist as active users in multiple tenants, which is fine — but within a single tenant there's no guard against duplicate emails (e.g., if Supabase creates duplicate accounts).

---

## 15. LOW: Migration 005 Uses String Interpolation in SQL

**File:** `alembic/versions/005_user_features.py:27-31`
**Severity:** Low (the values are hardcoded constants, not user input)

```python
op.execute(f"UPDATE users SET features = '{ALL_FEATURES}' WHERE role IN ('owner', 'admin')")
```

Not exploitable here since values are constants, but bad practice. Use `sa.text()` with bind params.

---

## 16. LOW: `ScoringResult` Rows Accumulate Without Cleanup

**File:** `main.py:426-432`
**Severity:** Low (storage growth)

Each call to `/score` creates a new `ScoringResultDB` row, even for the same session. No mechanism to:
- Delete old results when re-scoring
- Set a retention policy
- Clean up results for deleted sessions

---

## 17. LOW: `selectin` Lazy Loading on All Relationships

**File:** `db_models.py` (multiple)
**Severity:** Low (performance)

All relationships use `lazy="selectin"`, which eagerly loads related objects. For `Tenant.users`, this loads ALL tenant users whenever a tenant is accessed — potentially hundreds of users for large tenants.

Consider `lazy="raise"` (explicit loading) for `Tenant.users` and load users only when needed.

---

## Summary Table

| # | Severity | Issue | File |
|---|----------|-------|------|
| 1 | CRITICAL | Tenant isolation bypass in in-memory sessions | `main.py` |
| 2 | CRITICAL | Auto-reactivation defeats user deactivation | `auth.py` |
| 3 | CRITICAL | Concurrent AsyncSession misuse in `process_banners` | `main.py` |
| 4 | HIGH | Dual session management (DI vs manual) | `main.py` |
| 5 | HIGH | Soft delete reuses `failed` status | `main.py` |
| 6 | HIGH | Synchronous HTTP blocks event loop | `auth.py` |
| 7 | MEDIUM | `threading.Timer` in async app | `main.py` |
| 8 | MEDIUM | `log_audit` commits independently | `users.py` |
| 9 | MEDIUM | Count query ignores status filter | `sessions.py` |
| 10 | MEDIUM | No `ON DELETE CASCADE` on FKs | `db_models.py` |
| 11 | MEDIUM | No unique constraint on pending invites | `db_models.py` |
| 12 | MEDIUM | `tenant_query` is dead code | `database.py` |
| 13 | MEDIUM | `get_me` writes on every call | `users.py` |
| 14 | LOW | Email not unique within tenant | `db_models.py` |
| 15 | LOW | String interpolation in migration SQL | `005_user_features.py` |
| 16 | LOW | ScoringResult rows never cleaned up | `main.py` |
| 17 | LOW | `selectin` eager loading everywhere | `db_models.py` |

---

## Positive Observations

- Clean Alembic migration chain with correct revision links
- Proper use of `timezone.utc` for all timestamps
- Good index coverage for common query patterns (tenant+created_at)
- UUID primary keys are appropriate for multi-tenant
- Storage quota check before upload is well-implemented
- Role-based access control with proper hierarchy (owner > admin > analyst > viewer)
- Feature flags per user with admin override — flexible design
- Audit logging is comprehensive
- Idempotent registration with pending invite flow is well thought out
