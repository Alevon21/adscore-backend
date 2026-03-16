#!/bin/bash
set -e

echo "=== Running database migrations ==="

# Check if alembic_version table exists (= migrations were used before)
# If not, stamp current state so alembic doesn't try to recreate existing tables
python3 -c "
import os, sys, asyncio
import asyncpg

async def check():
    url = os.environ.get('DATABASE_URL', '')
    if not url:
        print('No DATABASE_URL, skipping migration check')
        sys.exit(0)
    # asyncpg needs postgresql:// not postgresql+asyncpg://
    url = url.replace('postgresql+asyncpg://', 'postgresql://')
    try:
        conn = await asyncpg.connect(url)
        result = await conn.fetchval(
            \"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='alembic_version')\"
        )
        await conn.close()
        if result:
            print('alembic_version table exists, will run upgrade')
            sys.exit(0)
        else:
            print('alembic_version table missing, will stamp + upgrade')
            sys.exit(1)
    except Exception as e:
        print(f'DB check failed: {e}')
        sys.exit(0)

asyncio.run(check())
" && {
    # alembic_version exists → just upgrade
    alembic upgrade head
} || {
    # alembic_version missing → stamp to 004 (last migration before features), then upgrade to head
    echo "Stamping existing DB at revision 004..."
    alembic stamp 004
    echo "Upgrading from 004 to head..."
    alembic upgrade head
}

echo "=== Migrations complete ==="
echo "=== Starting uvicorn ==="
exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
