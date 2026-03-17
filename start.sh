#!/bin/bash

echo "=== Starting deployment ==="

# Resolve DATABASE_URL — Railway may use different variable names
DB_URL="${DATABASE_URL:-${DATABASE_PRIVATE_URL:-${DATABASE_PUBLIC_URL:-}}}"

if [ -z "$DB_URL" ]; then
    echo "WARNING: No DATABASE_URL found, skipping migrations"
else
    echo "DATABASE_URL found, running migrations..."
    export DATABASE_URL="$DB_URL"

    # Check if alembic_version table exists
    NEEDS_STAMP=$(python3 -c "
import os, sys, asyncio
import asyncpg

async def check():
    url = os.environ.get('DATABASE_URL', '')
    # asyncpg needs plain postgresql://
    for prefix in ['postgresql+asyncpg://', 'postgres://']:
        if url.startswith(prefix):
            url = 'postgresql://' + url[len(prefix):]
            break
    try:
        conn = await asyncpg.connect(url)
        result = await conn.fetchval(
            \"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='alembic_version')\"
        )
        await conn.close()
        print('no' if result else 'yes')
    except Exception as e:
        print(f'error: {e}', file=sys.stderr)
        print('no')

asyncio.run(check())
" 2>/dev/null)

    echo "Needs stamp: $NEEDS_STAMP"

    if [ "$NEEDS_STAMP" = "yes" ]; then
        echo "Stamping existing DB at revision 005..."
        alembic stamp 005 || echo "WARNING: Stamp failed, continuing..."
    fi

    echo "Running alembic upgrade head..."
    alembic upgrade head || echo "WARNING: Migration failed, continuing with app startup..."

    echo "=== Migrations complete ==="
fi

echo "=== Starting uvicorn ==="
exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
