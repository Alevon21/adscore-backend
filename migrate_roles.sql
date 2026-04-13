-- Migration: Add manager role, starter plan, is_superadmin column
-- Run this against the production/development PostgreSQL database

-- 1. Add 'manager' to UserRole enum
ALTER TYPE userrole ADD VALUE IF NOT EXISTS 'manager' AFTER 'admin';

-- 2. Add 'starter' to TenantPlan enum
ALTER TYPE tenantplan ADD VALUE IF NOT EXISTS 'starter' AFTER 'free';

-- 3. Add is_superadmin column to users
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_superadmin BOOLEAN NOT NULL DEFAULT FALSE;

-- 4. (Optional) Make the first owner a superadmin
-- UPDATE users SET is_superadmin = TRUE WHERE role = 'owner' AND id = (SELECT id FROM users WHERE role = 'owner' ORDER BY created_at ASC LIMIT 1);
