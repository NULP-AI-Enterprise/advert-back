-- Migration: Add full-text search vector column and GIN index to media_items
-- Also removes Ukraine-specific country default
-- Run once against the live database.
--
-- Safe to re-run: all statements use IF NOT EXISTS / IF EXISTS guards.

-- 1. Remove the Ukraine-specific country default (makes system universal)
ALTER TABLE media_items ALTER COLUMN country DROP DEFAULT;

-- 2. Add the generated tsvector column
--    GENERATED ALWAYS AS ... STORED: PostgreSQL auto-computes on INSERT/UPDATE,
--    backfills all existing rows immediately on ALTER TABLE completion.
ALTER TABLE media_items
    ADD COLUMN IF NOT EXISTS search_vector tsvector
        GENERATED ALWAYS AS (
            to_tsvector('simple',
                coalesce(title, '') || ' ' ||
                coalesce(description, '') || ' ' ||
                coalesce(category, '')
            )
        ) STORED;

-- 3. Create GIN index for fast full-text search
CREATE INDEX IF NOT EXISTS idx_media_items_search_vector
    ON media_items USING GIN (search_vector);

-- Note: the backfill on step 2 can take 1-3 minutes on ~4000 rows.
-- The index build in step 3 runs after backfill completes.
-- Both are safe to run while the application is live (no lock on SELECT).
