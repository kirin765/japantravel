-- PostgreSQL schema for automated travel place recommendation blog system

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TYPE place_source AS ENUM ('apify', 'google_places', 'manual');
CREATE TYPE article_candidate_status AS ENUM (
    'generated',
    'reviewing',
    'approved',
    'rejected',
    'queued_publish',
    'published',
    'failed'
);
CREATE TYPE published_article_status AS ENUM ('draft', 'scheduled', 'published', 'failed', 'archived');
CREATE TYPE log_level AS ENUM ('debug', 'info', 'warning', 'error', 'critical');

CREATE TABLE place (
    id BIGSERIAL PRIMARY KEY,
    source place_source NOT NULL,
    external_place_id TEXT NOT NULL,
    google_place_id TEXT,
    apify_actor_id TEXT,
    name TEXT NOT NULL,
    description TEXT,
    address TEXT,
    region TEXT,
    country TEXT NOT NULL DEFAULT 'JP',
    latitude NUMERIC(9,6) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude NUMERIC(9,6) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    category TEXT[],
    rating NUMERIC(3,2) CHECK (rating BETWEEN 0 AND 5),
    review_count INTEGER NOT NULL DEFAULT 0 CHECK (review_count >= 0),
    price_level SMALLINT CHECK (price_level BETWEEN 0 AND 5),
    is_open BOOLEAN,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT uq_place_source_external UNIQUE (source, external_place_id)
);

CREATE TABLE article_candidate (
    id BIGSERIAL PRIMARY KEY,
    place_id BIGINT NOT NULL REFERENCES place(id) ON DELETE CASCADE,
    candidate_version INTEGER NOT NULL DEFAULT 1,
    title TEXT,
    outline TEXT,
    generated_prompt TEXT NOT NULL,
    generated_content_markdown TEXT,
    generated_content_json JSONB,
    rank_score NUMERIC(10,4),
    quality_score NUMERIC(5,2),
    review_score NUMERIC(5,2),
    status article_candidate_status NOT NULL DEFAULT 'generated',
    is_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
    error_count INTEGER NOT NULL DEFAULT 0 CHECK (error_count >= 0),
    candidate_hash CHAR(64),
    source_model TEXT NOT NULL DEFAULT 'gpt-4.1',
    source_temperature NUMERIC(3,2),
    scheduled_publish_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT,
    CONSTRAINT uq_articlecandidate_place_version UNIQUE (place_id, candidate_version),
    CONSTRAINT uq_articlecandidate_hash UNIQUE (candidate_hash)
);

CREATE TABLE published_article (
    id BIGSERIAL PRIMARY KEY,
    place_id BIGINT NOT NULL REFERENCES place(id) ON DELETE CASCADE,
    article_candidate_id BIGINT REFERENCES article_candidate(id) ON DELETE SET NULL,
    wp_post_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    slug TEXT NOT NULL,
    content_html TEXT NOT NULL,
    excerpt TEXT,
    status published_article_status NOT NULL DEFAULT 'draft',
    target_publish_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    media_urls TEXT[],
    seo_title TEXT,
    seo_description TEXT,
    categories INTEGER[],
    tags TEXT[],
    content_hash CHAR(64),
    raw_publish_response JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT,
    CONSTRAINT uq_published_wp_post_id UNIQUE (wp_post_id),
    CONSTRAINT uq_published_slug UNIQUE (slug),
    CONSTRAINT uq_published_content_hash UNIQUE (content_hash)
);

CREATE TABLE publish_logs (
    id BIGSERIAL PRIMARY KEY,
    publish_module TEXT NOT NULL,
    place_id BIGINT REFERENCES place(id) ON DELETE SET NULL,
    article_candidate_id BIGINT REFERENCES article_candidate(id) ON DELETE SET NULL,
    published_article_id BIGINT REFERENCES published_article(id) ON DELETE CASCADE,
    attempt_no SMALLINT NOT NULL DEFAULT 1 CHECK (attempt_no >= 1),
    request_id TEXT,
    adapter TEXT NOT NULL,
    event_stage TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('start', 'success', 'retry', 'fail', 'skip')),
    request_payload JSONB,
    response_payload JSONB,
    http_status INTEGER,
    error_message TEXT,
    elapsed_ms INTEGER CHECK (elapsed_ms >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE error_logs (
    id BIGSERIAL PRIMARY KEY,
    level log_level NOT NULL DEFAULT 'error',
    place_id BIGINT REFERENCES place(id) ON DELETE SET NULL,
    article_candidate_id BIGINT REFERENCES article_candidate(id) ON DELETE SET NULL,
    published_article_id BIGINT REFERENCES published_article(id) ON DELETE SET NULL,
    publish_log_id BIGINT REFERENCES publish_logs(id) ON DELETE SET NULL,
    module_name TEXT NOT NULL,
    function_name TEXT,
    error_code TEXT,
    error_message TEXT NOT NULL,
    stack_trace TEXT,
    request_context JSONB,
    request_id TEXT,
    is_resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMPTZ,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_error_target_present CHECK (
        place_id IS NOT NULL OR
        article_candidate_id IS NOT NULL OR
        published_article_id IS NOT NULL OR
        publish_log_id IS NOT NULL OR
        module_name IS NOT NULL
    )
);

CREATE INDEX idx_place_source_ext ON place(source, external_place_id);
CREATE INDEX idx_place_region_active ON place(region, is_active);
CREATE INDEX idx_place_rank_base ON place(is_active, rating DESC, review_count DESC, updated_at DESC) WHERE is_active = TRUE;
CREATE INDEX idx_place_name_trgm ON place USING gin(name gin_trgm_ops);

CREATE INDEX idx_article_candidate_status_rank
  ON article_candidate(status, rank_score DESC, created_at DESC);
CREATE INDEX idx_article_candidate_place_status
  ON article_candidate(place_id, status, created_at DESC);
CREATE INDEX idx_article_candidate_created_at ON article_candidate(created_at DESC);
CREATE INDEX idx_article_candidate_quality
  ON article_candidate(quality_score DESC) WHERE quality_score IS NOT NULL;

CREATE INDEX idx_published_article_status_pubat
  ON published_article(status, published_at DESC);
CREATE INDEX idx_published_article_place_pubat
  ON published_article(place_id, published_at DESC);
CREATE INDEX idx_published_article_created_at
  ON published_article(created_at DESC);
CREATE INDEX idx_published_article_slug_trgm
  ON published_article USING gin(slug gin_trgm_ops);

CREATE INDEX idx_publish_logs_article_created
  ON publish_logs(published_article_id, created_at DESC);
CREATE INDEX idx_publish_logs_candidate_created
  ON publish_logs(article_candidate_id, created_at DESC) WHERE article_candidate_id IS NOT NULL;
CREATE INDEX idx_publish_logs_status_created
  ON publish_logs(status, created_at DESC);

CREATE INDEX idx_error_logs_occurred_at ON error_logs(occurred_at DESC);
CREATE INDEX idx_error_logs_level ON error_logs(level, occurred_at DESC);
CREATE INDEX idx_error_logs_entity ON error_logs(place_id, article_candidate_id, published_article_id);
CREATE INDEX idx_error_logs_unresolved
  ON error_logs(occurred_at DESC) WHERE is_resolved = FALSE;
