CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS users (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email         VARCHAR(255) NOT NULL UNIQUE,
    display_name  VARCHAR(255),
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_login_at TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS magic_links (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id    UUID NOT NULL REFERENCES users(id),
    token      VARCHAR(255) NOT NULL UNIQUE,
    used       BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS chat_sessions (
    id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id  VARCHAR(255) NOT NULL,
    context  JSONB DEFAULT '{}',
    title    VARCHAR(255) DEFAULT 'New Chat',
    user_ref UUID REFERENCES users(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS chat_messages (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES chat_sessions(id),
    role       VARCHAR(20) NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
    content    TEXT NOT NULL,
    metadata   JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS media_items (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Core identity
    title       VARCHAR(500) NOT NULL,
    url         VARCHAR(500),
    marketplace_url VARCHAR(500),
    country     VARCHAR(100),

    -- Placement specs (from PRNEW CSV)
    cost_usd               NUMERIC(10,2),
    format_type            VARCHAR(50),
    language               VARCHAR(100),
    lead_time_hours        INTEGER,
    hyperlinks_type        VARCHAR(50),

    -- Traffic & SEO metrics (from PRNEW CSV)
    similarweb_visits       BIGINT,
    ahrefs_dr               SMALLINT,
    moz_da                  SMALLINT,
    semrush_score           SMALLINT,
    organic_traffic_ahrefs  INTEGER,
    organic_traffic_semrush INTEGER,

    -- Content restrictions (JSONB: {crypto: true, gambling: false, ...})
    restrictions JSONB DEFAULT '{}',

    -- LLM-generated fields (populated by enricher)
    description TEXT,
    category    VARCHAR(100),
    tags        TEXT[],
    audience    JSONB DEFAULT '{}',
    metrics     JSONB DEFAULT '{}',

    -- Vector embedding (managed by enricher, @Transient in Hibernate)
    embedding   vector(1536),

    -- Full-text search vector (auto-computed from title + description + category)
    -- tags excluded: array_to_string() is STABLE not IMMUTABLE, forbidden in generated columns
    search_vector tsvector GENERATED ALWAYS AS (
        to_tsvector('simple',
            coalesce(title, '') || ' ' ||
            coalesce(description, '') || ' ' ||
            coalesce(category, '')
        )
    ) STORED,

    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_chat_messages_session_id  ON chat_messages(session_id);
CREATE INDEX IF NOT EXISTS idx_chat_sessions_user_id     ON chat_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_chat_sessions_user_ref    ON chat_sessions(user_ref);
CREATE INDEX IF NOT EXISTS idx_magic_links_token         ON magic_links(token);
CREATE INDEX IF NOT EXISTS idx_users_email               ON users(email);

CREATE INDEX IF NOT EXISTS idx_media_items_category      ON media_items(category);
CREATE INDEX IF NOT EXISTS idx_media_items_country       ON media_items(country);
CREATE INDEX IF NOT EXISTS idx_media_items_format_type   ON media_items(format_type);
CREATE INDEX IF NOT EXISTS idx_media_items_cost_usd      ON media_items(cost_usd);
CREATE INDEX IF NOT EXISTS idx_media_items_visits        ON media_items(similarweb_visits DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_media_items_ahrefs_dr     ON media_items(ahrefs_dr DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_media_items_embedding     ON media_items USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX IF NOT EXISTS idx_media_items_search_vector ON media_items USING GIN (search_vector);
