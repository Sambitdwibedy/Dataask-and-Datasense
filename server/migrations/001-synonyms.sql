-- Migration 001: Synonym Management Tables
-- Supports the 3-layer synonym architecture:
--   Layer 1: Global Ontology (global_synonyms) — ships with product
--   Layer 2: Domain Packs (global_synonyms with domain_pack set)
--   Layer 3: Application-level (app_synonyms) — per-application, builder-curated

-- ─── Global Synonyms (Solix Ontology + Domain Packs) ───
-- Not tied to any application. Applied during enrichment as baseline.
CREATE TABLE IF NOT EXISTS global_synonyms (
  id SERIAL PRIMARY KEY,
  term VARCHAR(255) NOT NULL,               -- The synonym/alias (e.g., "revenue")
  canonical_name VARCHAR(255) NOT NULL,     -- What it maps to (e.g., "INVOICE_AMOUNT")
  category VARCHAR(100),                    -- Grouping (e.g., "financial", "date", "identity")
  domain_pack VARCHAR(100),                 -- NULL = global, otherwise domain name (e.g., "healthcare", "manufacturing")
  description TEXT,                         -- Usage guidance
  created_by INTEGER REFERENCES users(id),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(term, canonical_name, domain_pack)
);

CREATE INDEX IF NOT EXISTS idx_global_synonyms_term ON global_synonyms(term);
CREATE INDEX IF NOT EXISTS idx_global_synonyms_canonical ON global_synonyms(canonical_name);
CREATE INDEX IF NOT EXISTS idx_global_synonyms_domain ON global_synonyms(domain_pack);

-- ─── Application-Level Synonyms ───
-- Per-column synonyms for a specific application.
-- Sources: 'solix_global' (auto-applied from global), 'domain_pack', 'ai_generated', 'builder_curated'
CREATE TABLE IF NOT EXISTS app_synonyms (
  id SERIAL PRIMARY KEY,
  app_id INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
  column_id INTEGER REFERENCES app_columns(id) ON DELETE CASCADE,  -- NULL for table-level synonyms
  table_id INTEGER REFERENCES app_tables(id) ON DELETE CASCADE,    -- NULL for app-level synonyms
  term VARCHAR(255) NOT NULL,               -- The synonym/alias
  source VARCHAR(50) NOT NULL DEFAULT 'builder_curated',  -- solix_global, domain_pack, ai_generated, builder_curated
  confidence_score NUMERIC(5,2) DEFAULT 100,
  status VARCHAR(50) DEFAULT 'active',      -- active, rejected, pending_review
  global_synonym_id INTEGER REFERENCES global_synonyms(id) ON DELETE SET NULL,  -- Link to global source
  created_by INTEGER REFERENCES users(id),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(app_id, column_id, term)
);

CREATE INDEX IF NOT EXISTS idx_app_synonyms_app ON app_synonyms(app_id);
CREATE INDEX IF NOT EXISTS idx_app_synonyms_column ON app_synonyms(column_id);
CREATE INDEX IF NOT EXISTS idx_app_synonyms_table ON app_synonyms(table_id);
CREATE INDEX IF NOT EXISTS idx_app_synonyms_term ON app_synonyms(term);
CREATE INDEX IF NOT EXISTS idx_app_synonyms_source ON app_synonyms(source);
CREATE INDEX IF NOT EXISTS idx_app_synonyms_status ON app_synonyms(status);

-- ─── Pipeline Step Status (for Guided Workflow) ───
-- Tracks completion state of each pipeline step per application.
CREATE TABLE IF NOT EXISTS pipeline_steps (
  id SERIAL PRIMARY KEY,
  app_id INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
  step_name VARCHAR(50) NOT NULL,           -- connect, load, profile, discover, enrich, synonyms, curate, index, validate, publish
  step_order INTEGER NOT NULL,              -- 1-10
  status VARCHAR(50) DEFAULT 'not_started', -- not_started, in_progress, completed, needs_attention, skipped
  quality_score NUMERIC(5,2),               -- 0-100, step-specific quality metric
  quality_details JSONB DEFAULT '{}',       -- Detailed quality breakdown
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  completed_by INTEGER REFERENCES users(id),
  notes TEXT,                               -- Builder notes about this step
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(app_id, step_name)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_steps_app ON pipeline_steps(app_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_steps_status ON pipeline_steps(status);
