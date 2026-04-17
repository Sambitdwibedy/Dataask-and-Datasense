-- Migration 001: Synonym Management Tables (MySQL)
-- Supports the 3-layer synonym architecture:
--   Layer 1: Global Ontology (global_synonyms) — ships with product
--   Layer 2: Domain Packs (global_synonyms with domain_pack set)
--   Layer 3: Application-level (app_synonyms) — per-application, builder-curated

-- Global Synonyms (Solix Ontology + Domain Packs)
CREATE TABLE IF NOT EXISTS global_synonyms (
  id INT AUTO_INCREMENT PRIMARY KEY,
  term VARCHAR(255) NOT NULL,
  canonical_name VARCHAR(255) NOT NULL,
  category VARCHAR(100),
  domain_pack VARCHAR(100),
  description TEXT,
  created_by INTEGER REFERENCES users(id),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE KEY uq_global_synonyms (term, canonical_name, domain_pack)
);

CREATE INDEX IF NOT EXISTS idx_global_synonyms_term ON global_synonyms(term);
CREATE INDEX IF NOT EXISTS idx_global_synonyms_canonical ON global_synonyms(canonical_name);
CREATE INDEX IF NOT EXISTS idx_global_synonyms_domain ON global_synonyms(domain_pack);

-- Application-Level Synonyms
CREATE TABLE IF NOT EXISTS app_synonyms (
  id INT AUTO_INCREMENT PRIMARY KEY,
  app_id INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
  column_id INTEGER REFERENCES app_columns(id) ON DELETE CASCADE,
  table_id INTEGER REFERENCES app_tables(id) ON DELETE CASCADE,
  term VARCHAR(255) NOT NULL,
  source VARCHAR(50) NOT NULL DEFAULT 'builder_curated',
  confidence_score DECIMAL(5,2) DEFAULT 100,
  status VARCHAR(50) DEFAULT 'active',
  global_synonym_id INTEGER REFERENCES global_synonyms(id) ON DELETE SET NULL,
  created_by INTEGER REFERENCES users(id),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE KEY uq_app_synonym (app_id, column_id, term)
);

CREATE INDEX IF NOT EXISTS idx_app_synonyms_app ON app_synonyms(app_id);
CREATE INDEX IF NOT EXISTS idx_app_synonyms_column ON app_synonyms(column_id);
CREATE INDEX IF NOT EXISTS idx_app_synonyms_table ON app_synonyms(table_id);
CREATE INDEX IF NOT EXISTS idx_app_synonyms_term ON app_synonyms(term);
CREATE INDEX IF NOT EXISTS idx_app_synonyms_source ON app_synonyms(source);
CREATE INDEX IF NOT EXISTS idx_app_synonyms_status ON app_synonyms(status);

-- Pipeline Step Status (for Guided Workflow)
CREATE TABLE IF NOT EXISTS pipeline_steps (
  id INT AUTO_INCREMENT PRIMARY KEY,
  app_id INTEGER NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
  step_name VARCHAR(50) NOT NULL,
  step_order INTEGER NOT NULL,
  status VARCHAR(50) DEFAULT 'not_started',
  quality_score DECIMAL(5,2),
  quality_details JSON,
  started_at TIMESTAMP NULL,
  completed_at TIMESTAMP NULL,
  completed_by INTEGER REFERENCES users(id),
  notes TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE KEY uq_pipeline_step (app_id, step_name)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_steps_app ON pipeline_steps(app_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_steps_status ON pipeline_steps(status);
