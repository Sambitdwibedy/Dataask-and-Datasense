-- BOKG Builder Application Schema (MySQL)
-- This schema supports the multi-application, multi-stage data governance workflow

-- Users table
CREATE TABLE IF NOT EXISTS users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  name VARCHAR(255) NOT NULL,
  role VARCHAR(50) DEFAULT 'operator',
  created_at TIMESTAMP DEFAULT NOW()
);

-- Applications
CREATE TABLE IF NOT EXISTS applications (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) UNIQUE NOT NULL,
  type VARCHAR(100) NOT NULL,
  description TEXT,
  status VARCHAR(50) DEFAULT 'draft',
  config JSON,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Modules (grouping of tables within an application)
CREATE TABLE IF NOT EXISTS app_modules (
  id INT AUTO_INCREMENT PRIMARY KEY,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  code VARCHAR(50) NOT NULL,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  UNIQUE KEY uq_app_module (app_id, code)
);

-- Tables (entities)
CREATE TABLE IF NOT EXISTS app_tables (
  id INT AUTO_INCREMENT PRIMARY KEY,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  module_id INTEGER REFERENCES app_modules(id) ON DELETE CASCADE,
  table_name VARCHAR(255) NOT NULL,
  entity_name VARCHAR(255),
  row_count INTEGER DEFAULT 0,
  description TEXT,
  UNIQUE KEY uq_app_table (app_id, table_name)
);

-- Columns (with enrichment data)
CREATE TABLE IF NOT EXISTS app_columns (
  id INT AUTO_INCREMENT PRIMARY KEY,
  table_id INTEGER REFERENCES app_tables(id) ON DELETE CASCADE,
  column_name VARCHAR(255) NOT NULL,
  data_type VARCHAR(100),
  is_pk TINYINT(1) DEFAULT 0,
  is_fk TINYINT(1) DEFAULT 0,
  fk_reference VARCHAR(255),
  business_name VARCHAR(255),
  description TEXT,
  value_mapping TEXT,
  column_role VARCHAR(30),
  enrichment_status VARCHAR(50) DEFAULT 'draft',
  confidence_score DECIMAL(5,2) DEFAULT 0,
  enriched_by VARCHAR(50),
  enriched_at TIMESTAMP NULL,
  UNIQUE KEY uq_col (table_id, column_name)
);

-- Relationships (foreign keys and inferred relationships)
CREATE TABLE IF NOT EXISTS app_relationships (
  id INT AUTO_INCREMENT PRIMARY KEY,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  from_table_id INTEGER REFERENCES app_tables(id) ON DELETE CASCADE,
  from_column VARCHAR(255),
  to_table_id INTEGER REFERENCES app_tables(id) ON DELETE CASCADE,
  to_column VARCHAR(255),
  rel_type VARCHAR(50) DEFAULT 'fk',
  cardinality VARCHAR(20),
  UNIQUE KEY uq_rel (from_table_id, from_column, to_table_id, to_column)
);

-- Pipeline runs (tracks execution of data processing stages)
CREATE TABLE IF NOT EXISTS pipeline_runs (
  id INT AUTO_INCREMENT PRIMARY KEY,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  triggered_by INTEGER REFERENCES users(id),
  status VARCHAR(50) DEFAULT 'pending',
  started_at TIMESTAMP NULL,
  completed_at TIMESTAMP NULL,
  stages JSON
);

-- Query patterns (QPD - discovered from query logs)
CREATE TABLE IF NOT EXISTS query_patterns (
  id INT AUTO_INCREMENT PRIMARY KEY,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  pattern_name VARCHAR(255),
  nl_template TEXT,
  sql_template TEXT,
  tables_used TEXT,
  status VARCHAR(50) DEFAULT 'discovered',
  usage_count INTEGER DEFAULT 0,
  confidence DECIMAL(5,2) DEFAULT 0,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE KEY uq_qp (app_id, pattern_name)
);

-- Test queries (user-generated test queries for evaluation)
CREATE TABLE IF NOT EXISTS test_queries (
  id INT AUTO_INCREMENT PRIMARY KEY,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  user_id INTEGER REFERENCES users(id),
  nl_query TEXT NOT NULL,
  generated_sql TEXT,
  execution_result JSON,
  feedback VARCHAR(20),
  confidence DECIMAL(5,2),
  created_at TIMESTAMP DEFAULT NOW()
);

-- Curation log (audit trail for all enrichment and validation activities)
CREATE TABLE IF NOT EXISTS curation_log (
  id INT AUTO_INCREMENT PRIMARY KEY,
  column_id INTEGER REFERENCES app_columns(id) ON DELETE CASCADE,
  user_id INTEGER REFERENCES users(id),
  action VARCHAR(50),
  old_value JSON,
  new_value JSON,
  notes TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Token consumption tracking
CREATE TABLE IF NOT EXISTS token_usage (
  id INT AUTO_INCREMENT PRIMARY KEY,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  pipeline_run_id INTEGER REFERENCES pipeline_runs(id) ON DELETE CASCADE,
  stage VARCHAR(50) NOT NULL,
  table_name VARCHAR(255),
  input_tokens INTEGER DEFAULT 0,
  output_tokens INTEGER DEFAULT 0,
  total_tokens INTEGER DEFAULT 0,
  model VARCHAR(100),
  cost_estimate DECIMAL(10,6) DEFAULT 0,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_app_tables_app ON app_tables(app_id);
CREATE INDEX IF NOT EXISTS idx_app_tables_module ON app_tables(module_id);
CREATE INDEX IF NOT EXISTS idx_app_columns_table ON app_columns(table_id);
CREATE INDEX IF NOT EXISTS idx_app_columns_status ON app_columns(enrichment_status);
CREATE INDEX IF NOT EXISTS idx_app_columns_confidence ON app_columns(confidence_score);
CREATE INDEX IF NOT EXISTS idx_app_relationships_app ON app_relationships(app_id);
CREATE INDEX IF NOT EXISTS idx_app_relationships_from ON app_relationships(from_table_id);
CREATE INDEX IF NOT EXISTS idx_app_relationships_to ON app_relationships(to_table_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_app ON pipeline_runs(app_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);
CREATE INDEX IF NOT EXISTS idx_query_patterns_app ON query_patterns(app_id);
CREATE INDEX IF NOT EXISTS idx_test_queries_app ON test_queries(app_id);
CREATE INDEX IF NOT EXISTS idx_test_queries_user ON test_queries(user_id);
CREATE INDEX IF NOT EXISTS idx_curation_log_column ON curation_log(column_id);
CREATE INDEX IF NOT EXISTS idx_curation_log_user ON curation_log(user_id);
CREATE INDEX IF NOT EXISTS idx_curation_log_created ON curation_log(created_at);
CREATE INDEX IF NOT EXISTS idx_token_usage_app ON token_usage(app_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_run ON token_usage(pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_token_usage_stage ON token_usage(stage);
