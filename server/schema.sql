-- Data Ask — Additional tables for unstructured pipeline
-- These are ADDITIVE to the existing BOKG Builder schema.
-- They do NOT modify any existing tables.

-- Enable pgvector extension (if not already enabled)
CREATE EXTENSION IF NOT EXISTS vector;

-- Document collections (e.g., "CDP User Guides", "OEBS AP Documentation")
CREATE TABLE IF NOT EXISTS doc_collections (
  id SERIAL PRIMARY KEY,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  doc_count INTEGER DEFAULT 0,
  chunk_count INTEGER DEFAULT 0,
  status VARCHAR(50) DEFAULT 'active',  -- active, archived
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Individual source documents
CREATE TABLE IF NOT EXISTS doc_sources (
  id SERIAL PRIMARY KEY,
  collection_id INTEGER REFERENCES doc_collections(id) ON DELETE CASCADE,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  filename VARCHAR(500) NOT NULL,
  file_type VARCHAR(50) NOT NULL,       -- pdf, docx, txt, md, html
  file_size_bytes INTEGER,
  extracted_text TEXT,                   -- Full extracted text
  page_count INTEGER,
  chunk_count INTEGER DEFAULT 0,
  status VARCHAR(50) DEFAULT 'pending', -- pending, processing, ready, error
  error_message TEXT,
  metadata JSONB DEFAULT '{}',          -- title, author, custom fields
  uploaded_by INTEGER REFERENCES users(id),
  created_at TIMESTAMP DEFAULT NOW()
);

-- Document chunks with vector embeddings
CREATE TABLE IF NOT EXISTS doc_chunks (
  id SERIAL PRIMARY KEY,
  source_id INTEGER REFERENCES doc_sources(id) ON DELETE CASCADE,
  collection_id INTEGER REFERENCES doc_collections(id) ON DELETE CASCADE,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  chunk_index INTEGER NOT NULL,         -- Position in document
  content TEXT NOT NULL,                 -- Chunk text
  content_length INTEGER,
  embedding vector(1536),               -- OpenAI text-embedding-3-small
  metadata JSONB DEFAULT '{}',          -- page_number, section_title, etc.
  created_at TIMESTAMP DEFAULT NOW()
);

-- Index for vector similarity search
CREATE INDEX IF NOT EXISTS idx_doc_chunks_embedding
  ON doc_chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- Index for filtering by app
CREATE INDEX IF NOT EXISTS idx_doc_chunks_app_id ON doc_chunks(app_id);
CREATE INDEX IF NOT EXISTS idx_doc_chunks_collection_id ON doc_chunks(collection_id);

-- Full-text search index on chunk content (tsvector for hybrid search)
ALTER TABLE doc_chunks ADD COLUMN IF NOT EXISTS content_tsv tsvector;
CREATE INDEX IF NOT EXISTS idx_doc_chunks_tsv ON doc_chunks USING GIN(content_tsv);

-- Trigger to auto-update tsvector on insert/update
CREATE OR REPLACE FUNCTION update_chunk_tsv() RETURNS trigger AS $$
BEGIN
  NEW.content_tsv := to_tsvector('english', NEW.content);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_doc_chunks_tsv ON doc_chunks;
CREATE TRIGGER trg_doc_chunks_tsv
  BEFORE INSERT OR UPDATE OF content ON doc_chunks
  FOR EACH ROW EXECUTE FUNCTION update_chunk_tsv();

-- ─── Workspaces (maps to CDP Knowledge Bases) ───
-- A workspace groups structured data (app) + document collections for a user's scope.
-- In production CDP, this maps to KB (Knowledge Base) assignments.

CREATE TABLE IF NOT EXISTS workspaces (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  app_id INTEGER REFERENCES applications(id) ON DELETE SET NULL,  -- nullable: doc-only workspaces
  is_default BOOLEAN DEFAULT FALSE,
  status VARCHAR(50) DEFAULT 'active',  -- active, archived
  created_by INTEGER REFERENCES users(id),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Workspace membership (maps to CDP KB Assignment)
-- Controls which users can access which workspaces and their data
CREATE TABLE IF NOT EXISTS workspace_members (
  id SERIAL PRIMARY KEY,
  workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role VARCHAR(50) DEFAULT 'reader',     -- reader, writer, admin (maps to CDP Read/Write privilege)
  is_default BOOLEAN DEFAULT FALSE,      -- User's default workspace on login
  start_date DATE DEFAULT CURRENT_DATE,  -- Maps to CDP KB Assignment start date
  end_date DATE,                         -- Maps to CDP KB Assignment end date (null = no expiry)
  enabled BOOLEAN DEFAULT TRUE,          -- Maps to CDP KB Assignment enabled toggle
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(workspace_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_workspace_members_user ON workspace_members(user_id);
CREATE INDEX IF NOT EXISTS idx_workspace_members_workspace ON workspace_members(workspace_id);
CREATE INDEX IF NOT EXISTS idx_workspaces_app ON workspaces(app_id);

-- Link doc_collections to workspaces (a collection belongs to a workspace)
ALTER TABLE doc_collections ADD COLUMN IF NOT EXISTS workspace_id INTEGER REFERENCES workspaces(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_doc_collections_workspace ON doc_collections(workspace_id);

-- Link doc_chunks to workspaces for direct cross-workspace search
ALTER TABLE doc_chunks ADD COLUMN IF NOT EXISTS workspace_id INTEGER REFERENCES workspaces(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_doc_chunks_workspace ON doc_chunks(workspace_id);

-- Conversation history for Data Ask
CREATE TABLE IF NOT EXISTS ida_conversations (
  id SERIAL PRIMARY KEY,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  workspace_id INTEGER REFERENCES workspaces(id) ON DELETE SET NULL,
  user_id INTEGER REFERENCES users(id),
  session_id VARCHAR(100),              -- Group messages into sessions
  role VARCHAR(20) NOT NULL,            -- user, assistant
  content TEXT NOT NULL,
  intent VARCHAR(20),                   -- structured, unstructured, hybrid, clarify
  response_data JSONB,                  -- Full response payload (SQL results, citations, etc.)
  confidence VARCHAR(20),               -- high, medium, low, exploratory
  token_usage JSONB,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Add workspace_id column if table already exists (idempotent migration)
ALTER TABLE ida_conversations ADD COLUMN IF NOT EXISTS workspace_id INTEGER REFERENCES workspaces(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_ida_conversations_workspace ON ida_conversations(workspace_id);

CREATE INDEX IF NOT EXISTS idx_ida_conversations_session ON ida_conversations(session_id);
CREATE INDEX IF NOT EXISTS idx_ida_conversations_app ON ida_conversations(app_id);
