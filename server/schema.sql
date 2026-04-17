-- Data Ask — Additional tables for unstructured pipeline (MySQL)
-- These are ADDITIVE to the existing BOKG Builder schema.
-- They do NOT modify any existing tables.

-- Document collections (e.g., "CDP User Guides", "OEBS AP Documentation")
CREATE TABLE IF NOT EXISTS doc_collections (
  id INT AUTO_INCREMENT PRIMARY KEY,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  doc_count INTEGER DEFAULT 0,
  chunk_count INTEGER DEFAULT 0,
  status VARCHAR(50) DEFAULT 'active',
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Individual source documents
CREATE TABLE IF NOT EXISTS doc_sources (
  id INT AUTO_INCREMENT PRIMARY KEY,
  collection_id INTEGER REFERENCES doc_collections(id) ON DELETE CASCADE,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  filename VARCHAR(500) NOT NULL,
  file_type VARCHAR(50) NOT NULL,
  file_size_bytes INTEGER,
  extracted_text LONGTEXT,
  page_count INTEGER,
  chunk_count INTEGER DEFAULT 0,
  status VARCHAR(50) DEFAULT 'pending',
  error_message TEXT,
  metadata JSON,
  uploaded_by INTEGER REFERENCES users(id),
  created_at TIMESTAMP DEFAULT NOW()
);

-- Document chunks (no vector embeddings in MySQL — use LIKE-based search)
CREATE TABLE IF NOT EXISTS doc_chunks (
  id INT AUTO_INCREMENT PRIMARY KEY,
  source_id INTEGER REFERENCES doc_sources(id) ON DELETE CASCADE,
  collection_id INTEGER REFERENCES doc_collections(id) ON DELETE CASCADE,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  chunk_index INTEGER NOT NULL,
  content LONGTEXT NOT NULL,
  content_length INTEGER,
  embedding LONGTEXT,
  metadata JSON,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for filtering
CREATE INDEX IF NOT EXISTS idx_doc_chunks_app_id ON doc_chunks(app_id);
CREATE INDEX IF NOT EXISTS idx_doc_chunks_collection_id ON doc_chunks(collection_id);
CREATE INDEX IF NOT EXISTS idx_doc_chunks_source_id ON doc_chunks(source_id);

-- Workspaces
CREATE TABLE IF NOT EXISTS workspaces (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  app_id INTEGER REFERENCES applications(id) ON DELETE SET NULL,
  is_default TINYINT(1) DEFAULT 0,
  status VARCHAR(50) DEFAULT 'active',
  created_by INTEGER REFERENCES users(id),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

-- Workspace membership
CREATE TABLE IF NOT EXISTS workspace_members (
  id INT AUTO_INCREMENT PRIMARY KEY,
  workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role VARCHAR(50) DEFAULT 'reader',
  is_default TINYINT(1) DEFAULT 0,
  start_date DATE DEFAULT (CURRENT_DATE),
  end_date DATE,
  enabled TINYINT(1) DEFAULT 1,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE KEY uq_ws_member (workspace_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_workspace_members_user ON workspace_members(user_id);
CREATE INDEX IF NOT EXISTS idx_workspace_members_workspace ON workspace_members(workspace_id);
CREATE INDEX IF NOT EXISTS idx_workspaces_app ON workspaces(app_id);

-- Link doc_collections to workspaces
ALTER TABLE doc_collections ADD COLUMN IF NOT EXISTS workspace_id INTEGER REFERENCES workspaces(id);
CREATE INDEX IF NOT EXISTS idx_doc_collections_workspace ON doc_collections(workspace_id);

-- Link doc_chunks to workspaces
ALTER TABLE doc_chunks ADD COLUMN IF NOT EXISTS workspace_id INTEGER REFERENCES workspaces(id);
CREATE INDEX IF NOT EXISTS idx_doc_chunks_workspace ON doc_chunks(workspace_id);

-- Conversation history for Data Ask
CREATE TABLE IF NOT EXISTS ida_conversations (
  id INT AUTO_INCREMENT PRIMARY KEY,
  app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
  workspace_id INTEGER REFERENCES workspaces(id) ON DELETE SET NULL,
  user_id INTEGER REFERENCES users(id),
  session_id VARCHAR(100),
  role VARCHAR(20) NOT NULL,
  content TEXT NOT NULL,
  intent VARCHAR(20),
  response_data JSON,
  confidence VARCHAR(20),
  token_usage JSON,
  created_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE ida_conversations ADD COLUMN IF NOT EXISTS workspace_id INTEGER REFERENCES workspaces(id);
CREATE INDEX IF NOT EXISTS idx_ida_conversations_workspace ON ida_conversations(workspace_id);
CREATE INDEX IF NOT EXISTS idx_ida_conversations_session ON ida_conversations(session_id);
CREATE INDEX IF NOT EXISTS idx_ida_conversations_app ON ida_conversations(app_id);
