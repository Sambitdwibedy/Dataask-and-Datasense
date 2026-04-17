/**
 * IDA — Intelligent Data Assistant (Consolidated)
 *
 * Merged from BOKG Builder + Data Ask into a single Express server.
 * Serves the full 8-stage KG pipeline, NL2SQL query engine,
 * unstructured document pipeline (pgvector), and intent routing.
 *
 * Shares PostgreSQL via DATABASE_URL.
 */
require('dotenv').config();
const express = require('express');
const path = require('path');
const cors = require('cors');
const helmet = require('helmet');
const compression = require('compression');
const fs = require('fs');

const { requireAuth, optionalAuth } = require('./middleware/auth');

// ─── BOKG Builder Routes ───
const authRoutes = require('./routes/auth');
const applicationsRoutes = require('./routes/applications');
const pipelineRoutes = require('./routes/pipeline');
const schemaRoutes = require('./routes/schema');
const curationRoutes = require('./routes/curation');
const patternsRoutes = require('./routes/patterns');
const qualityRoutes = require('./routes/quality');
const queryEngineRoutes = require('./routes/query-engine');
const dataBrowserRoutes = require('./routes/data-browser');
const consumptionRoutes = require('./routes/consumption');
const contextRoutes = require('./routes/context');

// ─── Data Ask Routes ───
const askRoutes = require('./routes/ask');
const documentsRoutes = require('./routes/documents');
const workspacesRoutes = require('./routes/workspaces');
const synonymsRoutes = require('./routes/synonyms');
const pipelineStepsRoutes = require('./routes/pipeline-steps');

const app = express();
const PORT = process.env.PORT || 3002;
const CORS_ORIGIN = process.env.CORS_ORIGIN || '*';

// ─── Middleware ───
// Helmet CSP: superset of both apps (CDN scripts + SheetJS + mediaSrc for voice)
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'", "'unsafe-eval'",
        "https://cdnjs.cloudflare.com", "https://unpkg.com",
        "https://cdn.jsdelivr.net", "https://cdn.sheetjs.com"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:", "https:"],
      connectSrc: ["'self'", "https:", "http:"],
      fontSrc: ["'self'", "https:", "data:"],
      mediaSrc: ["'self'", "blob:"],
    },
  },
}));
app.use(compression());
app.use(cors({
  origin: CORS_ORIGIN === '*' ? true : CORS_ORIGIN,
  credentials: true,
}));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// ─── Static Files ───
app.use(express.static(path.join(__dirname, 'client')));

// ─── Health Check ───
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    service: 'ida-data-ask',
    timestamp: new Date().toISOString(),
    version: process.env.APP_VERSION || '0.3.0',
  });
});

// ─── Diagnostics (no auth — for debugging deploys) ───
app.get('/api/diag', async (req, res) => {
  try {
    const { query } = require('./db');
    const checks = {};
    const tables = ['applications', 'app_tables', 'app_columns', 'app_relationships',
      'doc_collections', 'doc_sources', 'doc_chunks', 'ida_conversations',
      'users', 'token_usage', 'context_documents', 'query_patterns', 'test_queries',
      'global_synonyms', 'app_synonyms'];
    for (const t of tables) {
      try {
        const [r] = await query(`SELECT COUNT(*) as c FROM ${t}`);
        checks[t] = parseInt(r[0].c);
      } catch (e) {
        checks[t] = `ERROR: ${e.message.substring(0, 80)}`;
      }
    }
    // pgvector not available in MySQL
    checks.pgvector = 'N/A (MySQL)';
    checks.anthropic_key = process.env.ANTHROPIC_API_KEY ? 'set' : 'MISSING';
    checks.openai_key = process.env.OPENAI_API_KEY ? 'set' : 'MISSING';
    // Check appdata schemas
    try {
      const [schemas] = await query(`SELECT schema_name FROM information_schema.SCHEMATA WHERE schema_name LIKE 'appdata_%' ORDER BY schema_name`);
      checks.appdata_schemas = schemas.map(r => r.schema_name);
      for (const s of checks.appdata_schemas) {
        const [tblCount] = await query(`SELECT COUNT(*) as c FROM information_schema.tables WHERE table_schema = ?`, [s]);
        checks[`${s}_tables`] = parseInt(tblCount[0].c);
      }
    } catch (e) {
      checks.appdata_schemas = `ERROR: ${e.message.substring(0, 80)}`;
    }
    res.json({ status: 'ok', checks });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Debug: step-by-step ask test (no auth, temporary) ───
app.get('/api/test-ask', async (req, res) => {
  const steps = {};
  try {
    const { query: dbQuery } = require('./db');
    const appIdParam = req.query.appId || '1';
    const question = req.query.q || 'How many tables are there?';
    steps.params = { appId: appIdParam, question };

    // Step 1: Check app exists
    const [appRows] = await dbQuery('SELECT id, name FROM applications WHERE id = ?', [appIdParam]);
    steps.step1_app = appRows[0] || 'NOT FOUND';

    // Step 2: Count tables for this app
    const [tableCount] = await dbQuery(
      `SELECT COUNT(*) as c FROM app_tables WHERE app_id = ? AND enrichment_status IN ('approved','ai_enriched')`,
      [appIdParam]
    );
    steps.step2_tables = parseInt(tableCount[0].c);

    // Step 3: Intent classification
    try {
      const { classifyIntent } = require('./services/intent-router');
      const classification = await classifyIntent(question, { appId: appIdParam });
      steps.step3_intent = classification;
    } catch (e) {
      steps.step3_intent = `ERROR: ${e.message}`;
      res.json({ steps });
      return;
    }

    // Step 4: Try structured engine (schema load only)
    try {
      const { loadSchemaContext } = require('./services/structured-engine');
      const schema = await loadSchemaContext(appIdParam);
      steps.step4_schema = {
        tables: schema.tables.length,
        columns: schema.columns.length,
        relationships: schema.relationships.length,
        contextDocs: schema.contextDocs.length,
      };
    } catch (e) {
      steps.step4_schema = `ERROR: ${e.message}`;
    }

    // Step 5: Full query (only if we got this far)
    try {
      const { queryStructuredData } = require('./services/structured-engine');
      const result = await queryStructuredData(question, { appId: appIdParam, userId: null, conversationHistory: [] });
      steps.step5_query = {
        hasSql: !!result.sql,
        hasResults: !!(result.results && result.results.rows),
        rowCount: result.results?.rowCount || 0,
        confidence: result.confidence,
        answer: result.answer?.substring(0, 200),
        error: result.error,
      };
    } catch (e) {
      steps.step5_query = `ERROR: ${e.message}`;
    }

    res.json({ status: 'ok', steps });
  } catch (err) {
    steps.fatal = err.message;
    res.json({ status: 'error', steps });
  }
});

// ─── API Routes (BOKG Builder) ───
app.use('/api/auth', authRoutes);
app.use('/api/applications', requireAuth, applicationsRoutes);
app.use('/api/pipeline', requireAuth, pipelineRoutes);
app.use('/api/schema', requireAuth, schemaRoutes);
app.use('/api/curation', requireAuth, curationRoutes);
app.use('/api/patterns', requireAuth, patternsRoutes);
app.use('/api/quality', requireAuth, qualityRoutes);
app.use('/api/test', requireAuth, queryEngineRoutes);
app.use('/api/data', requireAuth, dataBrowserRoutes);
app.use('/api/consumption', requireAuth, consumptionRoutes);
app.use('/api/context', requireAuth, contextRoutes);
app.use('/api/pipeline-steps', requireAuth, pipelineStepsRoutes);

// ─── API Routes (Data Ask) ───
app.use('/api/ask', requireAuth, askRoutes);
app.use('/api/documents', requireAuth, documentsRoutes);
app.use('/api/workspaces', requireAuth, workspacesRoutes);
app.use('/api/synonyms', requireAuth, synonymsRoutes);

// ─── Error Handling ───
app.use((err, req, res, next) => {
  console.error('Error:', err.message);

  if (err.name === 'MulterError') {
    return res.status(400).json({ error: `Upload error: ${err.message}` });
  }
  if (err.name === 'ValidationError') {
    return res.status(400).json({ error: err.message });
  }
  if (err.name === 'UnauthorizedError') {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  res.status(err.status || 500).json({
    error: err.message || 'Internal Server Error',
  });
});

// ─── SPA Fallback ───
app.use((req, res) => {
  if (req.path.startsWith('/api/')) {
    return res.status(404).json({ error: 'Not Found' });
  }
  res.sendFile(path.join(__dirname, 'client', 'index.html'));
});

// ═══════════════════════════════════════════════════════════════
//  STARTUP FUNCTIONS (merged from BOKG Builder + Data Ask)
// ═══════════════════════════════════════════════════════════════

// ─── Data Ask: pgvector / document tables / tsvector ───
async function initDatabase() {
  try {
    const { query } = require('./db');

    const steps = [
      { name: 'pgvector extension (skipped for MySQL)', sql: `SELECT 1` },
      { name: 'doc_collections table', sql: `
        CREATE TABLE IF NOT EXISTS doc_collections (
          id INT AUTO_INCREMENT PRIMARY KEY,
          app_id INTEGER,
          name VARCHAR(255) NOT NULL,
          description TEXT,
          doc_count INTEGER DEFAULT 0,
          chunk_count INTEGER DEFAULT 0,
          status VARCHAR(50) DEFAULT 'active',
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW()
        )` },
      { name: 'doc_sources table', sql: `
        CREATE TABLE IF NOT EXISTS doc_sources (
          id INT AUTO_INCREMENT PRIMARY KEY,
          collection_id INTEGER,
          app_id INTEGER,
          filename VARCHAR(500) NOT NULL,
          file_type VARCHAR(50) NOT NULL,
          file_size_bytes INTEGER,
          extracted_text LONGTEXT,
          page_count INTEGER,
          chunk_count INTEGER DEFAULT 0,
          status VARCHAR(50) DEFAULT 'pending',
          error_message TEXT,
          metadata JSON,
          uploaded_by INTEGER,
          created_at TIMESTAMP DEFAULT NOW()
        )` },
      { name: 'doc_chunks table', sql: `
        CREATE TABLE IF NOT EXISTS doc_chunks (
          id INT AUTO_INCREMENT PRIMARY KEY,
          source_id INTEGER,
          collection_id INTEGER,
          app_id INTEGER,
          chunk_index INTEGER NOT NULL,
          content LONGTEXT NOT NULL,
          content_length INTEGER,
          embedding LONGTEXT,
          metadata JSON,
          created_at TIMESTAMP DEFAULT NOW()
        )` },
      { name: 'doc_chunks index app_id', sql: `CREATE INDEX IF NOT EXISTS idx_doc_chunks_app_id ON doc_chunks(app_id)` },
      { name: 'doc_chunks index collection_id', sql: `CREATE INDEX IF NOT EXISTS idx_doc_chunks_collection_id ON doc_chunks(collection_id)` },
      { name: 'content_tsv column (skipped)', sql: `SELECT 1` },
      { name: 'content_tsv GIN index (skipped)', sql: `SELECT 1` },
      { name: 'tsvector trigger (skipped)', sql: `SELECT 1` },
      { name: 'tsvector trigger (skipped)', sql: `SELECT 1` },
      { name: 'ida_conversations table', sql: `
        CREATE TABLE IF NOT EXISTS ida_conversations (
          id INT AUTO_INCREMENT PRIMARY KEY,
          app_id INTEGER,
          user_id INTEGER,
          session_id VARCHAR(100),
          role VARCHAR(20) NOT NULL,
          content LONGTEXT NOT NULL,
          intent VARCHAR(20),
          response_data JSON,
          confidence VARCHAR(20),
          token_usage JSON,
          created_at TIMESTAMP DEFAULT NOW()
        )` },
      { name: 'ida_conversations index session', sql: `CREATE INDEX IF NOT EXISTS idx_ida_conversations_session ON ida_conversations(session_id)` },
      { name: 'ida_conversations index app', sql: `CREATE INDEX IF NOT EXISTS idx_ida_conversations_app ON ida_conversations(app_id)` },
    ];

    for (const step of steps) {
      try {
        await query(step.sql);
        console.log(`  ✓ ${step.name}`);
      } catch (err) {
        if (err.message.includes('already exists') || err.message.includes('duplicate')) {
          console.log(`  ✓ ${step.name} (exists)`);
        } else {
          console.warn(`  ⚠ ${step.name}: ${err.message.substring(0, 120)}`);
        }
      }
    }

    // ivfflat index skipped — pgvector not available in MySQL
    console.log('  ○ ivfflat vector index skipped (MySQL — using LIKE-based fallback)');

    console.log('✓ Data Ask schema initialized');
  } catch (err) {
    console.error('Database init error:', err.message);
  }
}

// ─── BOKG: Ensure admin user has bcrypt password hash ───
async function ensureAdminUser() {
  try {
    const bcryptjs = require('bcryptjs');
    const { query } = require('./db');

    const [rows] = await query('SELECT id, password_hash FROM users WHERE email = ?', ['mark@solix.com']);

    if (rows.length === 0) {
      const hash = await bcryptjs.hash('demo2026', 10);
      await query(
        'INSERT INTO users (email, password_hash, name, role) VALUES (?, ?, ?, ?)',
        ['mark@solix.com', hash, 'Mark Lee', 'admin']
      );
      console.log('✓ Admin user created with bcrypt password');
    } else if (!rows[0].password_hash.startsWith('?')) {
      const hash = await bcryptjs.hash('demo2026', 10);
      await query('UPDATE users SET password_hash = ? WHERE email = ?', [hash, 'mark@solix.com']);
      console.log('✓ Admin user password hash migrated to bcrypt');
    } else {
      console.log('✓ Admin user OK');
    }
  } catch (err) {
    console.error('Admin user check failed (DB may not be ready):', err.message);
  }
}

// ─── BOKG: Ensure end-user demo account ───
async function ensureEndUser() {
  try {
    const bcryptjs = require('bcryptjs');
    const { query } = require('./db');

    const [rows] = await query('SELECT id FROM users WHERE email = ?', ['analyst@solix.com']);

    if (rows.length === 0) {
      const hash = await bcryptjs.hash('demo2026', 10);
      await query(
        'INSERT INTO users (email, password_hash, name, role) VALUES (?, ?, ?, ?)',
        ['analyst@solix.com', hash, 'Mark Lee', 'end_user']
      );
      console.log('✓ End-user demo account created (analyst@solix.com)');
    } else {
      await query('UPDATE users SET role = ?, name = ? WHERE email = ?', ['end_user', 'Mark Lee', 'analyst@solix.com']);
      console.log('✓ End-user demo account OK');
    }
  } catch (err) {
    console.error('End-user account check failed:', err.message);
  }
}

// ─── BOKG: Lightweight migrations for new tables/columns ───
async function runMigrations() {
  try {
    const { query: dbQuery } = require('./db');

    // Add token_usage table
    await dbQuery(`CREATE TABLE IF NOT EXISTS token_usage (
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
    )`);
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_token_usage_app ON token_usage(app_id)');
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_token_usage_run ON token_usage(pipeline_run_id)');

    // Add entity_metadata JSON column to app_tables
    try { await dbQuery(`ALTER TABLE app_tables ADD COLUMN entity_metadata JSON DEFAULT ('{}')`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }

    // Add entity-level approval fields to app_tables
    try { await dbQuery(`ALTER TABLE app_tables ADD COLUMN enrichment_status VARCHAR(50) DEFAULT 'draft'`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }
    try { await dbQuery(`ALTER TABLE app_tables ADD COLUMN confidence_score DECIMAL(5,2) DEFAULT 0`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }
    try { await dbQuery(`ALTER TABLE app_tables ADD COLUMN enriched_by VARCHAR(50)`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }
    try { await dbQuery(`ALTER TABLE app_tables ADD COLUMN enriched_at TIMESTAMP`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }

    // Add relationship approval fields to app_relationships
    try { await dbQuery(`ALTER TABLE app_relationships ADD COLUMN enrichment_status VARCHAR(50) DEFAULT 'ai_enriched'`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }
    try { await dbQuery(`ALTER TABLE app_relationships ADD COLUMN confidence_score DECIMAL(5,2) DEFAULT 80`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }
    try { await dbQuery(`ALTER TABLE app_relationships ADD COLUMN enriched_by VARCHAR(50) DEFAULT 'ai'`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }
    try { await dbQuery(`ALTER TABLE app_relationships ADD COLUMN enriched_at TIMESTAMP`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }

    // Create context_documents table for Context-Assisted Build
    await dbQuery(`CREATE TABLE IF NOT EXISTS context_documents (
      id INT AUTO_INCREMENT PRIMARY KEY,
      app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
      filename VARCHAR(255) NOT NULL,
      file_type VARCHAR(50) NOT NULL,
      file_size INTEGER DEFAULT 0,
      extracted_text TEXT,
      description TEXT,
      metadata JSON,
      uploaded_by INTEGER REFERENCES users(id),
      uploaded_at TIMESTAMP DEFAULT NOW()
    )`);
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_context_docs_app ON context_documents(app_id)');
    try { await dbQuery(`ALTER TABLE context_documents ADD COLUMN metadata JSON`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }

    // Create query_patterns table (used by quality dashboard)
    await dbQuery(`CREATE TABLE IF NOT EXISTS query_patterns (
      id INT AUTO_INCREMENT PRIMARY KEY,
      app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
      pattern_name VARCHAR(255),
      pattern_type VARCHAR(50),
      sql_template TEXT,
      tables_involved TEXT,
      description TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    )`);
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_query_patterns_app ON query_patterns(app_id)');

    // Create test_queries table (for NL2SQL history + feedback)
    await dbQuery(`CREATE TABLE IF NOT EXISTS test_queries (
      id INT AUTO_INCREMENT PRIMARY KEY,
      app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
      user_id INTEGER REFERENCES users(id),
      nl_query TEXT NOT NULL,
      generated_sql TEXT,
      execution_result JSON,
      confidence DECIMAL(3,2) DEFAULT 0,
      feedback VARCHAR(20),
      created_at TIMESTAMP DEFAULT NOW()
    )`);
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_test_queries_app ON test_queries(app_id)');
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_test_queries_user ON test_queries(user_id)');

    // Add published_at and published_by to applications
    try { await dbQuery(`ALTER TABLE applications ADD COLUMN published_at TIMESTAMP`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }
    try { await dbQuery(`ALTER TABLE applications ADD COLUMN published_by INTEGER REFERENCES users(id)`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }

    // Add progress_snapshot column to pipeline_runs for resilience across restarts
    try { await dbQuery(`ALTER TABLE pipeline_runs ADD COLUMN progress_snapshot JSON`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }

    // Add column_role to app_columns
    try { await dbQuery(`ALTER TABLE app_columns ADD COLUMN column_role VARCHAR(30)`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }

    // Recover zombie runs: any runs marked 'running' in the DB but with no active process
    const [zombieRuns] = await dbQuery(
      `SELECT id, app_id FROM pipeline_runs WHERE status = 'running'`
    );
    for (const zombie of zombieRuns) {
      console.log(`[Recovery] Marking zombie pipeline run #${zombie.id} (app ${zombie.app_id}) as failed — server restarted`);
      const [stagesRows] = await dbQuery('SELECT stages FROM pipeline_runs WHERE id = ?', [zombie.id]);
      const stages = stagesRows[0]?.stages || {};
      const parsedStages = typeof stages === 'string' ? JSON.parse(stages) : stages;
      for (const [name, stage] of Object.entries(parsedStages)) {
        if (stage.status === 'running' || stage.status === 'awaiting_approval') {
          stage.status = 'failed';
          stage.error = 'Server restarted — pipeline interrupted';
          stage.completed_at = new Date().toISOString();
        }
      }
      await dbQuery(
        `UPDATE pipeline_runs SET status = 'failed', completed_at = NOW(), stages = ? WHERE id = ?`,
        [JSON.stringify(parsedStages), zombie.id]
      );
      await dbQuery("UPDATE applications SET status = 'profiling', updated_at = NOW() WHERE id = ?", [zombie.app_id]);
    }
    if (zombieRuns.length > 0) {
      console.log(`[Recovery] Cleaned up ${zombieRuns.length} zombie pipeline run(s)`);
    }

    // ── Workspace model (v0.4.0) ──
    await dbQuery(`CREATE TABLE IF NOT EXISTS workspaces (
      id INT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      description TEXT,
      app_id INTEGER REFERENCES applications(id) ON DELETE SET NULL,
      is_default TINYINT(1) DEFAULT 0,
      status VARCHAR(50) DEFAULT 'active',
      created_by INTEGER REFERENCES users(id),
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )`);
    await dbQuery(`CREATE TABLE IF NOT EXISTS workspace_members (
      id INT AUTO_INCREMENT PRIMARY KEY,
      workspace_id INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
      user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      role VARCHAR(50) DEFAULT 'reader',
      is_default TINYINT(1) DEFAULT 0,
      start_date DATE DEFAULT (CURRENT_DATE),
      end_date DATE,
      enabled TINYINT(1) DEFAULT 1,
      created_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(workspace_id, user_id)
    )`);
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_workspace_members_user ON workspace_members(user_id)');
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_workspace_members_ws ON workspace_members(workspace_id)');
    // Add workspace_id to existing tables
    try { await dbQuery(`ALTER TABLE doc_collections ADD COLUMN workspace_id INTEGER REFERENCES workspaces(id)`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }
    try { await dbQuery(`ALTER TABLE doc_chunks ADD COLUMN workspace_id INTEGER`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }
    try { await dbQuery(`ALTER TABLE ida_conversations ADD COLUMN workspace_id INTEGER`) } catch(e) { if (!e.message.includes('Duplicate column')) throw e; }
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_doc_collections_ws ON doc_collections(workspace_id)');
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_doc_chunks_ws ON doc_chunks(workspace_id)');
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_conversations_ws ON ida_conversations(workspace_id)');
    console.log('✓ Workspace tables ready');

    // ── Synonym & Ontology Management ──
    await dbQuery(`CREATE TABLE IF NOT EXISTS global_synonyms (
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
    )`);
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_global_synonyms_term ON global_synonyms(term)');
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_global_synonyms_pack ON global_synonyms(domain_pack)');

    await dbQuery(`CREATE TABLE IF NOT EXISTS app_synonyms (
      id INT AUTO_INCREMENT PRIMARY KEY,
      app_id INTEGER REFERENCES applications(id) ON DELETE CASCADE,
      column_id INTEGER,
      table_id INTEGER,
      term VARCHAR(255) NOT NULL,
      source VARCHAR(50) DEFAULT 'builder_curated',
      confidence_score DECIMAL(5,2) DEFAULT 90,
      status VARCHAR(30) DEFAULT 'active',
      global_synonym_id INTEGER REFERENCES global_synonyms(id),
      created_by INTEGER REFERENCES users(id),
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )`);
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_app_synonyms_app ON app_synonyms(app_id)');
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_app_synonyms_col ON app_synonyms(column_id)');
    await dbQuery('CREATE INDEX IF NOT EXISTS idx_app_synonyms_status ON app_synonyms(app_id, status)');
    console.log('✓ Synonym tables ready');

    console.log('✓ Migrations complete');
  } catch (err) {
    console.error('Migration warning:', err.message);
  }
}

// ─── BOKG: Ensure enterprise demo apps (OEBS, SAP) ───
async function ensureEnterpriseApps() {
  try {
    const { query: dbQuery } = require('./db');

    const apps = [
      {
        name: 'Oracle E-Business Suite R12',
        type: 'Oracle EBS',
        description: 'Oracle E-Business Suite R12 — 11 modules (AP, AR, GL, PO, OM, INV, HR, PAY, BEN, CE, FND) with 123 tables, 1,924 columns, and 58,249 rows of synthetic demo data. Includes FND_ AOL metadata tables for BOKG enrichment.',
        config: {
          sourceType: 'oracle_ebs', version: 'R12.2',
          modules: ['AP', 'AR', 'GL', 'PO', 'OM', 'INV', 'HR', 'PAY', 'BEN', 'CE', 'FND'],
          tableCount: 123, columnCount: 1924,
          pipeline: { parallel_concurrency: 4, batch_column_threshold: 8, batch_max_columns: 30, sample_row_count: 10 },
          query_engine: { model: 'claude-sonnet-4-20250514', show_token_cost: true, show_sql_details: true, schema_link_threshold: 15, column_link_threshold: 20, auto_seed_qpd: false, max_seed_questions_per_entity: 3 }
        }
      },
      {
        name: 'SAP ECC 6.0',
        type: 'SAP ECC',
        description: 'SAP ECC 6.0 — 10 modules (FI-GL, FI-AP, FI-AR, CO, SD, MM, HR, PAY, BEN, PM) with 124 tables and 3,747 rows of synthetic demo data covering core Finance, Sales & Distribution, Materials Management, and Human Resources.',
        config: {
          sourceType: 'sap_ecc', version: '6.0 EhP8',
          modules: ['FI_GL', 'FI_AP', 'FI_AR', 'CO', 'SD', 'MM', 'HR', 'PAY', 'BEN', 'PM'],
          tableCount: 124, columnCount: 699,
          pipeline: { parallel_concurrency: 4, batch_column_threshold: 8, batch_max_columns: 30, sample_row_count: 10 },
          query_engine: { model: 'claude-sonnet-4-20250514', show_token_cost: true, show_sql_details: true, schema_link_threshold: 15, column_link_threshold: 20, auto_seed_qpd: false, max_seed_questions_per_entity: 3 }
        }
      }
    ];

    for (const app of apps) {
      await dbQuery(
        `INSERT IGNORE INTO applications (name, type, description, status, config)
         VALUES (?, ?, ?, ?, ?)`,
        [app.name, app.type, app.description, 'ingesting', JSON.stringify(app.config)]
      );
    }
    console.log('✓ Enterprise demo apps OK (OEBS, SAP)');
  } catch (err) {
    console.error('Enterprise apps check failed:', err.message);
  }
}

// ─── BOKG: Auto-import QPD sample questions ───
async function importEnterpriseQPD() {
  try {
    const { query: dbQuery } = require('./db');
    const dataDir = path.join(__dirname, '..', 'data');

    const imports = [
      { appName: 'Oracle E-Business Suite R12', file: 'oebs_sample_questions.json' },
      { appName: 'SAP ECC 6.0', file: 'sap_sample_questions.json' },
    ];

    for (const { appName, file } of imports) {
      const filePath = path.join(dataDir, file);
      if (!fs.existsSync(filePath)) continue;

      const [appRows] = await dbQuery('SELECT id FROM applications WHERE name = ?', [appName]);
      if (appRows.length === 0) continue;
      const appId = appRows[0].id;

      const [existingRows] = await dbQuery('SELECT COUNT(*) as cnt FROM test_queries WHERE app_id = ?', [appId]);
      if (parseInt(existingRows[0].cnt) > 0) {
        console.log(`✓ QPD already loaded for ${appName} (${existingRows[0].cnt} queries)`);
        continue;
      }

      const [userRows] = await dbQuery('SELECT id FROM users WHERE email = ?', ['mark@solix.com']);
      const userId = userRows.length > 0 ? userRows[0].id : 1;

      const questions = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      let imported = 0;
      for (const q of questions) {
        if (!q.nl_query) continue;
        try {
          await dbQuery(
            `INSERT IGNORE INTO test_queries (app_id, user_id, nl_query, generated_sql, execution_result, confidence, feedback, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, NOW())`,
            [appId, userId, q.nl_query, q.sql_template || '',
             JSON.stringify({ source: 'prototype_import', objects: q.objects || [] }),
             0.85, 'thumbs_up']
          );
          imported++;
        } catch (e) { /* skip duplicates */ }
      }
      console.log(`✓ Imported ${imported} QPD queries for ${appName}`);
    }
  } catch (err) {
    console.error('QPD import check failed:', err.message);
  }
}

// ─── BOKG: Auto-upload schema reference context documents ───
async function importEnterpriseContext() {
  try {
    const { query: dbQuery } = require('./db');
    const dataDir = path.join(__dirname, '..', 'data');

    const docs = [
      { appName: 'Oracle E-Business Suite R12', file: 'oebs_schema_reference.txt', desc: 'OEBS R12 schema reference — modules, business objects, tables, columns, and relationships' },
      { appName: 'SAP ECC 6.0', file: 'sap_schema_reference.txt', desc: 'SAP ECC 6.0 schema reference — modules, business objects, tables, columns, and relationships' },
      { appName: 'Apache OFBiz 18.12', file: 'ofbiz_schema_reference.txt', desc: 'OFBiz 18.12 schema reference — 12 modules, 83 packages, 804 entities, package-to-domain mapping, business process flows (O2C, P2P, R2R), naming patterns' },
      { appName: 'Apache OFBiz 18.12', file: 'ofbiz_schema_analysis.md', desc: 'OFBiz schema analysis — module breakdown, package hierarchy, field type distribution, largest tables, LLM obscurity assessment' },
    ];

    for (const { appName, file, desc } of docs) {
      const filePath = path.join(dataDir, file);
      if (!fs.existsSync(filePath)) continue;

      const [appRows] = await dbQuery('SELECT id FROM applications WHERE name = ?', [appName]);
      if (appRows.length === 0) continue;
      const appId = appRows[0].id;

      const [existingRows] = await dbQuery('SELECT COUNT(*) as cnt FROM context_documents WHERE app_id = ? AND filename = ?', [appId, file]);
      if (parseInt(existingRows[0].cnt) > 0) {
        console.log(`✓ Context doc already loaded for ${appName}`);
        continue;
      }

      const [userRows] = await dbQuery('SELECT id FROM users WHERE email = ?', ['mark@solix.com']);
      const userId = userRows.length > 0 ? userRows[0].id : 1;

      const text = fs.readFileSync(filePath, 'utf8');
      await dbQuery(
        `INSERT INTO context_documents (app_id, filename, file_type, file_size, extracted_text, description, uploaded_by)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [appId, file, 'text/plain', text.length, text, desc, userId]
      );
      console.log(`✓ Imported context doc "${file}" for ${appName} (${text.length} chars)`);
    }
  } catch (err) {
    console.error('Context doc import failed:', err.message);
  }
}

// ─── BOKG: Patch entity_metadata with booking/sales/AR sample questions ───
async function patchSalesMetadata() {
  try {
    const { query: dbQuery } = require('./db');

    const [appRows] = await dbQuery("SELECT id FROM applications WHERE name = 'Oracle E-Business Suite R12'");
    if (appRows.length === 0) return;
    const appId = appRows[0].id;

    const patches = {
      'OE_ORDER_HEADERS_ALL': [
        'What are the total bookings for this quarter?',
        'Show me the booking trend by month for 2025',
        'How many sales orders were booked last month?',
        'What is the total booking value by sales rep?',
        'Show me all booked orders that have not been shipped yet',
        'Compare bookings vs revenue for the last 4 quarters',
      ],
      'OE_ORDER_LINES_ALL': [
        'What are the top 10 products by booking value?',
        'Show me order line details for booked orders this quarter',
        'What is the average order size by product category?',
        'Which items have the highest booking volume this year?',
      ],
      'RA_CUSTOMER_TRX_ALL': [
        'What is the total revenue this quarter?',
        'Show me the AR aging report by customer',
        'How many invoices are overdue?',
        'What is the total outstanding receivables amount?',
        'Show me revenue by month for the last 12 months',
        'Which customers have the largest unpaid balances?',
      ],
      'RA_CUSTOMER_TRX_LINES_ALL': [
        'What are the top selling products by revenue?',
        'Show me invoice line details for the largest invoices',
        'What is the average invoice amount by customer?',
      ],
      'AR_CASH_RECEIPTS_ALL': [
        'What is the total cash collected this quarter?',
        'Show me the collections trend by month',
        'Which customers have made the most payments?',
        'What is our Days Sales Outstanding (DSO)?',
      ],
      'AR_PAYMENT_SCHEDULES_ALL': [
        'Show me the AR aging buckets (current, 30, 60, 90+ days)',
        'Which invoices are past due by more than 60 days?',
        'What is the total overdue amount by customer?',
      ],
      'AR_RECEIVABLE_APPLICATIONS_ALL': [
        'Show me unapplied cash receipts',
        'What is the total amount applied to invoices this month?',
      ],
    };

    let patchCount = 0;
    for (const [tableName, newQuestions] of Object.entries(patches)) {
      const [tableRows] = await dbQuery(
        'SELECT id, entity_metadata FROM app_tables WHERE app_id = ? AND table_name = ?',
        [appId, tableName]
      );
      if (tableRows.length === 0) continue;

      const row = tableRows[0];
      const meta = row.entity_metadata || {};
      const existing = meta.sample_questions || [];

      const existingLower = new Set(existing.map(q => q.toLowerCase()));
      const merged = [...existing];
      for (const q of newQuestions) {
        if (!existingLower.has(q.toLowerCase())) {
          merged.push(q);
        }
      }

      if (merged.length > existing.length) {
        meta.sample_questions = merged;
        await dbQuery(
          'UPDATE app_tables SET entity_metadata = ? WHERE id = ?',
          [JSON.stringify(meta), row.id]
        );
        patchCount++;
      }
    }
    if (patchCount > 0) {
      console.log(`✓ Patched entity_metadata with sales/booking questions for ${patchCount} OEBS tables`);
    } else {
      console.log('✓ Sales/booking metadata already up to date');
    }
  } catch (err) {
    console.error('Sales metadata patch failed:', err.message);
  }
}

// ─── BOKG: Seed proven SQL templates for key demo queries ───
async function seedProvenQueryTemplates() {
  try {
    const { query: dbQuery } = require('./db');

    const [appRows] = await dbQuery("SELECT id FROM applications WHERE name = 'Oracle E-Business Suite R12'");
    if (appRows.length === 0) return;
    const appId = appRows[0].id;
    const schema = `appdata_${appId}`;

    // Clean up old non-aggregated AR aging queries
    try {
      await dbQuery(
        `DELETE FROM test_queries WHERE app_id = ? AND nl_query LIKE '%aging%'
         AND generated_sql LIKE '%TRX_NUMBER%' AND generated_sql NOT LIKE '%GROUP BY%'`,
        [appId]
      );
    } catch (e) { /* ignore */ }

    const [schemaCheck] = await dbQuery(
      `SELECT schema_name FROM information_schema.SCHEMATA WHERE schema_name = ?`, [schema]
    );
    if (schemaCheck.length === 0) return;

    const [userRows] = await dbQuery("SELECT id FROM users WHERE email = ?", ['mark@solix.com']);
    const userId = userRows.length > 0 ? userRows[0].id : 1;

    const templates = [
      {
        nl_query: 'What are the total bookings for this quarter?',
        sql: `SELECT SUM(ool.ORDERED_QUANTITY * ool.UNIT_SELLING_PRICE) AS total_bookings
FROM \`${schema}\`.OE_ORDER_HEADERS_ALL ooh
JOIN \`${schema}\`.OE_ORDER_LINES_ALL ool ON ooh.HEADER_ID = ool.HEADER_ID
WHERE ooh.BOOKED_FLAG = 'Y'
  AND ool.CANCELLED_FLAG = 'N'
  AND ooh.BOOKED_DATE >= DATE_FORMAT(CURDATE(), '%Y-%m-01')`,
        objects: ['Sales_Order']
      },
      {
        nl_query: 'Show me the booking trend by month for 2025',
        sql: `SELECT DATE_FORMAT(ooh.BOOKED_DATE, '%Y-%m') AS month,
  COUNT(DISTINCT ooh.HEADER_ID) AS order_count,
  SUM(ool.ORDERED_QUANTITY * ool.UNIT_SELLING_PRICE) AS total_booking_value
FROM \`${schema}\`.OE_ORDER_HEADERS_ALL ooh
JOIN \`${schema}\`.OE_ORDER_LINES_ALL ool ON ooh.HEADER_ID = ool.HEADER_ID
WHERE ooh.BOOKED_FLAG = 'Y'
  AND ool.CANCELLED_FLAG = 'N'
  AND ooh.BOOKED_DATE >= '2025-01-01'
  AND ooh.BOOKED_DATE < '2026-01-01'
GROUP BY DATE_FORMAT(ooh.BOOKED_DATE, '%Y-%m')
ORDER BY month`,
        objects: ['Sales_Order']
      },
      {
        nl_query: 'What is the total revenue this quarter?',
        sql: `SELECT SUM(rctl.EXTENDED_AMOUNT) AS total_revenue
FROM \`${schema}\`.RA_CUSTOMER_TRX_ALL rcta
JOIN \`${schema}\`.RA_CUSTOMER_TRX_LINES_ALL rctl ON rcta.CUSTOMER_TRX_ID = rctl.CUSTOMER_TRX_ID
WHERE rctl.LINE_TYPE = 'LINE'
  AND rcta.TRX_DATE >= DATE_FORMAT(CURDATE(), '%Y-%m-01')`,
        objects: ['AR_Invoice']
      },
      {
        nl_query: 'Show me the AR aging report by customer',
        sql: `SELECT
  aps.CUSTOMER_ID,
  CASE
    WHEN DATEDIFF(CURDATE(), aps.DUE_DATE) <= 0 THEN 'Current'
    WHEN DATEDIFF(CURDATE(), aps.DUE_DATE) <= 30 THEN '1-30 Days'
    WHEN DATEDIFF(CURDATE(), aps.DUE_DATE) <= 60 THEN '31-60 Days'
    WHEN DATEDIFF(CURDATE(), aps.DUE_DATE) <= 90 THEN '61-90 Days'
    ELSE '90+ Days'
  END AS aging_bucket,
  COUNT(*) AS invoice_count,
  SUM(aps.AMOUNT_DUE_REMAINING) AS total_outstanding
FROM \`${schema}\`.AR_PAYMENT_SCHEDULES_ALL aps
WHERE aps.CLASS = 'INV'
  AND aps.AMOUNT_DUE_REMAINING > 0
GROUP BY aps.CUSTOMER_ID, aging_bucket
ORDER BY aps.CUSTOMER_ID, aging_bucket`,
        objects: ['AR_Payment_Schedule']
      },
      {
        nl_query: 'What is the total booking value by sales rep?',
        sql: `SELECT ooh.SALESREP_ID,
  COUNT(DISTINCT ooh.HEADER_ID) AS order_count,
  SUM(ool.ORDERED_QUANTITY * ool.UNIT_SELLING_PRICE) AS total_booking_value
FROM \`${schema}\`.OE_ORDER_HEADERS_ALL ooh
JOIN \`${schema}\`.OE_ORDER_LINES_ALL ool ON ooh.HEADER_ID = ool.HEADER_ID
WHERE ooh.BOOKED_FLAG = 'Y'
  AND ool.CANCELLED_FLAG = 'N'
GROUP BY ooh.SALESREP_ID
ORDER BY total_booking_value DESC`,
        objects: ['Sales_Order']
      },
      {
        nl_query: 'Show me all booked orders that have not been shipped yet',
        sql: `SELECT ooh.ORDER_NUMBER, ooh.BOOKED_DATE, ooh.SOLD_TO_ORG_ID,
  SUM(ool.ORDERED_QUANTITY) AS total_ordered,
  SUM(COALESCE(ool.SHIPPED_QUANTITY, 0)) AS total_shipped
FROM \`${schema}\`.OE_ORDER_HEADERS_ALL ooh
JOIN \`${schema}\`.OE_ORDER_LINES_ALL ool ON ooh.HEADER_ID = ool.HEADER_ID
WHERE ooh.BOOKED_FLAG = 'Y'
  AND ooh.OPEN_FLAG = 'Y'
  AND ool.CANCELLED_FLAG = 'N'
GROUP BY ooh.ORDER_NUMBER, ooh.BOOKED_DATE, ooh.SOLD_TO_ORG_ID
HAVING SUM(COALESCE(ool.SHIPPED_QUANTITY, 0)) < SUM(ool.ORDERED_QUANTITY)
ORDER BY ooh.BOOKED_DATE DESC
LIMIT 50`,
        objects: ['Sales_Order']
      },
      {
        nl_query: 'What is the total cash collected this quarter?',
        sql: `SELECT SUM(acr.AMOUNT) AS total_collections
FROM \`${schema}\`.AR_CASH_RECEIPTS_ALL acr
WHERE acr.STATUS = 'APP'
  AND acr.RECEIPT_DATE >= DATE_FORMAT(CURDATE(), '%Y-%m-01')`,
        objects: ['AR_Cash_Receipt']
      },
      {
        nl_query: 'Compare bookings vs revenue for the last 4 quarters',
        sql: `SELECT
  COALESCE(b.quarter, r.quarter) AS quarter,
  COALESCE(b.booking_value, 0) AS bookings,
  COALESCE(r.revenue_value, 0) AS revenue
FROM (
  SELECT DATE_FORMAT(ooh.BOOKED_DATE, '%Y-Q%q') AS quarter,
    SUM(ool.ORDERED_QUANTITY * ool.UNIT_SELLING_PRICE) AS booking_value
  FROM \`${schema}\`.OE_ORDER_HEADERS_ALL ooh
  JOIN \`${schema}\`.OE_ORDER_LINES_ALL ool ON ooh.HEADER_ID = ool.HEADER_ID
  WHERE ooh.BOOKED_FLAG = 'Y' AND ool.CANCELLED_FLAG = 'N'
    AND ooh.BOOKED_DATE >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
  GROUP BY DATE_FORMAT(ooh.BOOKED_DATE, '%Y-Q%q')
) b
LEFT JOIN (
  SELECT DATE_FORMAT(rcta.TRX_DATE, '%Y-Q%q') AS quarter,
    SUM(rctl.EXTENDED_AMOUNT) AS revenue_value
  FROM \`${schema}\`.RA_CUSTOMER_TRX_ALL rcta
  JOIN \`${schema}\`.RA_CUSTOMER_TRX_LINES_ALL rctl ON rcta.CUSTOMER_TRX_ID = rctl.CUSTOMER_TRX_ID
  WHERE rctl.LINE_TYPE = 'LINE'
    AND rcta.TRX_DATE >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
  GROUP BY DATE_FORMAT(rcta.TRX_DATE, '%Y-Q%q')
) r ON b.quarter = r.quarter
ORDER BY quarter`,
        objects: ['Sales_Order', 'AR_Invoice']
      },
    ];

    let seeded = 0;
    for (const t of templates) {
      const [existing] = await dbQuery(
        'SELECT id, generated_sql FROM test_queries WHERE app_id = ? AND nl_query = ?', [appId, t.nl_query]
      );
      if (existing.length > 0) {
        if (existing[0].generated_sql !== t.sql) {
          await dbQuery(
            'UPDATE test_queries SET generated_sql = ?, execution_result = ?, created_at = NOW() WHERE id = ?',
            [t.sql, JSON.stringify({ source: 'proven_template', objects: t.objects }), existing[0].id]
          );
          seeded++;
          console.log(`  ↻ Updated template: ${t.nl_query.substring(0, 50)}`);
        }
        continue;
      }

      await dbQuery(
        `INSERT INTO test_queries (app_id, user_id, nl_query, generated_sql, execution_result, confidence, feedback, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, NOW())`,
        [appId, userId, t.nl_query, t.sql,
         JSON.stringify({ source: 'proven_template', objects: t.objects }),
         0.95, 'thumbs_up']
      );
      seeded++;
    }
    if (seeded > 0) {
      console.log(`✓ Seeded ${seeded} proven SQL templates for OEBS demo queries`);
    } else {
      console.log('✓ Proven SQL templates already seeded');
    }
  } catch (err) {
    console.error('Proven template seeding failed:', err.message);
  }
}

// ─── BOKG: Enrich OEBS source data with sales orders + AR transactions ───
async function enrichOEBSSalesData() {
  try {
    const { query: dbQuery } = require('./db');

    const [appRows] = await dbQuery("SELECT id FROM applications WHERE name = 'Oracle E-Business Suite R12'");
    if (appRows.length === 0) return;
    const appId = appRows[0].id;
    const schema = `appdata_${appId}`;

    const [schemaCheck] = await dbQuery(
      `SELECT schema_name FROM information_schema.SCHEMATA WHERE schema_name = ?`, [schema]
    );
    if (schemaCheck.length === 0) {
      console.log('⏳ OEBS data schema not loaded yet — skipping sales enrichment');
      return;
    }

    // Check if we already enriched (sentinel: header_id 90001)
    const sentinel = await dbQuery(
      `SELECT 1 FROM \`${schema}\`.\`OE_ORDER_HEADERS_ALL\` WHERE HEADER_ID = 90001 LIMIT 1`
    );
    if (sentinel.length > 0) {
      console.log('✓ OEBS sales data already enriched');
      return;
    }

    console.log('📊 Enriching OEBS data with additional sales/booking/AR records...');

    // Get existing reference data
    const existingCustomers = await dbQuery(
      `SELECT DISTINCT SOLD_TO_ORG_ID FROM \`${schema}\`.\`OE_ORDER_HEADERS_ALL\` WHERE SOLD_TO_ORG_ID IS NOT NULL LIMIT 20`
    );
    const customerIds = existingCustomers.map(r => r.SOLD_TO_ORG_ID);
    if (customerIds.length === 0) {
      console.log('⚠ No existing customers found — skipping enrichment');
      return;
    }

    const existingSalesreps = await dbQuery(
      `SELECT DISTINCT SALESREP_ID FROM \`${schema}\`.\`OE_ORDER_HEADERS_ALL\` WHERE SALESREP_ID IS NOT NULL LIMIT 10`
    );
    const salesrepIds = existingSalesreps.map(r => r.SALESREP_ID);

    const existingItems = await dbQuery(
      `SELECT DISTINCT INVENTORY_ITEM_ID, ORDERED_ITEM, ORDER_QUANTITY_UOM, UNIT_LIST_PRICE
       FROM \`${schema}\`.\`OE_ORDER_LINES_ALL\`
       WHERE INVENTORY_ITEM_ID IS NOT NULL AND UNIT_LIST_PRICE IS NOT NULL
       LIMIT 30`
    );
    const items = existingItems;
    if (items.length === 0) {
      console.log('⚠ No existing items found — skipping enrichment');
      return;
    }

    const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
    const randBetween = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
    const months2025 = ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06',
                        '2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12'];
    const months2026 = ['2026-01', '2026-02', '2026-03'];

    // INSERT NEW BOOKED ORDERS (90001-90120)
    let headerIdCounter = 90001;
    let lineIdCounter = 900001;
    let orderNumber = 90001;
    let headerInserted = 0;
    let lineInserted = 0;

    const allMonths = [...months2025, ...months2026];
    for (const month of allMonths) {
      const ordersThisMonth = randBetween(8, 14);
      for (let i = 0; i < ordersThisMonth; i++) {
        const day = String(randBetween(1, 28)).padStart(2, '0');
        const bookedDate = `${month}-${day}`;
        const custId = pick(customerIds);
        const repId = salesrepIds.length > 0 ? pick(salesrepIds) : null;
        const headerId = headerIdCounter++;
        const orderNum = orderNumber++;

        const isOlderMonth = month < '2025-10';
        const flowStatus = isOlderMonth && Math.random() < 0.6 ? 'CLOSED' : 'BOOKED';
        const openFlag = flowStatus === 'CLOSED' ? 'N' : 'Y';

        await dbQuery(`INSERT INTO \`${schema}\`.\`OE_ORDER_HEADERS_ALL\`
          (HEADER_ID, ORDER_NUMBER, ORDERED_DATE, ORDER_TYPE_ID, SOLD_TO_ORG_ID,
           SALESREP_ID, TRANSACTIONAL_CURR_CODE, BOOKED_FLAG, BOOKED_DATE,
           FLOW_STATUS_CODE, OPEN_FLAG, CANCELLED_FLAG, ORG_ID,
           CREATION_DATE, CREATED_BY, LAST_UPDATE_DATE, LAST_UPDATED_BY)
          VALUES (?,?,?,1,?,?,'USD','Y',?,?,?,'N',204,?,1,?,1)`,
          [headerId, orderNum, bookedDate, custId, repId, bookedDate,
           flowStatus, openFlag, bookedDate, bookedDate]
        );
        headerInserted++;

        const lineCount = randBetween(1, 5);
        for (let ln = 1; ln <= lineCount; ln++) {
          const item = pick(items);
          const qty = randBetween(1, 50);
          const unitPrice = parseFloat(item.UNIT_LIST_PRICE) || randBetween(50, 500);
          const sellingPrice = +(unitPrice * (0.85 + Math.random() * 0.15)).toFixed(2);
          const shippedQty = flowStatus === 'CLOSED' ? qty : (Math.random() < 0.3 ? qty : 0);
          const invoicedQty = flowStatus === 'CLOSED' ? qty : 0;
          const lineStatus = flowStatus === 'CLOSED' ? 'CLOSED' : (shippedQty > 0 ? 'SHIPPED' : 'AWAITING_SHIPPING');

          await dbQuery(`INSERT INTO \`${schema}\`.\`OE_ORDER_LINES_ALL\`
            (LINE_ID, HEADER_ID, LINE_NUMBER, ORDERED_ITEM, INVENTORY_ITEM_ID,
             ITEM_TYPE_CODE, ORDERED_QUANTITY, SHIPPED_QUANTITY, INVOICED_QUANTITY,
             ORDER_QUANTITY_UOM, UNIT_SELLING_PRICE, UNIT_LIST_PRICE,
             LINE_CATEGORY_CODE, FLOW_STATUS_CODE, OPEN_FLAG, CANCELLED_FLAG,
             BOOKED_FLAG, ORG_ID, CREATION_DATE, CREATED_BY, LAST_UPDATE_DATE, LAST_UPDATED_BY)
            VALUES (?,?,?,?,?,'STANDARD',?,?,?,?,?,?,'ORDER',?,?,'N','Y',204,?,1,?,1)`,
            [lineIdCounter++, headerId, ln, item.ORDERED_ITEM, item.INVENTORY_ITEM_ID,
             qty, shippedQty, invoicedQty, item.ORDER_QUANTITY_UOM || 'Each',
             sellingPrice, unitPrice, lineStatus,
             flowStatus === 'CLOSED' ? 'N' : 'Y',
             bookedDate, bookedDate]
          );
          lineInserted++;
        }
      }
    }

    console.log(`  ✓ Inserted ${headerInserted} booked order headers, ${lineInserted} order lines`);

    // INSERT AR INVOICES for closed orders
    let trxIdCounter = 90001;
    let trxLineIdCounter = 900001;
    let trxInserted = 0;
    let trxLineInserted = 0;

    const closedOrders = await dbQuery(
      `SELECT h.HEADER_ID, h.ORDER_NUMBER, h.ORDERED_DATE, h.SOLD_TO_ORG_ID, h.SALESREP_ID
       FROM \`${schema}\`.\`OE_ORDER_HEADERS_ALL\` h
       WHERE h.HEADER_ID >= 90001 AND h.FLOW_STATUS_CODE = 'CLOSED'`
    );

    for (const order of closedOrders) {
      const trxId = trxIdCounter++;
      const trxDate = order.ORDERED_DATE;
      const orderDate = new Date(trxDate);
      orderDate.setDate(orderDate.getDate() + randBetween(5, 15));
      const invoiceDate = orderDate.toISOString().split('T')[0];

      await dbQuery(`INSERT INTO \`${schema}\`.\`RA_CUSTOMER_TRX_ALL\`
        (CUSTOMER_TRX_ID, TRX_NUMBER, TRX_DATE, CUST_TRX_TYPE_ID,
         BILL_TO_CUSTOMER_ID, INVOICE_CURRENCY_CODE, PRIMARY_SALESREP_ID,
         COMPLETE_FLAG, STATUS_TRX, ORG_ID, CREATION_DATE, CREATED_BY,
         LAST_UPDATE_DATE, LAST_UPDATED_BY)
        VALUES (?,?,?,1,?,'USD',?,'Y','OP',204,?,1,?,1)`,
        [trxId, `INV-${order.ORDER_NUMBER}`, invoiceDate, order.SOLD_TO_ORG_ID,
         order.SALESREP_ID, invoiceDate, invoiceDate]
      );
      trxInserted++;

      const orderLines = await dbQuery(
        `SELECT LINE_ID, LINE_NUMBER, ORDERED_ITEM, INVENTORY_ITEM_ID,
                INVOICED_QUANTITY, UNIT_SELLING_PRICE, ORDER_QUANTITY_UOM
         FROM \`${schema}\`.\`OE_ORDER_LINES_ALL\`
         WHERE HEADER_ID = ?`, [order.HEADER_ID]
      );

      for (const line of orderLines) {
        const qty = line.INVOICED_QUANTITY || line.ORDERED_QUANTITY || 1;
        const price = parseFloat(line.UNIT_SELLING_PRICE) || 100;
        const extAmount = +(qty * price).toFixed(2);

        await dbQuery(`INSERT INTO \`${schema}\`.\`RA_CUSTOMER_TRX_LINES_ALL\`
          (CUSTOMER_TRX_LINE_ID, CUSTOMER_TRX_ID, LINE_NUMBER, LINE_TYPE,
           INVENTORY_ITEM_ID, DESCRIPTION, QUANTITY_INVOICED,
           UNIT_SELLING_PRICE, EXTENDED_AMOUNT, REVENUE_AMOUNT,
           UOM_CODE, SALES_ORDER, SALES_ORDER_LINE,
           CREATION_DATE, CREATED_BY, LAST_UPDATE_DATE, LAST_UPDATED_BY)
          VALUES (?,?,?,'LINE',?,?,?,?,?,?,?,?,?,?,1,?,1)`,
          [trxLineIdCounter++, trxId, line.LINE_NUMBER, line.INVENTORY_ITEM_ID,
           line.ORDERED_ITEM, qty, price, extAmount, extAmount,
           line.ORDER_QUANTITY_UOM || 'Each',
           String(order.ORDER_NUMBER), String(line.LINE_NUMBER),
           invoiceDate, invoiceDate]
        );
        trxLineInserted++;
      }

      // Payment schedule entry
      const totalAmount = await dbQuery(
        `SELECT SUM(EXTENDED_AMOUNT) as total FROM \`${schema}\`.\`RA_CUSTOMER_TRX_LINES_ALL\`
         WHERE CUSTOMER_TRX_ID = ?`, [trxId]
      );
      const invoiceTotal = parseFloat(totalAmount[0]?.total) || 0;

      const dueDate = new Date(invoiceDate);
      dueDate.setDate(dueDate.getDate() + 30);
      const dueDateStr = dueDate.toISOString().split('T')[0];

      const isPaid = Math.random() < 0.7;
      const amountApplied = isPaid ? invoiceTotal : 0;
      const amountRemaining = isPaid ? 0 : invoiceTotal;
      const status = isPaid ? 'CL' : 'OP';

      await dbQuery(`INSERT INTO \`${schema}\`.\`AR_PAYMENT_SCHEDULES_ALL\`
        (PAYMENT_SCHEDULE_ID, CUSTOMER_TRX_ID, CUSTOMER_ID, CLASS,
         TRX_NUMBER, TRX_DATE, DUE_DATE,
         AMOUNT_DUE_ORIGINAL, AMOUNT_DUE_REMAINING, AMOUNT_APPLIED,
         STATUS, INVOICE_CURRENCY_CODE, ORG_ID,
         CREATION_DATE, CREATED_BY, LAST_UPDATE_DATE, LAST_UPDATED_BY)
        VALUES (?,?,?,'INV',?,?,?,?,?,?,?,'USD',204,?,1,?,1)`,
        [trxId, trxId, order.SOLD_TO_ORG_ID,
         `INV-${order.ORDER_NUMBER}`, invoiceDate, dueDateStr,
         invoiceTotal, amountRemaining, amountApplied,
         status, invoiceDate, invoiceDate]
      );

      if (isPaid) {
        const receiptDate = new Date(invoiceDate);
        receiptDate.setDate(receiptDate.getDate() + randBetween(10, 35));
        const receiptDateStr = receiptDate.toISOString().split('T')[0];

        await dbQuery(`INSERT INTO \`${schema}\`.\`AR_CASH_RECEIPTS_ALL\`
          (CASH_RECEIPT_ID, RECEIPT_NUMBER, RECEIPT_DATE, AMOUNT,
           CURRENCY_CODE, PAY_FROM_CUSTOMER, STATUS, TYPE,
           DEPOSIT_DATE, GL_DATE, ORG_ID,
           CREATION_DATE, CREATED_BY, LAST_UPDATE_DATE, LAST_UPDATED_BY)
          VALUES (?,?,?,?,'USD',?,'APP','STANDARD',?,?,204,?,1,?,1)`,
          [trxId, `RCT-${order.ORDER_NUMBER}`, receiptDateStr, invoiceTotal,
           order.SOLD_TO_ORG_ID, receiptDateStr, receiptDateStr,
           receiptDateStr, receiptDateStr]
        );
      }
    }

    console.log(`  ✓ Inserted ${trxInserted} AR invoices, ${trxLineInserted} invoice lines`);
    console.log('✓ OEBS sales data enrichment complete');

  } catch (err) {
    console.error('OEBS sales data enrichment failed:', err.message);
  }
}

// ─── BOKG: One-time OFBiz domain reclassification ───
async function reclassifyOFBizDomains() {
  try {
    const { query: dbQuery } = require('./db');

    const [appRows] = await dbQuery("SELECT id FROM applications WHERE name LIKE '%OFBiz%' LIMIT 1");
    if (appRows.length === 0) return;
    const appId = appRows[0].id;

    const [checkRows] = await dbQuery(
      `SELECT COUNT(*) as cnt FROM app_tables WHERE app_id = ? AND (
        JSON_UNQUOTE(JSON_EXTRACT(entity_metadata, '$.domain')) IS NULL OR JSON_UNQUOTE(JSON_EXTRACT(entity_metadata, '$.domain')) = '' OR JSON_UNQUOTE(JSON_EXTRACT(entity_metadata, '$.domain')) = 'Unclassified'
      )`, [appId]
    );
    const unclassifiedCount = parseInt(checkRows[0].cnt);
    if (unclassifiedCount < 50) {
      console.log(`✓ OFBiz domains OK (${unclassifiedCount} unclassified)`);
      return;
    }

    console.log(`🔄 Reclassifying ${unclassifiedCount} unclassified OFBiz tables...`);

    const CANONICAL = {
      'Order Management': {
        prefixes: ['order_', 'cart_', 'quote_', 'requirement', 'shopping_list', 'allocation_plan', 'return_'],
        existing: ['order management', 'order management - returns', 'order management & fulfillment',
          'order management / fulfillment', 'order management / shipping', 'order management / logistics',
          'order management & inventory', 'quote management', 'requirements management', 'e-commerce shopping lists']
      },
      'Product Management': {
        prefixes: ['product_', 'prod_', 'good_identification', 'config_option', 'desired_feature', 'supplier_'],
        existing: ['product catalog management', 'product management', 'product store management',
          'product pricing', 'product accounting', 'product configuration', 'product costing',
          'product management - maintenance', 'product promotion management', 'promotion management',
          'e-commerce store management', 'e-commerce marketing']
      },
      'Party & Contact Management': {
        prefixes: ['party_', 'person_', 'contact_mech', 'contact_list', 'postal_address', 'telecom_number',
          'telecom_gateway', 'telecom_method', 'email_address', 'address_match', 'valid_contact',
          'communication_event', 'comm_content'],
        existing: ['party management', 'contact management', 'communication management',
          'party communication', 'marketing & communications', 'marketing communications', 'marketing/communications']
      },
      'Financial Management': {
        prefixes: ['acctg_trans', 'gl_', 'fin_account', 'budget', 'settlement_term', 'variance_reason',
          'custom_time_period', 'period_type'],
        existing: ['financial management', 'accounting', 'general ledger', 'accounting/invoicing',
          'accounting/financial management', 'accounting/billing', 'accounting - general ledger',
          'general ledger / accounting', 'budgeting & financial planning', 'accounting configuration',
          'financial management - budgeting', 'accounting/costing']
      },
      'Invoice & Billing': {
        prefixes: ['invoice_', 'billing_account'],
        existing: ['billing management', 'invoice management']
      },
      'Payment Processing': {
        prefixes: ['payment_', 'credit_card', 'eft_account', 'gift_card', 'pay_pal', 'check_account', 'value_link'],
        existing: ['payment gateway configuration', 'payment management', 'payment processing', 'gift card management']
      },
      'Inventory Management': {
        prefixes: ['inventory_item', 'inventory_transfer', 'physical_inventory', 'lot', 'item_issuance'],
        existing: ['inventory management']
      },
      'Shipping & Logistics': {
        prefixes: ['shipment_', 'carrier_', 'picklist', 'delivery_', 'shipping_', 'tracking_code'],
        existing: ['shipment management', 'shipping gateway configuration', 'logistics/shipping',
          'supply chain management', 'logistics/fulfillment', 'e-commerce shipping',
          'shipping & logistics', 'logistics and shipping', 'order management / shipping']
      },
      'Facility Management': {
        prefixes: ['facility_', 'container', 'container_'],
        existing: ['facility management']
      },
      'Human Resources': {
        prefixes: ['empl_', 'employment', 'job_interview', 'pay_grade', 'pay_history', 'salary_step',
          'perf_review', 'performance_note', 'benefit_type', 'party_benefit', 'training_',
          'person_training', 'unemployment', 'termination_type', 'responsibility_type',
          'valid_responsibility', 'skill_type', 'party_skill', 'party_qual', 'party_resume',
          'rejection_reason', 'deduction'],
        existing: ['human resources', 'human resources - compensation', 'human resources - payroll',
          'human resources - performance management', 'human resources & billing', 'human resources - work management']
      },
      'Content Management': {
        prefixes: ['content_', 'content', 'data_resource', 'audio_data', 'video_data', 'image_data',
          'electronic_text', 'document', 'java_resource', 'file_extension', 'mime_type',
          'character_set', 'meta_data', 'keyword_thesaurus'],
        existing: ['content management', 'data management', 'content management / theming']
      },
      'Marketing & Sales': {
        prefixes: ['marketing_', 'market_interest', 'sales_forecast', 'sales_opportunity', 'segment_group',
          'tracking_code', 'web_analytics'],
        existing: ['sales management', 'sales management / crm', 'crm/sales management',
          'marketing & partnerships', 'marketing management', 'marketing campaign management',
          'customer relationship management', 'customer service management', 'customer service/crm', 'web analytics management']
      },
      'Manufacturing & Work Management': {
        prefixes: ['work_effort', 'work_order', 'work_req', 'tech_data', 'mrp_event', 'timesheet',
          'time_entry', 'cost_component', 'fixed_asset', 'component', 'deliverable'],
        existing: ['work effort management', 'work management', 'manufacturing & work management',
          'manufacturing/production', 'manufacturing/scheduling', 'manufacturing & costing',
          'manufacturing cost accounting', 'asset management', 'fixed asset management']
      },
      'Tax & Compliance': {
        prefixes: ['tax_authority', 'zip_sales_tax'],
        existing: ['tax management']
      },
      'Survey & Feedback': {
        prefixes: ['survey_', 'survey', 'cust_request'],
        existing: ['survey management']
      },
      'Agreement & Contract Management': {
        prefixes: ['agreement_', 'agreement', 'addendum', 'term_type'],
        existing: ['agreement management', 'terms and conditions management']
      },
      'Subscription Management': {
        prefixes: ['subscription_'],
        existing: ['subscription management', 'party management / subscription services']
      },
      'System & Reference Data': {
        prefixes: ['enumeration', 'status_', 'uom', 'geo', 'geo_', 'standard_language', 'country_',
          'note_data', 'sequence_value', 'entity_key', 'system_property', 'custom_method',
          'custom_screen', 'user_pref', 'web_preference', 'data_source', 'data_template',
          'application_sandbox', 'tenant', 'role_type', 'priority_type', 'quantity_break',
          'old_', 'responding_party', 'portal_', 'portlet_', 'visual_theme', 'web_site',
          'web_user', 'sale_type', 'marital_status', 'accommodation_', 'email_template', 'vendor'],
        existing: ['system configuration', 'system administration', 'common/reference data',
          'user interface management', 'portal management', 'website management',
          'website security & access control', 'integration/file transfer', 'e-commerce store management']
      }
    };

    const nameMap = {};
    for (const [canonical, config] of Object.entries(CANONICAL)) {
      for (const ex of config.existing) nameMap[ex] = canonical;
    }

    const [allTables] = await dbQuery('SELECT id, table_name, entity_metadata FROM app_tables WHERE app_id = ?', [appId]);
    let reclassified = 0;
    const stats = {};

    for (const row of allTables) {
      const meta = typeof row.entity_metadata === 'string' ? JSON.parse(row.entity_metadata || '{}') : (row.entity_metadata || {});
      const current = (meta.domain || meta.module || 'Unclassified').toLowerCase();
      const tableName = row.table_name.toLowerCase();
      let newDomain = null;

      if (nameMap[current]) newDomain = nameMap[current];

      if (!newDomain) {
        for (const [canonical, config] of Object.entries(CANONICAL)) {
          for (const prefix of config.prefixes) {
            if (tableName.startsWith(prefix) || tableName === prefix) { newDomain = canonical; break; }
          }
          if (newDomain) break;
        }
      }

      if (!newDomain) {
        const titleCurrent = meta.domain || meta.module || 'Unclassified';
        newDomain = CANONICAL[titleCurrent] ? titleCurrent : 'System & Reference Data';
      }

      stats[newDomain] = (stats[newDomain] || 0) + 1;
      const oldDomain = meta.domain || meta.module || 'Unclassified';
      if (newDomain !== oldDomain) {
        meta.domain = newDomain;
        meta._previous_domain = oldDomain;
        await dbQuery('UPDATE app_tables SET entity_metadata = ? WHERE id = ?', [JSON.stringify(meta), row.id]);
        reclassified++;
      }
    }

    const domainCount = Object.keys(stats).length;
    const sorted = Object.entries(stats).sort((a, b) => b[1] - a[1]);
    console.log(`✓ OFBiz domains reclassified: ${reclassified} tables → ${domainCount} domains`);
    for (const [d, c] of sorted) console.log(`    ${String(c).padStart(4)}  ${d}`);
  } catch (err) {
    console.error('OFBiz domain reclassification failed:', err.message);
  }
}

// ─── Ensure uploads directory exists ───
function ensureDirectories() {
  const uploadDir = path.join(__dirname, '..', 'uploads');
  if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir, { recursive: true });
    console.log('✓ Uploads directory created');
  }
}

// ═══════════════════════════════════════════════════════════════
// ─── Global Error Handlers ───
// Prevent unhandled errors from silently crashing the process
process.on('unhandledRejection', (reason, promise) => {
  console.error('[FATAL] Unhandled Promise Rejection:', reason);
  console.error('[FATAL] Stack:', reason?.stack?.split('\n').slice(0, 8).join('\n'));
});
// ─── Seed Global Synonyms — Healthcare Domain Pack ───
async function seedGlobalSynonyms() {
  try {
    const { query: dbQuery } = require('./db');

    // Check if already seeded
    const [existingRows] = await dbQuery('SELECT COUNT(*) as c FROM global_synonyms');
    if (parseInt(existingRows[0].c) > 10) {
      console.log(`✓ Global synonyms already seeded (${existingRows[0].c} terms)`);
      return;
    }

    // Healthcare domain pack — realistic medical terminology from thrombosis/clinical data
    const healthcarePack = [
      // Lab values — Liver function
      { term: 'aspartate aminotransferase', canonical_name: 'GOT', category: 'Liver Function' },
      { term: 'AST', canonical_name: 'GOT', category: 'Liver Function' },
      { term: 'SGOT', canonical_name: 'GOT', category: 'Liver Function' },
      { term: 'alanine aminotransferase', canonical_name: 'GPT', category: 'Liver Function' },
      { term: 'ALT', canonical_name: 'GPT', category: 'Liver Function' },
      { term: 'SGPT', canonical_name: 'GPT', category: 'Liver Function' },
      { term: 'lactate dehydrogenase', canonical_name: 'LDH', category: 'Tissue Damage' },
      { term: 'alkaline phosphatase', canonical_name: 'ALP', category: 'Liver Function' },
      { term: 'total bilirubin', canonical_name: 'T-BIL', category: 'Liver Function' },
      { term: 'bilirubin', canonical_name: 'T-BIL', category: 'Liver Function' },
      { term: 'serum bilirubin', canonical_name: 'T-BIL', category: 'Liver Function' },

      // Lab values — Blood proteins
      { term: 'total protein', canonical_name: 'TP', category: 'Blood Chemistry' },
      { term: 'serum protein', canonical_name: 'TP', category: 'Blood Chemistry' },
      { term: 'albumin', canonical_name: 'ALB', category: 'Blood Chemistry' },
      { term: 'serum albumin', canonical_name: 'ALB', category: 'Blood Chemistry' },

      // Lab values — Kidney function
      { term: 'uric acid', canonical_name: 'UA', category: 'Kidney Function' },
      { term: 'urea nitrogen', canonical_name: 'UN', category: 'Kidney Function' },
      { term: 'BUN', canonical_name: 'UN', category: 'Kidney Function' },
      { term: 'blood urea nitrogen', canonical_name: 'UN', category: 'Kidney Function' },
      { term: 'creatinine', canonical_name: 'CRE', category: 'Kidney Function' },
      { term: 'serum creatinine', canonical_name: 'CRE', category: 'Kidney Function' },

      // Lab values — Lipid panel
      { term: 'total cholesterol', canonical_name: 'T-CHO', category: 'Lipid Panel' },
      { term: 'cholesterol', canonical_name: 'T-CHO', category: 'Lipid Panel' },
      { term: 'triglycerides', canonical_name: 'TG', category: 'Lipid Panel' },
      { term: 'triglyceride level', canonical_name: 'TG', category: 'Lipid Panel' },

      // Lab values — CBC (Complete Blood Count)
      { term: 'white blood cells', canonical_name: 'WBC', category: 'CBC' },
      { term: 'white blood cell count', canonical_name: 'WBC', category: 'CBC' },
      { term: 'leukocytes', canonical_name: 'WBC', category: 'CBC' },
      { term: 'red blood cells', canonical_name: 'RBC', category: 'CBC' },
      { term: 'red blood cell count', canonical_name: 'RBC', category: 'CBC' },
      { term: 'erythrocytes', canonical_name: 'RBC', category: 'CBC' },
      { term: 'hemoglobin', canonical_name: 'HGB', category: 'CBC' },
      { term: 'blood hemoglobin', canonical_name: 'HGB', category: 'CBC' },
      { term: 'hematocrit', canonical_name: 'HCT', category: 'CBC' },
      { term: 'platelets', canonical_name: 'PLT', category: 'CBC' },
      { term: 'platelet count', canonical_name: 'PLT', category: 'CBC' },
      { term: 'thrombocytes', canonical_name: 'PLT', category: 'CBC' },

      // Lab values — Coagulation
      { term: 'prothrombin time', canonical_name: 'PT', category: 'Coagulation' },
      { term: 'activated partial thromboplastin time', canonical_name: 'APTT', category: 'Coagulation' },
      { term: 'aPTT', canonical_name: 'APTT', category: 'Coagulation' },
      { term: 'fibrinogen', canonical_name: 'FG', category: 'Coagulation' },
      { term: 'fibrinogen level', canonical_name: 'FG', category: 'Coagulation' },
      { term: 'thrombin-antithrombin complex', canonical_name: 'TAT', category: 'Coagulation' },

      // Lab values — Metabolic
      { term: 'glucose', canonical_name: 'GLU', category: 'Metabolic' },
      { term: 'blood glucose', canonical_name: 'GLU', category: 'Metabolic' },
      { term: 'blood sugar', canonical_name: 'GLU', category: 'Metabolic' },
      { term: 'fasting glucose', canonical_name: 'GLU', category: 'Metabolic' },
      { term: 'creatine kinase', canonical_name: 'CPK', category: 'Muscle Enzyme' },
      { term: 'creatinine phosphokinase', canonical_name: 'CPK', category: 'Muscle Enzyme' },

      // Lab values — Immunology
      { term: 'immunoglobulin G', canonical_name: 'IGG', category: 'Immunology' },
      { term: 'IgG', canonical_name: 'IGG', category: 'Immunology' },
      { term: 'immunoglobulin A', canonical_name: 'IGA', category: 'Immunology' },
      { term: 'IgA', canonical_name: 'IGA', category: 'Immunology' },
      { term: 'immunoglobulin M', canonical_name: 'IGM', category: 'Immunology' },
      { term: 'IgM', canonical_name: 'IGM', category: 'Immunology' },
      { term: 'C-reactive protein', canonical_name: 'CRP', category: 'Inflammation' },
      { term: 'CRP level', canonical_name: 'CRP', category: 'Inflammation' },

      // Lab values — Complement
      { term: 'complement 3', canonical_name: 'C3', category: 'Complement System' },
      { term: 'complement C3', canonical_name: 'C3', category: 'Complement System' },
      { term: 'complement 4', canonical_name: 'C4', category: 'Complement System' },
      { term: 'complement C4', canonical_name: 'C4', category: 'Complement System' },

      // Autoimmune antibodies
      { term: 'anti-ribonuclear protein', canonical_name: 'RNP', category: 'Autoimmune Antibody' },
      { term: 'anti-RNP', canonical_name: 'RNP', category: 'Autoimmune Antibody' },
      { term: 'anti-Smith antibody', canonical_name: 'SM', category: 'Autoimmune Antibody' },
      { term: 'anti-SM', canonical_name: 'SM', category: 'Autoimmune Antibody' },
      { term: 'anti-Scl-70', canonical_name: 'SC170', category: 'Autoimmune Antibody' },
      { term: 'anti-topoisomerase', canonical_name: 'SC170', category: 'Autoimmune Antibody' },
      { term: 'anti-SSA', canonical_name: 'SSA', category: 'Autoimmune Antibody' },
      { term: 'anti-Ro', canonical_name: 'SSA', category: 'Autoimmune Antibody' },
      { term: 'anti-SSB', canonical_name: 'SSB', category: 'Autoimmune Antibody' },
      { term: 'anti-La', canonical_name: 'SSB', category: 'Autoimmune Antibody' },
      { term: 'anti-centromere', canonical_name: 'CENTROMEA', category: 'Autoimmune Antibody' },
      { term: 'anti-DNA', canonical_name: 'DNA', category: 'Autoimmune Antibody' },
      { term: 'anti-dsDNA', canonical_name: 'DNA', category: 'Autoimmune Antibody' },
      { term: 'anti-double stranded DNA', canonical_name: 'DNA', category: 'Autoimmune Antibody' },

      // Examination table
      { term: 'anticardiolipin IgG', canonical_name: 'aCL IgG', category: 'APS Marker' },
      { term: 'anticardiolipin IgM', canonical_name: 'aCL IgM', category: 'APS Marker' },
      { term: 'antinuclear antibody', canonical_name: 'ANA', category: 'Autoimmune Screening' },
      { term: 'ANA titer', canonical_name: 'ANA', category: 'Autoimmune Screening' },
      { term: 'antinuclear antibody pattern', canonical_name: 'ANA Pattern', category: 'Autoimmune Screening' },
      { term: 'kaolin clotting time', canonical_name: 'KCT', category: 'Coagulation' },
      { term: 'Russell viper venom time', canonical_name: 'RVVT', category: 'Coagulation' },
      { term: 'lupus anticoagulant', canonical_name: 'LAC', category: 'Coagulation' },
      { term: 'thrombosis degree', canonical_name: 'Thrombosis', category: 'Clinical Outcome' },
      { term: 'degree of thrombosis', canonical_name: 'Thrombosis', category: 'Clinical Outcome' },

      // Patient table
      { term: 'admission status', canonical_name: 'Admission', category: 'Patient Admin' },
      { term: 'treatment type', canonical_name: 'Admission', category: 'Patient Admin' },
      { term: 'first hospital visit date', canonical_name: 'Description', category: 'Patient Admin' },
      { term: 'registration date', canonical_name: 'Description', category: 'Patient Admin' },
      { term: 'first examination date', canonical_name: 'First Date', category: 'Patient Admin' },
      { term: 'first visit date', canonical_name: 'First Date', category: 'Patient Admin' },

      // Diagnosis abbreviations (general healthcare)
      { term: 'Systemic Lupus Erythematosus', canonical_name: 'SLE', category: 'Diagnosis' },
      { term: 'Antiphospholipid Syndrome', canonical_name: 'APS', category: 'Diagnosis' },
      { term: 'Rheumatoid Arthritis', canonical_name: 'RA', category: 'Diagnosis' },
      { term: 'Mixed Connective Tissue Disease', canonical_name: 'MCTD', category: 'Diagnosis' },
      { term: 'rheumatoid factor', canonical_name: 'RF', category: 'Autoimmune' },
      { term: 'urine protein', canonical_name: 'U-PRO', category: 'Urinalysis' },
      { term: 'proteinuria', canonical_name: 'U-PRO', category: 'Urinalysis' },
    ];

    // Universal ERP/Finance terms (global — no domain_pack)
    const globalTerms = [
      { term: 'purchase order', canonical_name: 'PO', category: 'Procurement' },
      { term: 'PO number', canonical_name: 'PO', category: 'Procurement' },
      { term: 'vendor', canonical_name: 'supplier', category: 'Procurement' },
      { term: 'supplier code', canonical_name: 'supplier', category: 'Procurement' },
      { term: 'general ledger account', canonical_name: 'GL account', category: 'Financial' },
      { term: 'G/L account', canonical_name: 'GL account', category: 'Financial' },
      { term: 'account number', canonical_name: 'GL account', category: 'Financial' },
      { term: 'standard cost', canonical_name: 'unit cost', category: 'Financial' },
      { term: 'item cost', canonical_name: 'unit cost', category: 'Financial' },
      { term: 'product cost', canonical_name: 'unit cost', category: 'Financial' },
      { term: 'on hand', canonical_name: 'on-hand quantity', category: 'Inventory' },
      { term: 'available inventory', canonical_name: 'on-hand quantity', category: 'Inventory' },
      { term: 'stock level', canonical_name: 'on-hand quantity', category: 'Inventory' },
      { term: 'cost center', canonical_name: 'cost center', category: 'Financial' },
      { term: 'department code', canonical_name: 'cost center', category: 'Financial' },
      { term: 'employee', canonical_name: 'associate', category: 'HR' },
      { term: 'staff member', canonical_name: 'associate', category: 'HR' },
      { term: 'headcount', canonical_name: 'FTE', category: 'HR' },
      { term: 'full-time equivalent', canonical_name: 'FTE', category: 'HR' },
    ];

    let inserted = 0;
    // Insert healthcare pack
    for (const s of healthcarePack) {
      try {
        await dbQuery(
          `INSERT IGNORE INTO global_synonyms (term, canonical_name, category, domain_pack, description)
           VALUES (?, ?, ?, 'healthcare', ?)`,
          [s.term, s.canonical_name, s.category, `Healthcare synonym: ${s.term} → ${s.canonical_name}`]
        );
        inserted++;
      } catch (e) { /* skip duplicates */ }
    }
    // Insert global terms
    for (const s of globalTerms) {
      try {
        await dbQuery(
          `INSERT IGNORE INTO global_synonyms (term, canonical_name, category, domain_pack, description)
           VALUES (?, ?, ?, NULL, ?)`,
          [s.term, s.canonical_name, s.category, `Global synonym: ${s.term} → ${s.canonical_name}`]
        );
        inserted++;
      } catch (e) { /* skip duplicates */ }
    }

    // ─── ERP Domain Pack (Phase 1: AP, AR, GL, PO, OM — 210 terms) ───
    const erpPack = [
      // ACCOUNTS PAYABLE
      { term: 'invoice', canonical_name: 'AP_INVOICES_ALL', category: 'Accounts Payable' },
      { term: 'bill', canonical_name: 'AP_INVOICES_ALL', category: 'Accounts Payable' },
      { term: 'vendor bill', canonical_name: 'AP_INVOICES_ALL', category: 'Accounts Payable' },
      { term: 'invoice number', canonical_name: 'INVOICE_NUM', category: 'Accounts Payable' },
      { term: 'bill number', canonical_name: 'INVOICE_NUM', category: 'Accounts Payable' },
      { term: 'invoice amount', canonical_name: 'INVOICE_AMOUNT', category: 'Accounts Payable' },
      { term: 'invoice total', canonical_name: 'INVOICE_AMOUNT', category: 'Accounts Payable' },
      { term: 'bill amount', canonical_name: 'INVOICE_AMOUNT', category: 'Accounts Payable' },
      { term: 'how much was billed', canonical_name: 'INVOICE_AMOUNT', category: 'Accounts Payable' },
      { term: 'billed amount', canonical_name: 'INVOICE_AMOUNT', category: 'Accounts Payable' },
      { term: 'gross amount', canonical_name: 'INVOICE_AMOUNT', category: 'Accounts Payable' },
      { term: 'tax amount', canonical_name: 'TAX_AMOUNT', category: 'Accounts Payable' },
      { term: 'sales tax', canonical_name: 'TAX_AMOUNT', category: 'Accounts Payable' },
      { term: 'freight charge', canonical_name: 'FREIGHT_AMOUNT', category: 'Accounts Payable' },
      { term: 'shipping cost', canonical_name: 'FREIGHT_AMOUNT', category: 'Accounts Payable' },
      { term: 'payment status', canonical_name: 'PAYMENT_STATUS_FLAG', category: 'Accounts Payable' },
      { term: 'paid', canonical_name: 'PAYMENT_STATUS_FLAG', category: 'Accounts Payable' },
      { term: 'unpaid', canonical_name: 'PAYMENT_STATUS_FLAG', category: 'Accounts Payable' },
      { term: 'is paid', canonical_name: 'PAYMENT_STATUS_FLAG', category: 'Accounts Payable' },
      { term: 'payment method', canonical_name: 'PAYMENT_METHOD_CODE', category: 'Accounts Payable' },
      { term: 'how was it paid', canonical_name: 'PAYMENT_METHOD_CODE', category: 'Accounts Payable' },
      { term: 'check', canonical_name: 'AP_CHECKS_ALL', category: 'Accounts Payable' },
      { term: 'payment', canonical_name: 'AP_CHECKS_ALL', category: 'Accounts Payable' },
      { term: 'check number', canonical_name: 'CHECK_NUMBER', category: 'Accounts Payable' },
      { term: 'payment number', canonical_name: 'CHECK_NUMBER', category: 'Accounts Payable' },
      { term: 'payment amount', canonical_name: 'AMOUNT', category: 'Accounts Payable' },
      { term: 'amount paid', canonical_name: 'AMOUNT_PAID', category: 'Accounts Payable' },
      { term: 'how much was paid', canonical_name: 'AMOUNT_PAID', category: 'Accounts Payable' },
      { term: 'vendor', canonical_name: 'AP_SUPPLIERS', category: 'Accounts Payable' },
      { term: 'supplier', canonical_name: 'AP_SUPPLIERS', category: 'Accounts Payable' },
      { term: 'vendor name', canonical_name: 'VENDOR_NAME', category: 'Accounts Payable' },
      { term: 'supplier name', canonical_name: 'VENDOR_NAME', category: 'Accounts Payable' },
      { term: 'who was the supplier', canonical_name: 'VENDOR_NAME', category: 'Accounts Payable' },
      { term: 'vendor id', canonical_name: 'VENDOR_ID', category: 'Accounts Payable' },
      { term: 'supplier id', canonical_name: 'VENDOR_ID', category: 'Accounts Payable' },
      { term: 'invoice date', canonical_name: 'INVOICE_DATE', category: 'Accounts Payable' },
      { term: 'when was it invoiced', canonical_name: 'INVOICE_DATE', category: 'Accounts Payable' },
      { term: 'billing date', canonical_name: 'INVOICE_DATE', category: 'Accounts Payable' },
      { term: 'payment date', canonical_name: 'CHECK_DATE', category: 'Accounts Payable' },
      { term: 'when was it paid', canonical_name: 'CHECK_DATE', category: 'Accounts Payable' },
      { term: 'gl date', canonical_name: 'GL_DATE', category: 'Accounts Payable' },
      { term: 'invoice line', canonical_name: 'AP_INVOICE_LINES_ALL', category: 'Accounts Payable' },
      { term: 'line item', canonical_name: 'AP_INVOICE_LINES_ALL', category: 'Accounts Payable' },
      { term: 'invoice type', canonical_name: 'INVOICE_TYPE_LOOKUP_CODE', category: 'Accounts Payable' },
      { term: 'discount taken', canonical_name: 'DISCOUNT_AMOUNT_TAKEN', category: 'Accounts Payable' },
      { term: 'early payment discount', canonical_name: 'DISCOUNT_AMOUNT_TAKEN', category: 'Accounts Payable' },
      { term: 'voucher number', canonical_name: 'VOUCHER_NUM', category: 'Accounts Payable' },
      { term: 'approval status', canonical_name: 'APPROVAL_STATUS', category: 'Accounts Payable' },
      { term: 'workflow status', canonical_name: 'WFAPPROVAL_STATUS', category: 'Accounts Payable' },
      // ACCOUNTS RECEIVABLE
      { term: 'customer transaction', canonical_name: 'RA_CUSTOMER_TRX_ALL', category: 'Accounts Receivable' },
      { term: 'ar transaction', canonical_name: 'RA_CUSTOMER_TRX_ALL', category: 'Accounts Receivable' },
      { term: 'transaction number', canonical_name: 'TRX_NUMBER', category: 'Accounts Receivable' },
      { term: 'transaction date', canonical_name: 'TRX_DATE', category: 'Accounts Receivable' },
      { term: 'customer invoice', canonical_name: 'RA_CUSTOMER_TRX_ALL', category: 'Accounts Receivable' },
      { term: 'cash receipt', canonical_name: 'AR_CASH_RECEIPTS_ALL', category: 'Accounts Receivable' },
      { term: 'receipt', canonical_name: 'AR_CASH_RECEIPTS_ALL', category: 'Accounts Receivable' },
      { term: 'receipt number', canonical_name: 'RECEIPT_NUMBER', category: 'Accounts Receivable' },
      { term: 'receipt amount', canonical_name: 'AMOUNT', category: 'Accounts Receivable' },
      { term: 'cash received', canonical_name: 'AMOUNT', category: 'Accounts Receivable' },
      { term: 'how much was received', canonical_name: 'AMOUNT', category: 'Accounts Receivable' },
      { term: 'receipt date', canonical_name: 'RECEIPT_DATE', category: 'Accounts Receivable' },
      { term: 'when was payment received', canonical_name: 'RECEIPT_DATE', category: 'Accounts Receivable' },
      { term: 'deposit date', canonical_name: 'DEPOSIT_DATE', category: 'Accounts Receivable' },
      { term: 'receipt status', canonical_name: 'STATUS', category: 'Accounts Receivable' },
      { term: 'customer id', canonical_name: 'PAY_FROM_CUSTOMER', category: 'Accounts Receivable' },
      { term: 'who is the customer', canonical_name: 'BILL_TO_CUSTOMER_ID', category: 'Accounts Receivable' },
      { term: 'bill to customer', canonical_name: 'BILL_TO_CUSTOMER_ID', category: 'Accounts Receivable' },
      { term: 'ship to customer', canonical_name: 'SHIP_TO_CUSTOMER_ID', category: 'Accounts Receivable' },
      { term: 'receipt currency', canonical_name: 'CURRENCY_CODE', category: 'Accounts Receivable' },
      { term: 'invoice currency', canonical_name: 'INVOICE_CURRENCY_CODE', category: 'Accounts Receivable' },
      { term: 'payment application', canonical_name: 'AR_RECEIVABLE_APPLICATIONS_ALL', category: 'Accounts Receivable' },
      { term: 'applied receipt', canonical_name: 'AR_RECEIVABLE_APPLICATIONS_ALL', category: 'Accounts Receivable' },
      { term: 'payment schedule', canonical_name: 'AR_PAYMENT_SCHEDULES_ALL', category: 'Accounts Receivable' },
      { term: 'amount due', canonical_name: 'AR_PAYMENT_SCHEDULES_ALL', category: 'Accounts Receivable' },
      { term: 'salesperson', canonical_name: 'PRIMARY_SALESREP_ID', category: 'Accounts Receivable' },
      { term: 'sales rep', canonical_name: 'PRIMARY_SALESREP_ID', category: 'Accounts Receivable' },
      { term: 'customer po number', canonical_name: 'PURCHASE_ORDER', category: 'Accounts Receivable' },
      // GENERAL LEDGER
      { term: 'journal entry', canonical_name: 'GL_JE_HEADERS', category: 'General Ledger' },
      { term: 'journal', canonical_name: 'GL_JE_HEADERS', category: 'General Ledger' },
      { term: 'je', canonical_name: 'GL_JE_HEADERS', category: 'General Ledger' },
      { term: 'journal name', canonical_name: 'NAME', category: 'General Ledger' },
      { term: 'journal entry name', canonical_name: 'NAME', category: 'General Ledger' },
      { term: 'journal line', canonical_name: 'GL_JE_LINES', category: 'General Ledger' },
      { term: 'je line', canonical_name: 'GL_JE_LINES', category: 'General Ledger' },
      { term: 'debit', canonical_name: 'ENTERED_DR', category: 'General Ledger' },
      { term: 'debit amount', canonical_name: 'ENTERED_DR', category: 'General Ledger' },
      { term: 'credit', canonical_name: 'ENTERED_CR', category: 'General Ledger' },
      { term: 'credit amount', canonical_name: 'ENTERED_CR', category: 'General Ledger' },
      { term: 'accounted debit', canonical_name: 'ACCOUNTED_DR', category: 'General Ledger' },
      { term: 'accounted credit', canonical_name: 'ACCOUNTED_CR', category: 'General Ledger' },
      { term: 'gl account', canonical_name: 'CODE_COMBINATION_ID', category: 'General Ledger' },
      { term: 'chart of accounts', canonical_name: 'CODE_COMBINATION_ID', category: 'General Ledger' },
      { term: 'account code', canonical_name: 'CODE_COMBINATION_ID', category: 'General Ledger' },
      { term: 'company segment', canonical_name: 'SEGMENT1', category: 'General Ledger' },
      { term: 'cost center', canonical_name: 'SEGMENT2', category: 'General Ledger' },
      { term: 'department', canonical_name: 'SEGMENT2', category: 'General Ledger' },
      { term: 'natural account', canonical_name: 'SEGMENT3', category: 'General Ledger' },
      { term: 'accounting period', canonical_name: 'PERIOD_NAME', category: 'General Ledger' },
      { term: 'fiscal period', canonical_name: 'PERIOD_NAME', category: 'General Ledger' },
      { term: 'gl period', canonical_name: 'PERIOD_NAME', category: 'General Ledger' },
      { term: 'accounting date', canonical_name: 'EFFECTIVE_DATE', category: 'General Ledger' },
      { term: 'je date', canonical_name: 'EFFECTIVE_DATE', category: 'General Ledger' },
      { term: 'posted date', canonical_name: 'POSTED_DATE', category: 'General Ledger' },
      { term: 'je source', canonical_name: 'JE_SOURCE', category: 'General Ledger' },
      { term: 'journal source', canonical_name: 'JE_SOURCE', category: 'General Ledger' },
      { term: 'je category', canonical_name: 'JE_CATEGORY', category: 'General Ledger' },
      { term: 'journal category', canonical_name: 'JE_CATEGORY', category: 'General Ledger' },
      { term: 'journal status', canonical_name: 'STATUS', category: 'General Ledger' },
      { term: 'gl balance', canonical_name: 'GL_BALANCES', category: 'General Ledger' },
      { term: 'account balance', canonical_name: 'GL_BALANCES', category: 'General Ledger' },
      { term: 'ledger', canonical_name: 'GL_LEDGERS', category: 'General Ledger' },
      { term: 'set of books', canonical_name: 'GL_SETS_OF_BOOKS', category: 'General Ledger' },
      { term: 'total debits', canonical_name: 'RUNNING_TOTAL_DR', category: 'General Ledger' },
      { term: 'total credits', canonical_name: 'RUNNING_TOTAL_CR', category: 'General Ledger' },
      // PURCHASING
      { term: 'purchase order', canonical_name: 'PO_HEADERS_ALL', category: 'Purchasing' },
      { term: 'po', canonical_name: 'PO_HEADERS_ALL', category: 'Purchasing' },
      { term: 'po header', canonical_name: 'PO_HEADERS_ALL', category: 'Purchasing' },
      { term: 'po number', canonical_name: 'SEGMENT1', category: 'Purchasing' },
      { term: 'purchase order number', canonical_name: 'SEGMENT1', category: 'Purchasing' },
      { term: 'po type', canonical_name: 'TYPE_LOOKUP_CODE', category: 'Purchasing' },
      { term: 'purchase order type', canonical_name: 'TYPE_LOOKUP_CODE', category: 'Purchasing' },
      { term: 'po line', canonical_name: 'PO_LINES_ALL', category: 'Purchasing' },
      { term: 'purchase order line', canonical_name: 'PO_LINES_ALL', category: 'Purchasing' },
      { term: 'unit price', canonical_name: 'UNIT_PRICE', category: 'Purchasing' },
      { term: 'price per unit', canonical_name: 'UNIT_PRICE', category: 'Purchasing' },
      { term: 'how much per unit', canonical_name: 'UNIT_PRICE', category: 'Purchasing' },
      { term: 'ordered quantity', canonical_name: 'QUANTITY', category: 'Purchasing' },
      { term: 'how many were ordered', canonical_name: 'QUANTITY', category: 'Purchasing' },
      { term: 'line amount', canonical_name: 'AMOUNT', category: 'Purchasing' },
      { term: 'po line amount', canonical_name: 'AMOUNT', category: 'Purchasing' },
      { term: 'item description', canonical_name: 'ITEM_DESCRIPTION', category: 'Purchasing' },
      { term: 'what was ordered', canonical_name: 'ITEM_DESCRIPTION', category: 'Purchasing' },
      { term: 'unit of measure', canonical_name: 'UNIT_MEAS_LOOKUP_CODE', category: 'Purchasing' },
      { term: 'supplier part number', canonical_name: 'VENDOR_PRODUCT_NUM', category: 'Purchasing' },
      { term: 'requisition', canonical_name: 'PO_REQUISITION_HEADERS_ALL', category: 'Purchasing' },
      { term: 'purchase requisition', canonical_name: 'PO_REQUISITION_HEADERS_ALL', category: 'Purchasing' },
      { term: 'req', canonical_name: 'PO_REQUISITION_HEADERS_ALL', category: 'Purchasing' },
      { term: 'requisition line', canonical_name: 'PO_REQUISITION_LINES_ALL', category: 'Purchasing' },
      { term: 'po approval status', canonical_name: 'AUTHORIZATION_STATUS', category: 'Purchasing' },
      { term: 'po approved', canonical_name: 'APPROVED_FLAG', category: 'Purchasing' },
      { term: 'approval date', canonical_name: 'APPROVED_DATE', category: 'Purchasing' },
      { term: 'when was po approved', canonical_name: 'APPROVED_DATE', category: 'Purchasing' },
      { term: 'po cancelled', canonical_name: 'CANCEL_FLAG', category: 'Purchasing' },
      { term: 'po closed', canonical_name: 'CLOSED_CODE', category: 'Purchasing' },
      { term: 'buyer', canonical_name: 'AGENT_ID', category: 'Purchasing' },
      { term: 'purchasing agent', canonical_name: 'AGENT_ID', category: 'Purchasing' },
      { term: 'who placed the order', canonical_name: 'AGENT_ID', category: 'Purchasing' },
      { term: 'po creation date', canonical_name: 'CREATION_DATE', category: 'Purchasing' },
      { term: 'when was po created', canonical_name: 'CREATION_DATE', category: 'Purchasing' },
      { term: 'blanket amount', canonical_name: 'BLANKET_TOTAL_AMOUNT', category: 'Purchasing' },
      { term: 'blanket po total', canonical_name: 'BLANKET_TOTAL_AMOUNT', category: 'Purchasing' },
      { term: 'po distribution', canonical_name: 'PO_DISTRIBUTIONS_ALL', category: 'Purchasing' },
      { term: 'charge account', canonical_name: 'PO_DISTRIBUTIONS_ALL', category: 'Purchasing' },
      { term: 'approved supplier', canonical_name: 'PO_APPROVED_SUPPLIER_LIST', category: 'Purchasing' },
      { term: 'qualified vendor', canonical_name: 'PO_APPROVED_SUPPLIER_LIST', category: 'Purchasing' },
      // ORDER MANAGEMENT
      { term: 'sales order', canonical_name: 'OE_ORDER_HEADERS_ALL', category: 'Order Management' },
      { term: 'order', canonical_name: 'OE_ORDER_HEADERS_ALL', category: 'Order Management' },
      { term: 'customer order', canonical_name: 'OE_ORDER_HEADERS_ALL', category: 'Order Management' },
      { term: 'order number', canonical_name: 'ORDER_NUMBER', category: 'Order Management' },
      { term: 'order date', canonical_name: 'ORDERED_DATE', category: 'Order Management' },
      { term: 'when was order placed', canonical_name: 'ORDERED_DATE', category: 'Order Management' },
      { term: 'booked date', canonical_name: 'BOOKED_DATE', category: 'Order Management' },
      { term: 'when was order booked', canonical_name: 'BOOKED_DATE', category: 'Order Management' },
      { term: 'booking date', canonical_name: 'BOOKED_DATE', category: 'Order Management' },
      { term: 'bookings', canonical_name: 'OE_ORDER_HEADERS_ALL', category: 'Order Management' },
      { term: 'order status', canonical_name: 'FLOW_STATUS_CODE', category: 'Order Management' },
      { term: 'flow status', canonical_name: 'FLOW_STATUS_CODE', category: 'Order Management' },
      { term: 'is order open', canonical_name: 'OPEN_FLAG', category: 'Order Management' },
      { term: 'customer po', canonical_name: 'CUST_PO_NUMBER', category: 'Order Management' },
      { term: 'customer purchase order', canonical_name: 'CUST_PO_NUMBER', category: 'Order Management' },
      { term: 'shipping method', canonical_name: 'SHIPPING_METHOD_CODE', category: 'Order Management' },
      { term: 'freight terms', canonical_name: 'FREIGHT_TERMS_CODE', category: 'Order Management' },
      { term: 'order category', canonical_name: 'ORDER_CATEGORY_CODE', category: 'Order Management' },
      { term: 'order cancelled', canonical_name: 'CANCELLED_FLAG', category: 'Order Management' },
      { term: 'order line', canonical_name: 'OE_ORDER_LINES_ALL', category: 'Order Management' },
      { term: 'sales order line', canonical_name: 'OE_ORDER_LINES_ALL', category: 'Order Management' },
      { term: 'ordered quantity', canonical_name: 'ORDERED_QUANTITY', category: 'Order Management' },
      { term: 'how many were ordered', canonical_name: 'ORDERED_QUANTITY', category: 'Order Management' },
      { term: 'shipped quantity', canonical_name: 'SHIPPED_QUANTITY', category: 'Order Management' },
      { term: 'how many were shipped', canonical_name: 'SHIPPED_QUANTITY', category: 'Order Management' },
      { term: 'fulfilled quantity', canonical_name: 'FULFILLED_QUANTITY', category: 'Order Management' },
      { term: 'invoiced quantity', canonical_name: 'INVOICED_QUANTITY', category: 'Order Management' },
      { term: 'cancelled quantity', canonical_name: 'CANCELLED_QUANTITY', category: 'Order Management' },
      { term: 'unit selling price', canonical_name: 'UNIT_SELLING_PRICE', category: 'Order Management' },
      { term: 'selling price', canonical_name: 'UNIT_SELLING_PRICE', category: 'Order Management' },
      { term: 'sale price', canonical_name: 'UNIT_SELLING_PRICE', category: 'Order Management' },
      { term: 'unit list price', canonical_name: 'UNIT_LIST_PRICE', category: 'Order Management' },
      { term: 'list price', canonical_name: 'UNIT_LIST_PRICE', category: 'Order Management' },
      { term: 'line status', canonical_name: 'FLOW_STATUS_CODE', category: 'Order Management' },
      { term: 'ship date', canonical_name: 'ACTUAL_SHIPMENT_DATE', category: 'Order Management' },
      { term: 'actual ship date', canonical_name: 'ACTUAL_SHIPMENT_DATE', category: 'Order Management' },
      { term: 'when did it ship', canonical_name: 'ACTUAL_SHIPMENT_DATE', category: 'Order Management' },
      { term: 'scheduled ship date', canonical_name: 'SCHEDULE_SHIP_DATE', category: 'Order Management' },
      { term: 'promise date', canonical_name: 'PROMISE_DATE', category: 'Order Management' },
      { term: 'request date', canonical_name: 'REQUEST_DATE', category: 'Order Management' },
      { term: 'requested delivery date', canonical_name: 'REQUEST_DATE', category: 'Order Management' },
      { term: 'pricing date', canonical_name: 'PRICING_DATE', category: 'Order Management' },
      { term: 'ordered item', canonical_name: 'ORDERED_ITEM', category: 'Order Management' },
      { term: 'what was ordered', canonical_name: 'ITEM_DESCRIPTION', category: 'Order Management' },
      { term: 'line category', canonical_name: 'LINE_CATEGORY_CODE', category: 'Order Management' },
      { term: 'return reason', canonical_name: 'RETURN_REASON_CODE', category: 'Order Management' },
      { term: 'tax amount', canonical_name: 'TAX_VALUE', category: 'Order Management' },
      { term: 'order tax', canonical_name: 'TAX_VALUE', category: 'Order Management' },
      { term: 'price adjustment', canonical_name: 'OE_PRICE_ADJUSTMENTS', category: 'Order Management' },
      { term: 'discount', canonical_name: 'OE_PRICE_ADJUSTMENTS', category: 'Order Management' },
      { term: 'sales credit', canonical_name: 'OE_SALES_CREDITS', category: 'Order Management' },
      { term: 'commission', canonical_name: 'OE_SALES_CREDITS', category: 'Order Management' },
      { term: 'order hold', canonical_name: 'OE_ORDER_HOLDS_ALL', category: 'Order Management' },
      { term: 'hold on order', canonical_name: 'OE_ORDER_HOLDS_ALL', category: 'Order Management' },
      { term: 'why is order on hold', canonical_name: 'OE_ORDER_HOLDS_ALL', category: 'Order Management' },
    ];

    // Insert ERP domain pack
    let erpInserted = 0;
    for (const s of erpPack) {
      try {
        await dbQuery(
          `INSERT IGNORE INTO global_synonyms (term, canonical_name, category, domain_pack, description)
           VALUES (?, ?, ?, 'erp', ?)`,
          [s.term, s.canonical_name, s.category, `ERP synonym: ${s.term} → ${s.canonical_name}`]
        );
        erpInserted++;
      } catch (e) { /* skip duplicates */ }
    }

    console.log(`✓ Seeded ${inserted} global synonyms (${healthcarePack.length} healthcare + ${globalTerms.length} universal) + ${erpInserted} ERP pack`);
  } catch (err) {
    console.error('Seed global synonyms warning:', err.message);
  }
}

process.on('uncaughtException', (err) => {
  console.error('[FATAL] Uncaught Exception:', err.message);
  console.error('[FATAL] Stack:', err.stack?.split('\n').slice(0, 8).join('\n'));
  // Don't exit — let the process continue serving requests
});

//  SERVER STARTUP
// ═══════════════════════════════════════════════════════════════
// On Vercel (serverless) require.main !== module, so listen() is skipped.
// The exported `app` is used directly by Vercel's runtime.
// Locally (node index.js) or on Railway/Docker, listen() runs normally.

if (require.main === module) {
app.listen(PORT, '0.0.0.0', async () => {
  console.log(`\n╔══════════════════════════════════════════╗`);
  console.log(`║   IDA — Intelligent Data Assistant        ║`);
  console.log(`╠══════════════════════════════════════════╣`);
  console.log(`║  Port:    ${String(PORT).padEnd(30)}║`);
  console.log(`║  Client:  ${path.join(__dirname, 'client').substring(0, 30).padEnd(30)}║`);
  console.log(`╚══════════════════════════════════════════╝\n`);

  // Check for SQLite data files
  const dataDir = path.join(__dirname, '..', 'data');
  console.log(`Data directory: ${dataDir} (exists: ${fs.existsSync(dataDir)})`);
  if (fs.existsSync(dataDir)) {
    const files = fs.readdirSync(dataDir);
    console.log(`Data files: ${files.join(', ')}`);
  }

  // Data Ask unique setup
  ensureDirectories();
  await initDatabase();

  // BOKG Builder startup sequence
  await ensureAdminUser();
  await ensureEndUser();
  await runMigrations();
  await ensureEnterpriseApps();
  await importEnterpriseQPD();
  await importEnterpriseContext();
  await patchSalesMetadata();
  await seedProvenQueryTemplates();
  await enrichOEBSSalesData();
  await reclassifyOFBizDomains();
  await seedGlobalSynonyms();

  // Verify database connectivity
  try {
    const { query } = require('./db');
    const [rows] = await query('SELECT COUNT(*) as app_count FROM applications');
    console.log(`✓ Connected to shared database (${rows[0].app_count} applications)`);

    const [tables] = await query('SELECT COUNT(*) as c FROM app_tables');
    const [docs] = await query("SELECT COUNT(*) as c FROM doc_sources WHERE status = 'ready'").catch(() => [[{ c: 0 }]]);
    console.log(`✓ Knowledge graph: ${tables[0].c} tables`);
    console.log(`✓ Document store: ${docs[0].c} documents`);
  } catch (err) {
    console.error('⚠ Database check failed:', err.message);
  }
});
} // end if (require.main === module)

module.exports = app;
