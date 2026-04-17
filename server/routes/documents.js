/**
 * Document Management Routes — Upload, list, delete documents
 *
 * Handles file uploads via multer, triggers the document pipeline
 * (extract → chunk → embed → store in pgvector).
 */
const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const { query } = require('../db');
const { processDocument } = require('../services/document-pipeline');

const router = express.Router();

// ─── Serial Document Processing Queue ───
// Processes one document at a time to avoid OOM from concurrent embedding operations.
// Each embedding batch holds 1536-dim float arrays; processing multiple docs in parallel
// can exhaust the Node.js heap (512MB-1GB).
const docProcessingQueue = [];
let docQueueProcessing = false;

async function processDocQueue() {
  if (docQueueProcessing || docProcessingQueue.length === 0) return;
  docQueueProcessing = true;

  while (docProcessingQueue.length > 0) {
    const { file, appId, collectionId, userId, workspaceId } = docProcessingQueue.shift();
    try {
      console.log(`[DocPipeline] Processing (${docProcessingQueue.length} remaining): ${file.originalname}`);
      const result = await processDocument(file.path, {
        appId, collectionId, userId, workspaceId,
        filename: file.originalname,
        fileType: file.ext,
      });
      console.log(`[DocPipeline] ✓ ${file.originalname}: ${result.chunkCount} chunks, ${result.textLength} chars`);
    } catch (err) {
      console.error(`[DocPipeline] ✗ ${file.originalname}: ${err.message}`);
      console.error(`[DocPipeline] Stack:`, err.stack?.split('\n').slice(0, 5).join('\n'));
    }
    // Clean up uploaded file
    try { fs.unlinkSync(file.path); } catch (e) { /* ignore */ }

    // Brief pause between docs to let GC run and event loop breathe
    await new Promise(r => setTimeout(r, 500));
  }

  docQueueProcessing = false;
  console.log('[DocPipeline] Queue empty — all documents processed');
}

// Configure multer for file uploads
const UPLOAD_DIR = path.join(__dirname, '..', '..', 'uploads');
if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOAD_DIR),
  filename: (req, file, cb) => {
    const unique = `${Date.now()}-${Math.round(Math.random() * 1E6)}`;
    cb(null, `${unique}-${file.originalname}`);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 50 * 1024 * 1024 },  // 50MB max
  fileFilter: (req, file, cb) => {
    const allowed = ['.pdf', '.docx', '.txt', '.md', '.html'];
    const ext = path.extname(file.originalname).toLowerCase();
    if (allowed.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error(`Unsupported file type: ${ext}. Allowed: ${allowed.join(', ')}`));
    }
  },
});

// ─── Collections ───

// GET /api/documents/collections?appId=X or ?workspaceId=X
router.get('/collections', async (req, res) => {
  try {
    const { appId, workspaceId } = req.query;
    if (!appId && !workspaceId) return res.status(400).json({ error: 'appId or workspaceId required' });

    let result;
    if (workspaceId) {
      result = await query(
        `SELECT id, name, description, doc_count, chunk_count, status, created_at, updated_at, workspace_id
         FROM doc_collections WHERE workspace_id = $1 ORDER BY created_at DESC`,
        [workspaceId]
      );
    } else {
      result = await query(
        `SELECT id, name, description, doc_count, chunk_count, status, created_at, updated_at, workspace_id
         FROM doc_collections WHERE app_id = $1 ORDER BY created_at DESC`,
        [appId]
      );
    }
    res.json({ collections: result.rows });
  } catch (err) {
    console.error('List collections error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/documents/collections
router.post('/collections', async (req, res) => {
  try {
    const { appId, workspaceId, name, description } = req.body;
    if (!name) return res.status(400).json({ error: 'name required' });
    if (!appId && !workspaceId) return res.status(400).json({ error: 'appId or workspaceId required' });

    // Resolve appId from workspace if needed
    let resolvedAppId = appId;
    let resolvedWorkspaceId = workspaceId;
    if (workspaceId && !appId) {
      const ws = await query('SELECT app_id FROM workspaces WHERE id = $1', [workspaceId]);
      if (ws.rows.length > 0) resolvedAppId = ws.rows[0].app_id;
    }
    if (appId && !workspaceId) {
      const ws = await query('SELECT id FROM workspaces WHERE app_id = $1 LIMIT 1', [appId]);
      if (ws.rows.length > 0) resolvedWorkspaceId = ws.rows[0].id;
    }

    const result = await query(
      `INSERT INTO doc_collections (app_id, workspace_id, name, description) VALUES ($1, $2, $3, $4) RETURNING *`,
      [resolvedAppId, resolvedWorkspaceId, name, description || '']
    );
    res.json({ collection: result.rows[0] });
  } catch (err) {
    console.error('Create collection error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/documents/collections/:id
router.delete('/collections/:id', async (req, res) => {
  try {
    await query('DELETE FROM doc_collections WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    console.error('Delete collection error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── Document Upload & Management ───

// GET /api/documents/sources?collectionId=X
router.get('/sources', async (req, res) => {
  try {
    const { collectionId, appId } = req.query;
    const conditions = [];
    const params = [];

    if (collectionId) {
      conditions.push(`collection_id = $${params.length + 1}`);
      params.push(collectionId);
    }
    if (appId) {
      conditions.push(`app_id = $${params.length + 1}`);
      params.push(appId);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const result = await query(
      `SELECT id, collection_id, filename, file_type, file_size_bytes, page_count, chunk_count, status, error_message, created_at
       FROM doc_sources ${where} ORDER BY created_at DESC`,
      params
    );
    res.json({ sources: result.rows });
  } catch (err) {
    console.error('List sources error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/documents/upload — Upload one or more files to a collection
// Responds immediately with accepted status, processes in background to avoid
// Railway's 30s gateway timeout on large files with many chunks to embed.
router.post('/upload', upload.array('files', 20), async (req, res) => {
  try {
    const { appId, collectionId, workspaceId } = req.body;
    if (!collectionId) {
      return res.status(400).json({ error: 'collectionId required' });
    }
    if (!appId && !workspaceId) {
      return res.status(400).json({ error: 'appId or workspaceId required' });
    }

    if (!req.files || req.files.length === 0) {
      return res.status(400).json({ error: 'No files uploaded' });
    }

    // Capture file info before responding
    const fileInfos = req.files.map(f => ({
      path: f.path,
      originalname: f.originalname,
      ext: path.extname(f.originalname).replace('.', '').toLowerCase(),
    }));

    // Respond immediately — processing happens in background
    res.json({
      status: 'accepted',
      message: `${fileInfos.length} file(s) accepted for processing. Check /sources?appId=${appId} for status.`,
      files: fileInfos.map(f => f.originalname),
    });

    // Queue files for serialized processing (one at a time to avoid OOM)
    for (const file of fileInfos) {
      docProcessingQueue.push({
        file,
        appId: parseInt(appId),
        collectionId: parseInt(collectionId),
        userId: req.user?.id || null,
        workspaceId: workspaceId ? parseInt(workspaceId) : null,
      });
    }
    processDocQueue().catch(err => {
      console.error('[DocPipeline] FATAL queue error:', err.message, err.stack?.split('\n').slice(0, 5).join('\n'));
      docQueueProcessing = false;  // Reset so future uploads can still be processed
    }); // Kick off processing if not already running
  } catch (err) {
    console.error('Upload error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/documents/sources/:id
router.delete('/sources/:id', async (req, res) => {
  try {
    // Chunks cascade-delete via FK
    await query('DELETE FROM doc_sources WHERE id = $1', [req.params.id]);
    res.json({ success: true });
  } catch (err) {
    console.error('Delete source error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/documents/stats?appId=X — Summary statistics
router.get('/stats', async (req, res) => {
  try {
    const { appId } = req.query;
    if (!appId) return res.status(400).json({ error: 'appId required' });

    const stats = await query(
      `SELECT
         (SELECT COUNT(*) FROM doc_collections WHERE app_id = $1) AS collection_count,
         (SELECT COUNT(*) FROM doc_sources WHERE app_id = $1 AND status = 'ready') AS doc_count,
         (SELECT COUNT(*) FROM doc_chunks WHERE app_id = $1) AS chunk_count,
         (SELECT COALESCE(SUM(file_size_bytes), 0) FROM doc_sources WHERE app_id = $1) AS total_size_bytes`,
      [appId]
    );
    res.json({ stats: stats.rows[0] });
  } catch (err) {
    console.error('Stats error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/documents/test-embed — Test OpenAI embedding API connectivity
// ?size=small|medium|large to test different input sizes
router.get('/test-embed', async (req, res) => {
  try {
    const { embedSingleText, EMBEDDING_MODEL, EMBEDDING_DIMS } = require('../services/document-pipeline');
    const size = req.query.size || 'small';
    let testText = 'This is a test document about accounts payable policies.';
    if (size === 'medium') {
      testText = testText.repeat(20); // ~1100 chars
    } else if (size === 'large') {
      testText = testText.repeat(60); // ~3300 chars (similar to a real chunk)
    }
    console.log(`[TestEmbed] Starting ${size} test embedding (${testText.length} chars)...`);
    // Log memory before
    const memBefore = process.memoryUsage();
    console.log(`[TestEmbed] Memory before: RSS=${Math.round(memBefore.rss/1024/1024)}MB Heap=${Math.round(memBefore.heapUsed/1024/1024)}/${Math.round(memBefore.heapTotal/1024/1024)}MB`);

    const start = Date.now();
    const embedding = await embedSingleText(testText);
    const elapsed = Date.now() - start;

    const memAfter = process.memoryUsage();
    console.log(`[TestEmbed] Memory after: RSS=${Math.round(memAfter.rss/1024/1024)}MB Heap=${Math.round(memAfter.heapUsed/1024/1024)}/${Math.round(memAfter.heapTotal/1024/1024)}MB`);
    console.log(`[TestEmbed] Success in ${elapsed}ms — vector length: ${embedding?.length}`);

    res.json({
      success: true,
      model: EMBEDDING_MODEL,
      input_chars: testText.length,
      expected_dims: EMBEDDING_DIMS,
      actual_dims: embedding?.length,
      elapsed_ms: elapsed,
      memory: {
        rss_mb: Math.round(memAfter.rss/1024/1024),
        heap_used_mb: Math.round(memAfter.heapUsed/1024/1024),
        heap_total_mb: Math.round(memAfter.heapTotal/1024/1024),
      },
      sample: embedding?.slice(0, 3),
    });
  } catch (err) {
    console.error('[TestEmbed] Failed:', err.message);
    res.status(500).json({ error: err.message, stack: err.stack?.split('\n').slice(0, 5) });
  }
});

// GET /api/documents/queue-status — Check processing queue state
router.get('/queue-status', async (req, res) => {
  res.json({
    queueLength: docProcessingQueue.length,
    isProcessing: docQueueProcessing,
    queueItems: docProcessingQueue.map(q => q.file.originalname),
  });
});

// ═══════════════════════════════════════════════════════════════
//  DOCUMENT SEARCH — Semantic, Full-Text, and Hybrid modes
// ═══════════════════════════════════════════════════════════════

// Ensure tsvector column + GIN index exist (idempotent, runs once on first search)
let _tsvectorReady = false;
async function ensureTsvector() {
  if (_tsvectorReady) return;
  try {
    await query(`ALTER TABLE doc_chunks ADD COLUMN IF NOT EXISTS content_tsv tsvector`);
    await query(`CREATE INDEX IF NOT EXISTS idx_doc_chunks_tsv ON doc_chunks USING GIN(content_tsv)`);
    // Backfill any chunks that don't have tsvector yet
    await query(`UPDATE doc_chunks SET content_tsv = to_tsvector('english', content) WHERE content_tsv IS NULL`);
    _tsvectorReady = true;
    console.log('[DocSearch] tsvector column and index ready');
  } catch (err) {
    console.error('[DocSearch] tsvector setup error:', err.message);
  }
}

// POST /api/documents/search — Search document chunks
// Body: { appId, collectionId?, query, mode: "semantic"|"fulltext"|"hybrid", limit? }
router.post('/search', async (req, res) => {
  try {
    const { appId, collectionId, query: searchQuery, mode = 'hybrid', limit = 5 } = req.body;
    if (!appId || !searchQuery) {
      return res.status(400).json({ error: 'appId and query are required' });
    }

    await ensureTsvector();

    const { embedSingleText } = require('../services/document-pipeline');
    const results = [];
    const timing = { embed_ms: 0, search_ms: 0 };

    if (mode === 'semantic' || mode === 'hybrid') {
      // Get query embedding
      const t1 = Date.now();
      const queryEmbedding = await embedSingleText(searchQuery);
      timing.embed_ms = Date.now() - t1;
      const embeddingStr = `[${queryEmbedding.join(',')}]`;

      if (mode === 'semantic') {
        // Pure semantic search — cosine similarity
        const t2 = Date.now();
        const semanticResults = await query(
          `SELECT dc.id, dc.source_id, dc.chunk_index, dc.content, dc.content_length,
                  ds.filename,
                  1 - (dc.embedding <=> $1::vector) AS similarity
           FROM doc_chunks dc
           JOIN doc_sources ds ON dc.source_id = ds.id
           WHERE dc.app_id = $2
             ${collectionId ? 'AND dc.collection_id = $3' : ''}
           ORDER BY dc.embedding <=> $1::vector
           LIMIT $${collectionId ? '4' : '3'}`,
          collectionId
            ? [embeddingStr, parseInt(appId), parseInt(collectionId), parseInt(limit)]
            : [embeddingStr, parseInt(appId), parseInt(limit)]
        );
        timing.search_ms = Date.now() - t2;
        for (const row of semanticResults.rows) {
          results.push({
            chunk_id: row.id,
            source_id: row.source_id,
            filename: row.filename,
            chunk_index: row.chunk_index,
            content: row.content,
            score: parseFloat(row.similarity),
            match_type: 'semantic',
          });
        }
      } else {
        // Hybrid mode — combine semantic + full-text
        const t2 = Date.now();
        const hybridResults = await query(
          `WITH semantic AS (
             SELECT dc.id, dc.source_id, dc.chunk_index, dc.content, dc.content_length,
                    ds.filename,
                    1 - (dc.embedding <=> $1::vector) AS sem_score
             FROM doc_chunks dc
             JOIN doc_sources ds ON dc.source_id = ds.id
             WHERE dc.app_id = $2
               ${collectionId ? 'AND dc.collection_id = $3' : ''}
           ),
           fulltext AS (
             SELECT dc.id,
                    ts_rank_cd(dc.content_tsv, plainto_tsquery('english', $${collectionId ? '4' : '3'})) AS ft_score
             FROM doc_chunks dc
             WHERE dc.app_id = $2
               ${collectionId ? 'AND dc.collection_id = $3' : ''}
               AND dc.content_tsv @@ plainto_tsquery('english', $${collectionId ? '4' : '3'})
           )
           SELECT s.id, s.source_id, s.chunk_index, s.content, s.content_length, s.filename, s.sem_score,
                  COALESCE(f.ft_score, 0) AS ft_score,
                  (0.7 * s.sem_score + 0.3 * COALESCE(f.ft_score, 0)) AS combined_score
           FROM semantic s
           LEFT JOIN fulltext f ON s.id = f.id
           ORDER BY combined_score DESC
           LIMIT $${collectionId ? '5' : '4'}`,
          collectionId
            ? [embeddingStr, parseInt(appId), parseInt(collectionId), searchQuery, parseInt(limit)]
            : [embeddingStr, parseInt(appId), searchQuery, parseInt(limit)]
        );
        timing.search_ms = Date.now() - t2;
        for (const row of hybridResults.rows) {
          results.push({
            chunk_id: row.id,
            source_id: row.source_id,
            filename: row.filename,
            chunk_index: row.chunk_index,
            content: row.content,
            score: parseFloat(row.combined_score),
            semantic_score: parseFloat(row.sem_score),
            fulltext_score: parseFloat(row.ft_score),
            match_type: 'hybrid',
          });
        }
      }
    } else if (mode === 'fulltext') {
      // Pure full-text search
      const t2 = Date.now();
      const ftResults = await query(
        `SELECT dc.id, dc.source_id, dc.chunk_index, dc.content, dc.content_length,
                ds.filename,
                ts_rank_cd(dc.content_tsv, plainto_tsquery('english', $1)) AS rank
         FROM doc_chunks dc
         JOIN doc_sources ds ON dc.source_id = ds.id
         WHERE dc.app_id = $2
           ${collectionId ? 'AND dc.collection_id = $3' : ''}
           AND dc.content_tsv @@ plainto_tsquery('english', $1)
         ORDER BY rank DESC
         LIMIT $${collectionId ? '4' : '3'}`,
        collectionId
          ? [searchQuery, parseInt(appId), parseInt(collectionId), parseInt(limit)]
          : [searchQuery, parseInt(appId), parseInt(limit)]
      );
      timing.search_ms = Date.now() - t2;
      for (const row of ftResults.rows) {
        results.push({
          chunk_id: row.id,
          source_id: row.source_id,
          filename: row.filename,
          chunk_index: row.chunk_index,
          content: row.content,
          score: parseFloat(row.rank),
          match_type: 'fulltext',
        });
      }
    } else {
      return res.status(400).json({ error: `Invalid mode: ${mode}. Use semantic, fulltext, or hybrid.` });
    }

    res.json({
      query: searchQuery,
      mode,
      result_count: results.length,
      timing,
      results,
    });
  } catch (err) {
    console.error('[DocSearch] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
