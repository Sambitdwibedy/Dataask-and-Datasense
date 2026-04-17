/**
 * Document Management Routes — Upload, list, delete documents
 *
 * Handles file uploads via multer, triggers the document pipeline
 * (extract → chunk → store).
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

    let rows;
    if (workspaceId) {
      [rows] = await query(
        `SELECT id, name, description, doc_count, chunk_count, status, created_at, updated_at, workspace_id
         FROM doc_collections WHERE workspace_id = ? ORDER BY created_at DESC`,
        [workspaceId]
      );
    } else {
      [rows] = await query(
        `SELECT id, name, description, doc_count, chunk_count, status, created_at, updated_at, workspace_id
         FROM doc_collections WHERE app_id = ? ORDER BY created_at DESC`,
        [appId]
      );
    }
    res.json({ collections: rows });
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
      const [wsRows] = await query('SELECT app_id FROM workspaces WHERE id = ?', [workspaceId]);
      if (wsRows.length > 0) resolvedAppId = wsRows[0].app_id;
    }
    if (appId && !workspaceId) {
      const [wsRows] = await query('SELECT id FROM workspaces WHERE app_id = ? LIMIT 1', [appId]);
      if (wsRows.length > 0) resolvedWorkspaceId = wsRows[0].id;
    }

    const [result] = await query(
      `INSERT INTO doc_collections (app_id, workspace_id, name, description) VALUES (?, ?, ?, ?)`,
      [resolvedAppId, resolvedWorkspaceId, name, description || '']
    );
    const [colRows] = await query('SELECT * FROM doc_collections WHERE id = ?', [result.insertId]);
    res.json({ collection: colRows[0] });
  } catch (err) {
    console.error('Create collection error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/documents/collections/:id
router.delete('/collections/:id', async (req, res) => {
  try {
    await query('DELETE FROM doc_collections WHERE id = ?', [req.params.id]);
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
      conditions.push(`collection_id = ?`);
      params.push(collectionId);
    }
    if (appId) {
      conditions.push(`app_id = ?`);
      params.push(appId);
    }

    const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
    const [rows] = await query(
      `SELECT id, collection_id, filename, file_type, file_size_bytes, page_count, chunk_count, status, error_message, created_at
       FROM doc_sources ${where} ORDER BY created_at DESC`,
      params
    );
    res.json({ sources: rows });
  } catch (err) {
    console.error('List sources error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/documents/upload — Upload one or more files to a collection
// Responds immediately with accepted status, processes in background to avoid
// gateway timeout on large files with many chunks to embed.
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
    await query('DELETE FROM doc_sources WHERE id = ?', [req.params.id]);
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

    const [[statsRow]] = await query(
      `SELECT
         (SELECT COUNT(*) FROM doc_collections WHERE app_id = ?) AS collection_count,
         (SELECT COUNT(*) FROM doc_sources WHERE app_id = ? AND status = 'ready') AS doc_count,
         (SELECT COUNT(*) FROM doc_chunks WHERE app_id = ?) AS chunk_count,
         (SELECT COALESCE(SUM(file_size_bytes), 0) FROM doc_sources WHERE app_id = ?) AS total_size_bytes`,
      [appId, appId, appId, appId]
    );
    res.json({ stats: statsRow });
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
//  DOCUMENT SEARCH — Full-text search (MySQL MATCH AGAINST)
// ═══════════════════════════════════════════════════════════════

// POST /api/documents/search — Search document chunks
// Body: { appId, collectionId?, query, mode: "semantic"|"fulltext"|"hybrid", limit? }
router.post('/search', async (req, res) => {
  try {
    const { appId, collectionId, query: searchQuery, mode = 'fulltext', limit = 5 } = req.body;
    if (!appId || !searchQuery) {
      return res.status(400).json({ error: 'appId and query are required' });
    }

    const results = [];
    const timing = { embed_ms: 0, search_ms: 0 };

    // MySQL full-text search (semantic/hybrid both fall back to full-text — no pgvector)
    const t2 = Date.now();
    const conditionParts = ['dc.app_id = ?'];
    const searchParams = [parseInt(appId)];

    if (collectionId) {
      conditionParts.push('dc.collection_id = ?');
      searchParams.push(parseInt(collectionId));
    }
    conditionParts.push('dc.content LIKE ?');
    searchParams.push(`%${searchQuery}%`);
    searchParams.push(parseInt(limit));

    const [ftRows] = await query(
      `SELECT dc.id, dc.source_id, dc.chunk_index, dc.content, dc.content_length,
              ds.filename,
              1.0 AS score
       FROM doc_chunks dc
       JOIN doc_sources ds ON dc.source_id = ds.id
       WHERE ${conditionParts.join(' AND ')}
       ORDER BY dc.id
       LIMIT ?`,
      searchParams
    );
    timing.search_ms = Date.now() - t2;

    for (const row of ftRows) {
      results.push({
        chunk_id: row.id,
        source_id: row.source_id,
        filename: row.filename,
        chunk_index: row.chunk_index,
        content: row.content,
        score: parseFloat(row.score),
        match_type: 'fulltext',
      });
    }

    res.json({
      query: searchQuery,
      mode: 'fulltext',
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
