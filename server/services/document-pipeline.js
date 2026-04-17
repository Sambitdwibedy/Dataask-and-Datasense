/**
 * Document Pipeline — Ingest → Extract → Chunk → Embed → Store
 *
 * Handles PDF, DOCX, TXT, MD, HTML files.
 * Chunks with section-awareness and overlap.
 * Embeds with OpenAI text-embedding-3-small (1536 dims).
 * Stores in PostgreSQL with pgvector.
 */
const fs = require('fs');
const path = require('path');
const pdfParse = require('pdf-parse');
const mammoth = require('mammoth');
const { query } = require('../db');

// ─── Configuration ───
const CHUNK_SIZE = 800;       // Target tokens per chunk (~3200 chars)
const CHUNK_OVERLAP = 100;    // Overlap tokens between chunks (~400 chars)
const CHARS_PER_TOKEN = 4;    // Rough estimate
const EMBEDDING_MODEL = 'text-embedding-3-small';
const EMBEDDING_DIMS = 1536;
const BATCH_SIZE = 10;        // Embeddings per API call (small batches to control memory)

// ─── Text Extraction ───

async function extractText(filePath, fileType) {
  switch (fileType.toLowerCase()) {
    case 'pdf': {
      const buffer = fs.readFileSync(filePath);
      const data = await pdfParse(buffer);
      return { text: data.text, pageCount: data.numpages, metadata: { info: data.info } };
    }
    case 'docx': {
      const result = await mammoth.extractRawText({ path: filePath });
      return { text: result.value, pageCount: null, metadata: {} };
    }
    case 'txt':
    case 'md':
    case 'html': {
      const text = fs.readFileSync(filePath, 'utf-8');
      return { text, pageCount: null, metadata: {} };
    }
    default:
      throw new Error(`Unsupported file type: ${fileType}`);
  }
}

// ─── Chunking ───

/**
 * Section-aware chunking with overlap.
 * Tries to split on paragraph boundaries, then sentence boundaries.
 */
function chunkText(text, options = {}) {
  const chunkChars = (options.chunkSize || CHUNK_SIZE) * CHARS_PER_TOKEN;
  const overlapChars = (options.overlap || CHUNK_OVERLAP) * CHARS_PER_TOKEN;

  // Normalize whitespace
  text = text.replace(/\r\n/g, '\n').replace(/\n{3,}/g, '\n\n').trim();

  if (text.length <= chunkChars) {
    return [{ content: text, index: 0 }];
  }

  const chunks = [];
  let start = 0;
  let index = 0;

  while (start < text.length) {
    let end = Math.min(start + chunkChars, text.length);

    // If this is the last chunk (remaining text fits in one chunk), take it all
    if (end >= text.length) {
      const content = text.slice(start).trim();
      if (content.length > 50) {
        chunks.push({ content, index });
      }
      break;
    }

    // Try to break at paragraph boundary
    const paraBreak = text.lastIndexOf('\n\n', end);
    if (paraBreak > start + chunkChars * 0.5) {
      end = paraBreak;
    } else {
      // Try sentence boundary
      const sentBreak = text.lastIndexOf('. ', end);
      if (sentBreak > start + chunkChars * 0.5) {
        end = sentBreak + 1;
      }
    }

    const content = text.slice(start, end).trim();
    if (content.length > 50) {
      chunks.push({ content, index });
      index++;
    }

    // Move forward with overlap — MUST advance past overlap to prevent infinite loop
    start = Math.max(end - overlapChars, start + chunkChars / 2);
  }

  return chunks;
}

// ─── Embedding ───

// Singleton OpenAI client — avoids creating a new client per chunk
let _openaiClient = null;
function getOpenAI() {
  if (!_openaiClient) {
    const OpenAI = require('openai');
    _openaiClient = new OpenAI({
      apiKey: process.env.OPENAI_API_KEY,
      timeout: 30000,       // 30s per request
      maxRetries: 2,        // retry on transient failures
    });
  }
  return _openaiClient;
}

async function embedTexts(texts) {
  const openai = getOpenAI();

  const allEmbeddings = [];
  for (let i = 0; i < texts.length; i += BATCH_SIZE) {
    const batch = texts.slice(i, i + BATCH_SIZE);
    console.log(`[Embed] Batch ${Math.floor(i/BATCH_SIZE)+1}/${Math.ceil(texts.length/BATCH_SIZE)}: ${batch.length} texts (${batch.map(t => t.length)} chars)`);
    try {
      const response = await openai.embeddings.create({
        model: EMBEDDING_MODEL,
        input: batch,
      });
      for (const item of response.data) {
        allEmbeddings.push(item.embedding);
      }
      console.log(`[Embed] Batch ${Math.floor(i/BATCH_SIZE)+1} complete — ${response.data.length} vectors`);
    } catch (embedErr) {
      console.error(`[Embed] Batch ${Math.floor(i/BATCH_SIZE)+1} FAILED:`, embedErr.message);
      throw embedErr;
    }
    // Yield to event loop between batches
    await new Promise(r => setImmediate(r));
  }
  return allEmbeddings;
}

async function embedSingleText(text) {
  const results = await embedTexts([text]);
  return results[0];
}

// ─── Full Pipeline ───

/**
 * Process a single document: extract → chunk → embed → store.
 * Returns { sourceId, chunkCount }.
 */
async function processDocument(filePath, { appId, collectionId, userId, filename, fileType, workspaceId }) {
  // 1. Update source status
  const srcResult = await query(
    `INSERT INTO doc_sources (collection_id, app_id, filename, file_type, file_size_bytes, status, uploaded_by)
     VALUES ($1, $2, $3, $4, $5, 'processing', $6) RETURNING id`,
    [collectionId, appId, filename, fileType, fs.statSync(filePath).size, userId]
  );
  const sourceId = srcResult.rows[0].id;

  // Helper to log progress into the DB (survives crashes)
  const logProgress = async (step) => {
    try {
      await query(`UPDATE doc_sources SET error_message = $1 WHERE id = $2`, [
        `PROGRESS: ${step} at ${new Date().toISOString()}`, sourceId
      ]);
    } catch (e) { /* ignore log failures */ }
  };

  try {
    // 2. Extract text
    await logProgress('step2-extract-start');
    console.log(`[DocPipeline] Extracting text from: ${filePath} (type: ${fileType})`);
    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${filePath} — may have been cleaned up before processing`);
    }
    const { text, pageCount, metadata } = await extractText(filePath, fileType);
    console.log(`[DocPipeline] Extracted ${text?.length || 0} chars from ${filename}`);
    await logProgress(`step2-extract-done chars=${text?.length || 0}`);
    if (!text || text.trim().length < 50) {
      throw new Error('Extracted text is empty or too short');
    }

    await logProgress('step3-update-source');
    await query(
      `UPDATE doc_sources SET extracted_text = $1, page_count = $2, metadata = $3 WHERE id = $4`,
      [text, pageCount, JSON.stringify(metadata), sourceId]
    );
    await logProgress('step3-update-source-done');

    // 3. Chunk
    const chunks = chunkText(text);
    console.log(`[DocPipeline] ${filename}: ${chunks.length} chunks created`);
    await logProgress(`step4-chunked count=${chunks.length}`);

    // 4+5. Embed and store ONE CHUNK AT A TIME to avoid OOM.
    //       Previous approach held all embedding vectors in memory simultaneously
    //       (N × 1536 floats × 8 bytes ≈ 12KB per vector), which exhausted the
    //       Node.js heap when combined with the app's ~600-700MB base footprint.
    console.log(`[DocPipeline] ${filename}: Embedding + storing ${chunks.length} chunks (streaming, 1 at a time)...`);
    for (let i = 0; i < chunks.length; i++) {
      const chunkStart = Date.now();
      await logProgress(`step5-chunk-${i+1}of${chunks.length}-embed-start`);
      console.log(`[DocPipeline] ${filename}: chunk ${i+1}/${chunks.length} — embedding (${chunks[i].content.length} chars)...`);

      // Embed single chunk
      const t1 = Date.now();
      const embedding = await embedSingleText(chunks[i].content);
      await logProgress(`step5-chunk-${i+1}of${chunks.length}-embed-done ${Date.now()-t1}ms`);
      console.log(`[DocPipeline] ${filename}: chunk ${i+1} — embedded in ${Date.now()-t1}ms`);

      // Serialize and insert immediately, then discard
      const embeddingStr = `[${embedding.join(',')}]`;
      const t2 = Date.now();
      await query(
        `INSERT INTO doc_chunks (source_id, collection_id, app_id, workspace_id, chunk_index, content, content_length, embedding, content_tsv, metadata)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8::vector, to_tsvector('english', $6), $9)`,
        [
          sourceId, collectionId, appId, workspaceId || null,
          chunks[i].index,
          chunks[i].content,
          chunks[i].content.length,
          embeddingStr,
          JSON.stringify({ page: null, section: null })
        ]
      );
      console.log(`[DocPipeline] ${filename}: chunk ${i+1} — inserted in ${Date.now()-t2}ms (total: ${Date.now()-chunkStart}ms)`);

      // Update progress so it's observable from outside
      await query(`UPDATE doc_sources SET chunk_count = $1 WHERE id = $2`, [i + 1, sourceId]);

      // Help GC reclaim the vector memory and yield to event loop
      embedding.length = 0;
      await new Promise(r => setImmediate(r));
    }
    console.log(`[DocPipeline] ${filename}: All ${chunks.length} chunks embedded and stored`);

    // 6. Update source and collection
    await query(
      `UPDATE doc_sources SET status = 'ready', chunk_count = $1 WHERE id = $2`,
      [chunks.length, sourceId]
    );
    await query(
      `UPDATE doc_collections
       SET doc_count = (SELECT COUNT(*) FROM doc_sources WHERE collection_id = $1 AND status = 'ready'),
           chunk_count = (SELECT COUNT(*) FROM doc_chunks WHERE collection_id = $1),
           updated_at = NOW()
       WHERE id = $1`,
      [collectionId]
    );

    return { sourceId, chunkCount: chunks.length, textLength: text.length };

  } catch (err) {
    await query(
      `UPDATE doc_sources SET status = 'error', error_message = $1 WHERE id = $2`,
      [err.message, sourceId]
    );
    throw err;
  }
}

module.exports = {
  extractText,
  chunkText,
  embedTexts,
  embedSingleText,
  processDocument,
  EMBEDDING_MODEL,
  EMBEDDING_DIMS,
};
