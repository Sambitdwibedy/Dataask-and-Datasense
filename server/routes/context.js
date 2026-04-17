const express = require('express');
const { query } = require('../db');
const multer = require('multer');
const path = require('path');
const fs = require('fs');

const router = express.Router();

// Configure multer for file uploads (in-memory storage for processing)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }, // 20MB max
  fileFilter: (req, file, cb) => {
    const allowedTypes = [
      'application/pdf',
      'text/plain', 'text/csv', 'text/markdown',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'application/vnd.ms-excel',
      'image/png', 'image/jpeg', 'image/gif', 'image/webp',
    ];
    const allowedExts = ['.pdf', '.txt', '.md', '.csv', '.xlsx', '.xls', '.png', '.jpg', '.jpeg', '.gif', '.webp', '.sql', '.json', '.xml', '.log'];
    const ext = path.extname(file.originalname).toLowerCase();
    if (allowedTypes.includes(file.mimetype) || allowedExts.includes(ext)) {
      cb(null, true);
    } else {
      cb(new Error(`Unsupported file type: ${file.mimetype} (${ext})`));
    }
  }
});

// ============================================================
// TEXT EXTRACTION HELPERS
// ============================================================

async function extractFromPDF(buffer) {
  try {
    const pdfParse = require('pdf-parse');
    const data = await pdfParse(buffer);
    return data.text || '';
  } catch (err) {
    console.error('PDF extraction error:', err.message);
    return `[PDF extraction failed: ${err.message}]`;
  }
}

function extractFromText(buffer) {
  return buffer.toString('utf-8');
}

function extractFromSpreadsheet(buffer) {
  try {
    const XLSX = require('xlsx');
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    const lines = [];

    for (const sheetName of workbook.SheetNames) {
      const sheet = workbook.Sheets[sheetName];
      const json = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });

      lines.push(`=== Sheet: ${sheetName} ===`);
      for (const row of json) {
        const rowStr = row.map(cell => String(cell).trim()).filter(Boolean).join(' | ');
        if (rowStr) lines.push(rowStr);
      }
      lines.push('');
    }

    return lines.join('\n');
  } catch (err) {
    console.error('Spreadsheet extraction error:', err.message);
    return `[Spreadsheet extraction failed: ${err.message}]`;
  }
}

async function extractFromImage(buffer, mimeType, filename) {
  // Use Claude's vision API to extract text/meaning from images
  const API_KEY = process.env.ANTHROPIC_API_KEY;
  if (!API_KEY) {
    return '[Image extraction requires ANTHROPIC_API_KEY]';
  }

  try {
    const base64Data = buffer.toString('base64');
    const mediaType = mimeType || 'image/png';

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 4096,
        temperature: 0,
        messages: [{
          role: 'user',
          content: [
            {
              type: 'image',
              source: { type: 'base64', media_type: mediaType, data: base64Data },
            },
            {
              type: 'text',
              text: `Extract ALL text and structured information from this image. This is a reference document for a database schema enrichment tool. Focus on:
- Column names and their descriptions/meanings
- Table names and entity definitions
- Relationships between entities
- Business rules, domain terminology, data dictionary entries
- Any ERD (Entity-Relationship Diagram) elements

Output as structured plain text. If this is an ERD, describe each entity, its attributes, and relationships.
If this is a data dictionary, output it as a table format.
Preserve all technical terms and column names exactly as shown.`
            }
          ]
        }]
      })
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error('Image extraction API error:', errText);
      return `[Image extraction failed: API error ${response.status}]`;
    }

    const data = await response.json();
    return data.content?.[0]?.text || '[No text extracted from image]';
  } catch (err) {
    console.error('Image extraction error:', err.message);
    return `[Image extraction failed: ${err.message}]`;
  }
}

async function extractText(buffer, mimetype, filename) {
  const ext = path.extname(filename).toLowerCase();

  if (mimetype === 'application/pdf' || ext === '.pdf') {
    return extractFromPDF(buffer);
  }

  if (['text/plain', 'text/csv', 'text/markdown'].includes(mimetype) ||
      ['.txt', '.md', '.csv'].includes(ext)) {
    return extractFromText(buffer);
  }

  if (mimetype?.includes('spreadsheet') || mimetype?.includes('excel') ||
      ['.xlsx', '.xls'].includes(ext)) {
    return extractFromSpreadsheet(buffer);
  }

  if (mimetype?.startsWith('image/') ||
      ['.png', '.jpg', '.jpeg', '.gif', '.webp'].includes(ext)) {
    return extractFromImage(buffer, mimetype, filename);
  }

  return `[Unsupported file type: ${mimetype}]`;
}

// ============================================================
// ROUTES
// ============================================================

// GET /api/context/:appId/documents - list all context documents for an app
router.get('/:appId/documents', async (req, res) => {
  try {
    const { appId } = req.params;
    const [rows] = await query(
      `SELECT id, app_id, filename, file_type, file_size, description,
              LENGTH(extracted_text) as text_length, uploaded_at
       FROM context_documents
       WHERE app_id = ?
       ORDER BY uploaded_at DESC`,
      [appId]
    );
    res.json({ documents: rows });
  } catch (err) {
    console.error('List context documents error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/context/:appId/documents/:docId - get a single document with extracted text
router.get('/:appId/documents/:docId', async (req, res) => {
  try {
    const { appId, docId } = req.params;
    const [rows] = await query(
      `SELECT * FROM context_documents WHERE id = ? AND app_id = ?`,
      [docId, appId]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Document not found' });
    }
    res.json({ document: rows[0] });
  } catch (err) {
    console.error('Get context document error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/context/:appId/upload - upload a context document
router.post('/:appId/upload', upload.single('file'), async (req, res) => {
  try {
    const { appId } = req.params;
    const file = req.file;
    const description = req.body.description || '';

    if (!file) {
      return res.status(400).json({ error: 'No file provided' });
    }

    console.log(`Processing context document: ${file.originalname} (${file.mimetype}, ${file.size} bytes)`);

    // Extract text from the uploaded document
    const extractedText = await extractText(file.buffer, file.mimetype, file.originalname);
    const textLength = extractedText ? extractedText.length : 0;
    console.log(`Extracted ${textLength} characters from ${file.originalname}`);

    // Store in database
    const [result] = await query(
      `INSERT INTO context_documents (app_id, filename, file_type, file_size, extracted_text, description, uploaded_by, uploaded_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, NOW())`,
      [appId, file.originalname, file.mimetype, file.size, extractedText, description, req.user?.id || null]
    );

    const [docRows] = await query('SELECT id, app_id, filename, file_type, file_size, description, LENGTH(extracted_text) as text_length, uploaded_at FROM context_documents WHERE id = ?', [result.insertId]);
    res.status(201).json({
      document: docRows[0],
      text_length: textLength,
      preview: extractedText ? extractedText.substring(0, 500) + (textLength > 500 ? '...' : '') : '',
    });
  } catch (err) {
    console.error('Upload context document error:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// DELETE /api/context/:appId/documents/:docId - remove a context document
router.delete('/:appId/documents/:docId', async (req, res) => {
  try {
    const { appId, docId } = req.params;
    const [rows] = await query(
      'DELETE FROM context_documents WHERE id = ? AND app_id = ?',
      [docId, appId]
    );
    if (rows.length === 0) {
      return res.status(404).json({ error: 'Document not found' });
    }
    res.json({ deleted: true });
  } catch (err) {
    console.error('Delete context document error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/context/:appId/combined - get all context as a single text block (for enrichment)
router.get('/:appId/combined', async (req, res) => {
  try {
    const { appId } = req.params;
    const [rows] = await query(
      `SELECT filename, extracted_text FROM context_documents
       WHERE app_id = ? AND extracted_text IS NOT NULL AND extracted_text != ''
       ORDER BY uploaded_at`,
      [appId]
    );

    const sections = rows.map(doc =>
      `=== Source: ${doc.filename} ===\n${doc.extracted_text}`
    );

    res.json({
      document_count: rows.length,
      combined_text: sections.join('\n\n'),
      total_characters: sections.join('\n\n').length,
    });
  } catch (err) {
    console.error('Get combined context error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/context/:appId/re-enrich - targeted re-enrich of specific columns using context
router.post('/:appId/re-enrich', async (req, res) => {
  try {
    const { appId } = req.params;
    const { column_ids } = req.body;

    if (!column_ids || !Array.isArray(column_ids) || column_ids.length === 0) {
      return res.status(400).json({ error: 'column_ids array required' });
    }

    if (column_ids.length > 50) {
      return res.status(400).json({ error: 'Maximum 50 columns per re-enrich request' });
    }

    // Get context documents
    const [contextRows] = await query(
      `SELECT filename, extracted_text FROM context_documents
       WHERE app_id = ? AND extracted_text IS NOT NULL AND extracted_text != ''
       ORDER BY uploaded_at`,
      [appId]
    );

    if (contextRows.length === 0) {
      return res.status(400).json({ error: 'No context documents uploaded. Upload reference materials first.' });
    }

    const contextText = contextRows
      .map(doc => `=== Source: ${doc.filename} ===\n${doc.extracted_text}`)
      .join('\n\n');

    console.log(`[Re-Enrich] ${contextRows.length} context docs loaded, total ${contextText.length} chars`);
    contextRows.forEach(doc => {
      console.log(`[Re-Enrich]   - ${doc.filename}: ${doc.extracted_text?.length || 0} chars`);
    });

    // Get the columns to re-enrich, grouped by table
    const placeholders = column_ids.map(() => '?').join(',');
    const [colRows] = await query(
      `SELECT ac.id, ac.column_name, ac.data_type, ac.is_pk, ac.is_fk, ac.fk_reference,
              ac.business_name as current_business_name, ac.description as current_description,
              ac.confidence_score as current_confidence,
              at.id as table_id, at.table_name, at.entity_name
       FROM app_columns ac
       JOIN app_tables at ON ac.table_id = at.id
       WHERE ac.id IN (${placeholders}) AND at.app_id = ?`,
      [...column_ids, appId]
    );

    if (colRows.length === 0) {
      return res.status(404).json({ error: 'No matching columns found' });
    }

    // Get app info
    const [appRows] = await query('SELECT * FROM applications WHERE id = ?', [appId]);
    const app = appRows[0];

    // Group columns by table
    const byTable = {};
    colRows.forEach(col => {
      if (!byTable[col.table_id]) {
        byTable[col.table_id] = { table_name: col.table_name, entity_name: col.entity_name, columns: [] };
      }
      byTable[col.table_id].columns.push(col);
    });

    // Re-enrich each table's columns with context
    const { enrichTable } = require('../services/llm-service');
    const { profileTable, hasSourceData } = require('../services/data-loader');
    const results = [];
    let totalTokens = { input_tokens: 0, output_tokens: 0, total_tokens: 0 };

    for (const [tableId, tableGroup] of Object.entries(byTable)) {
      const tableName = tableGroup.entity_name || tableGroup.table_name;

      // Get profiling data if available
      let profileData = null;
      const hasData = await hasSourceData(appId, tableGroup.table_name);
      if (hasData) {
        const colNames = tableGroup.columns.map(c => c.column_name);
        const sampleRowCount = ((app.config || {}).pipeline || {}).sample_row_count || 10;
        profileData = await profileTable(appId, tableGroup.table_name, colNames, sampleRowCount);
      }

      // Call enrichTable with context injected
      const enrichResult = await enrichTable(tableName, tableGroup.columns, {
        app_name: app.name,
        app_type: app.type,
        related_tables: [],
        context_documents: contextText,  // NEW: inject context
      }, profileData);

      if (enrichResult.token_usage) {
        totalTokens.input_tokens += enrichResult.token_usage.input_tokens;
        totalTokens.output_tokens += enrichResult.token_usage.output_tokens;
        totalTokens.total_tokens += enrichResult.token_usage.total_tokens;
      }

      // Update columns in DB
      for (const enrichedCol of enrichResult.columns) {
        const originalCol = tableGroup.columns.find(c =>
          c.column_name === enrichedCol.column_name || c.id === enrichedCol.id
        );
        if (!originalCol) continue;

        await query(
          `UPDATE app_columns
           SET business_name = ?, description = ?, confidence_score = ?,
               value_mapping = ?, enrichment_status = 'ai_enriched',
               enriched_by = 'ai+context', enriched_at = NOW()
           WHERE id = ?`,
          [
            enrichedCol.business_name || originalCol.current_business_name,
            enrichedCol.description || originalCol.current_description,
            enrichedCol.confidence_score || originalCol.current_confidence,
            enrichedCol.value_dictionary ? JSON.stringify(enrichedCol.value_dictionary) : null,
            originalCol.id,
          ]
        );

        results.push({
          column_id: originalCol.id,
          table_name: tableGroup.table_name,
          column_name: originalCol.column_name,
          old_confidence: parseFloat(originalCol.current_confidence),
          new_confidence: enrichedCol.confidence_score,
          old_business_name: originalCol.current_business_name,
          new_business_name: enrichedCol.business_name,
        });
      }
    }

    res.json({
      re_enriched: results.length,
      columns: results,
      token_usage: totalTokens,
      context_documents_used: contextRows.length,
    });
  } catch (err) {
    console.error('Re-enrich error:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

// ============================================================
// POST /api/context/:appId/merge - Merge context documents into BOKG
// This is the web app equivalent of merge_bird_descriptions.py from benchmarking.
// Strategy: BOKG descriptions win when high-confidence; external fills gaps
// for low-confidence or missing descriptions.
// ============================================================
router.post('/:appId/merge', async (req, res) => {
  try {
    const { appId } = req.params;
    const confidenceThreshold = parseFloat(req.body.confidence_threshold) || 0.50;

    // 1. Load context documents
    const [contextRows] = await query(
      `SELECT id, filename, extracted_text FROM context_documents
       WHERE app_id = ? AND extracted_text IS NOT NULL AND extracted_text != ''
       ORDER BY uploaded_at`,
      [appId]
    );

    if (contextRows.length === 0) {
      return res.status(400).json({ error: 'No context documents uploaded. Upload reference materials first.' });
    }

    const contextText = contextRows
      .map(doc => `=== Source: ${doc.filename} ===\n${doc.extracted_text}`)
      .join('\n\n');

    console.log(`[Merge] ${contextRows.length} context docs, total ${contextText.length} chars`);

    // 2. Load all tables and columns for this app (focus on low-confidence ones)
    const [tablesRows] = await query(
      `SELECT id, table_name, entity_name, description FROM app_tables WHERE app_id = ? ORDER BY table_name`,
      [appId]
    );

    const [colsRows] = await query(
      `SELECT ac.id, ac.column_name, ac.data_type, ac.business_name, ac.description,
              ac.confidence_score, ac.value_mapping, ac.enrichment_status,
              at.table_name, at.entity_name
       FROM app_columns ac
       JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = ?
       ORDER BY at.table_name, ac.column_name`,
      [appId]
    );

    // 3. Identify columns that need improvement
    const lowConfCols = colsRows.filter(c =>
      parseFloat(c.confidence_score || 0) < confidenceThreshold ||
      !c.description || c.description.trim() === '' ||
      !c.business_name || c.business_name.trim() === ''
    );

    console.log(`[Merge] ${colsRows.length} total columns, ${lowConfCols.length} below threshold (${confidenceThreshold})`);

    if (lowConfCols.length === 0) {
      return res.json({
        message: 'All columns are already well-enriched. No merge needed.',
        total_columns: colsRows.length,
        low_confidence_columns: 0,
        merged: 0,
      });
    }

    // 4. Build schema summary for the LLM
    const schemaLines = [];
    for (const table of tablesRows) {
      const tableCols = colsRows.filter(c => c.table_name === table.table_name);
      schemaLines.push(`TABLE: ${table.table_name}`);
      for (const col of tableCols) {
        const conf = parseFloat(col.confidence_score || 0);
        const marker = conf < confidenceThreshold ? ' [NEEDS IMPROVEMENT]' : '';
        schemaLines.push(`  - ${col.column_name} (${col.data_type})${marker}${col.business_name ? ` — current: ${col.business_name}` : ''}`);
      }
    }

    // 5. Use LLM to extract column descriptions from context documents
    const API_KEY = process.env.ANTHROPIC_API_KEY;
    if (!API_KEY) {
      return res.status(500).json({ error: 'ANTHROPIC_API_KEY not configured' });
    }

    // Limit context to avoid exceeding token limits
    const maxContextChars = 15000;
    const trimmedContext = contextText.length > maxContextChars
      ? contextText.substring(0, maxContextChars) + '\n[... truncated]'
      : contextText;

    const extractPrompt = `You are a data dictionary expert. Given context documents and a database schema, extract column-level descriptions.

DATABASE SCHEMA:
${schemaLines.join('\n')}

CONTEXT DOCUMENTS:
${trimmedContext}

TASK: For each column marked [NEEDS IMPROVEMENT], extract a description from the context documents.
Only include columns where the context documents actually provide useful information.
Do NOT guess or fabricate descriptions — only extract what the documents explicitly state or clearly imply.

Return a JSON array of objects, each with:
- "table_name": exact table name from the schema
- "column_name": exact column name from the schema
- "business_name": a clear, human-readable business name for this column
- "description": a concise description of what this column contains
- "value_info": (optional) any value encoding information (e.g., "A=active, I=inactive")
- "confidence": your confidence in the extraction (0.5-0.95)

Return ONLY the JSON array, no explanation or markdown. If no columns can be improved, return [].`;

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 4096,
        temperature: 0,
        messages: [{ role: 'user', content: extractPrompt }],
      })
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error('[Merge] LLM API error:', errText);
      return res.status(500).json({ error: `LLM API error: ${response.status}` });
    }

    const llmData = await response.json();
    const rawText = (llmData.content?.[0]?.text || '').trim();
    const tokenUsage = llmData.usage || {};

    // Log token usage
    try {
      const costEstimate = ((tokenUsage.input_tokens || 0) * 3 / 1000000) +
                           ((tokenUsage.output_tokens || 0) * 15 / 1000000);
      await query(
        `INSERT INTO token_usage (app_id, stage, table_name, input_tokens, output_tokens, total_tokens, model, cost_estimate)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [appId, 'context_merge', null, tokenUsage.input_tokens || 0, tokenUsage.output_tokens || 0,
         (tokenUsage.input_tokens || 0) + (tokenUsage.output_tokens || 0), 'claude-sonnet-4-20250514', costEstimate]
      );
    } catch (tokenErr) {
      console.warn('[Merge] Failed to log token usage:', tokenErr.message);
    }

    // 6. Parse the LLM response
    let extractions = [];
    try {
      // Handle potential markdown code blocks
      let jsonStr = rawText;
      const codeBlock = jsonStr.match(/```(?:json)?\s*\n?([\s\S]*?)```/);
      if (codeBlock) jsonStr = codeBlock[1].trim();
      extractions = JSON.parse(jsonStr);
      if (!Array.isArray(extractions)) extractions = [];
    } catch (parseErr) {
      console.error('[Merge] Failed to parse LLM response:', parseErr.message);
      console.error('[Merge] Raw response:', rawText.substring(0, 500));
      return res.status(500).json({
        error: 'Failed to parse LLM extraction results',
        raw_response_preview: rawText.substring(0, 200),
      });
    }

    console.log(`[Merge] LLM extracted ${extractions.length} column descriptions`);

    // 7. Apply merge logic: update columns where BOKG is weak and external is available
    const mergeResults = [];
    for (const ext of extractions) {
      if (!ext.table_name || !ext.column_name) continue;

      // Find the matching column
      const col = colsRows.find(c =>
        c.table_name.toLowerCase() === ext.table_name.toLowerCase() &&
        c.column_name.toLowerCase() === ext.column_name.toLowerCase()
      );
      if (!col) {
        console.log(`[Merge] Skipping ${ext.table_name}.${ext.column_name} — not found in schema`);
        continue;
      }

      const currentConf = parseFloat(col.confidence_score || 0);

      // BOKG wins if already high-confidence
      if (currentConf >= confidenceThreshold) {
        console.log(`[Merge] Skipping ${col.table_name}.${col.column_name} — already ${currentConf} (above threshold)`);
        continue;
      }

      // External fills the gap
      const newBizName = ext.business_name || col.business_name || col.column_name;
      const newDesc = ext.description || col.description || '';
      const newConf = Math.min(ext.confidence || 0.60, 0.85); // Cap at 0.85 — human review can push higher

      // Build value_mapping from value_info if provided
      let newValueMapping = col.value_mapping;
      if (ext.value_info && typeof ext.value_info === 'string' && ext.value_info.trim()) {
        // Try to parse "A=active, I=inactive" style into JSON
        try {
          const pairs = ext.value_info.split(/[,;]\s*/).filter(Boolean);
          const mapping = {};
          for (const pair of pairs) {
            const [k, ...vParts] = pair.split('=');
            if (k && vParts.length > 0) {
              mapping[k.trim()] = vParts.join('=').trim();
            }
          }
          if (Object.keys(mapping).length > 0) {
            newValueMapping = JSON.stringify(mapping);
          }
        } catch (e) { /* keep existing */ }
      }

      // Update the column
      await query(
        `UPDATE app_columns
         SET business_name = ?, description = ?, confidence_score = ?,
             value_mapping = COALESCE(?, value_mapping),
             enrichment_status = 'ai_enriched', enriched_by = 'ai+context_merge',
             enriched_at = NOW()
         WHERE id = ?`,
        [newBizName, newDesc, newConf, newValueMapping, col.id]
      );

      mergeResults.push({
        table_name: col.table_name,
        column_name: col.column_name,
        old_business_name: col.business_name,
        new_business_name: newBizName,
        old_confidence: currentConf,
        new_confidence: newConf,
        old_description: (col.description || '').substring(0, 80),
        new_description: (newDesc || '').substring(0, 80),
      });

      console.log(`[Merge] Updated ${col.table_name}.${col.column_name}: ${currentConf} → ${newConf} "${newBizName}"`);
    }

    console.log(`[Merge] Complete: ${mergeResults.length} columns updated out of ${extractions.length} extracted`);

    res.json({
      total_columns: colsRows.length,
      low_confidence_columns: lowConfCols.length,
      extracted: extractions.length,
      merged: mergeResults.length,
      columns: mergeResults,
      context_documents_used: contextRows.length,
      token_usage: {
        input_tokens: tokenUsage.input_tokens || 0,
        output_tokens: tokenUsage.output_tokens || 0,
        total_tokens: (tokenUsage.input_tokens || 0) + (tokenUsage.output_tokens || 0),
      },
    });
  } catch (err) {
    console.error('Context merge error:', err);
    res.status(500).json({ error: err.message || 'Internal server error' });
  }
});

module.exports = router;
