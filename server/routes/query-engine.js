const express = require('express');
const multer = require('multer');
const { query } = require('../db');
const { executeOnSourceData, hasSourceData } = require('../services/data-loader');

const router = express.Router();

// Multer config for audio uploads (in-memory, max 10MB)
const audioUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 },
});

// Cost estimates: Sonnet input=$3/MTok, output=$15/MTok
const COST_PER_INPUT_TOKEN = 3 / 1000000;
const COST_PER_OUTPUT_TOKEN = 15 / 1000000;

// Phase 2 config
const MAX_RETRIES = 2;        // Execution-based retry attempts
const SCHEMA_LINK_THRESHOLD = 15; // Tables above this trigger schema linking
// TODO: Make this a per-app builder setting stored in applications table
const DEFAULT_LLM_MODEL = 'claude-sonnet-4-20250514';

// ─────────────────────────────────────────────────────────────────────────────
// 0. COLUMN BUSINESS NAME RESOLUTION
// Simple rule: if the SQL has an explicit AS alias, humanize the alias.
// Only fall back to DB business name lookup for raw (unaliased) column names.
// ─────────────────────────────────────────────────────────────────────────────
async function buildColumnBusinessNames(appId, columns, sql) {
  const columnBusinessNames = {};
  if (!columns || columns.length === 0 || !sql) return columnBusinessNames;

  try {
    // Step 1: Find all explicit AS aliases in the SQL
    const explicitAliases = new Set();
    const asPattern = /\bAS\s+(\w+)/gi;
    let m;
    while ((m = asPattern.exec(sql)) !== null) {
      explicitAliases.add(m[1].toLowerCase());
    }

    // Step 2: For columns WITH an explicit alias, humanize the alias directly
    // The LLM chose descriptive names like order_count, total_booking_value — use them
    for (const col of columns) {
      const lower = col.toLowerCase();
      if (explicitAliases.has(lower)) {
        columnBusinessNames[lower] = lower
          .replace(/_/g, ' ')
          .replace(/\b\w/g, l => l.toUpperCase());
      }
    }

    // Step 3: For columns WITHOUT an alias (raw DB column names), look up business names
    const unmappedCols = columns.filter(c => !columnBusinessNames[c.toLowerCase()]);
    if (unmappedCols.length > 0) {
      const colNames = unmappedCols.map(c => c.toLowerCase());

      // Extract table names from SQL for scoped lookup
      const sqlTableMatches = sql.match(/"([A-Z][A-Z0-9_]+)"/g) || [];
      const sqlTableNames = [...new Set(sqlTableMatches.map(m => m.replace(/"/g, '')))];

      let tableFilter = '';
      let queryParams = [appId, colNames];
      if (sqlTableNames.length > 0) {
        tableFilter = ' AND UPPER(at.table_name) = ANY($3::text[])';
        queryParams.push(sqlTableNames.map(t => t.toUpperCase()));
      }

      const bizNames = await query(
        `SELECT ac.column_name, ac.business_name FROM app_columns ac
         JOIN app_tables at ON ac.table_id = at.id
         WHERE at.app_id = $1 AND LOWER(ac.column_name) = ANY($2::text[])
         AND ac.business_name IS NOT NULL AND ac.business_name != ''${tableFilter}`,
        queryParams
      );

      for (const row of bizNames.rows) {
        const key = row.column_name.toLowerCase();
        if (!columnBusinessNames[key]) {
          columnBusinessNames[key] = row.business_name;
        }
      }

      // Step 4: Any remaining unmapped columns — humanize them as fallback
      for (const col of unmappedCols) {
        const lower = col.toLowerCase();
        if (!columnBusinessNames[lower]) {
          columnBusinessNames[lower] = lower
            .replace(/_/g, ' ')
            .replace(/\b\w/g, l => l.toUpperCase());
        }
      }
    }
  } catch (err) {
    console.warn('[ColumnNames] Failed to build business names:', err.message);
    // Fallback: humanize all column names
    for (const col of columns) {
      const lower = col.toLowerCase();
      if (!columnBusinessNames[lower]) {
        columnBusinessNames[lower] = lower.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
      }
    }
  }

  return columnBusinessNames;
}

// ─────────────────────────────────────────────────────────────────────────────
// 0b. FORMAT VALUES FOR SPOKEN SUMMARIES
// Makes numbers human-friendly for TTS — conversational shorthand.
// ─────────────────────────────────────────────────────────────────────────────
// Produces natural spoken forms like "61.1 million dollars" or "86.8 million dollars".
// Nobody says "61 million, 105 thousand, 250 dollars and 76 cents" in a meeting —
// they say "about 61 million" or "61.1 million."
function spokenNumber(num) {
  const abs = Math.abs(num);
  const sign = num < 0 ? 'negative ' : '';

  // Small numbers (under 10,000) — just read naturally
  if (abs < 10000) {
    return null; // signal caller to use simple formatting
  }

  // Format with one decimal place at the highest magnitude
  if (abs >= 1e12) {
    const t = abs / 1e12;
    return sign + (t % 1 < 0.05 ? Math.round(t) : t.toFixed(1)) + ' trillion dollars';
  }
  if (abs >= 1e9) {
    const b = abs / 1e9;
    return sign + (b % 1 < 0.05 ? Math.round(b) : b.toFixed(1)) + ' billion dollars';
  }
  if (abs >= 1e6) {
    const m = abs / 1e6;
    return sign + (m % 1 < 0.05 ? Math.round(m) : m.toFixed(1)) + ' million dollars';
  }
  // 10K–999K: round to nearest thousand
  if (abs >= 10000) {
    const k = abs / 1000;
    return sign + (k % 1 < 0.05 ? Math.round(k) : k.toFixed(1)) + ' thousand dollars';
  }

  return null;
}

function formatSpokenValue(val, columnName) {
  if (val === null || val === undefined) return 'not available';
  if (typeof val === 'number' || (typeof val === 'string' && /^-?\d+\.?\d*$/.test(val.trim()))) {
    const num = typeof val === 'number' ? val : parseFloat(val);
    if (isNaN(num)) return String(val);
    // Detect currency columns by name patterns
    // "total" alone is NOT currency (e.g., "Total Onhand Quantity") — needs a money keyword
    const lc = (columnName || '').toLowerCase();
    const hasMoney = /amount|revenue|booking|collection|balance|cost|price|payment|invoice|credit|debit|avg_amount|dollar|usd|spend/i.test(lc);
    const isTotal = /^total\b/i.test(lc) && !hasMoney;
    const isCurrency = hasMoney || (/value/i.test(lc) && !/quantity|count|score|rating|flag/i.test(lc));
    if (isCurrency) {
      // For large currency values, use spoken-word format so TTS reads correctly
      const spoken = spokenNumber(num);
      if (spoken) return spoken;
      // Small currency — simple dollar format is fine for TTS
      return '$' + num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }
    // Large non-currency numbers — shorthand spoken format
    if (Math.abs(num) >= 10000) {
      const abs = Math.abs(num);
      const sign = num < 0 ? 'negative ' : '';
      if (abs >= 1e9) { const b = abs / 1e9; return sign + (b % 1 < 0.05 ? Math.round(b) : b.toFixed(1)) + ' billion'; }
      if (abs >= 1e6) { const m = abs / 1e6; return sign + (m % 1 < 0.05 ? Math.round(m) : m.toFixed(1)) + ' million'; }
      if (abs >= 10000) { const k = abs / 1000; return sign + (k % 1 < 0.05 ? Math.round(k) : k.toFixed(1)) + ' thousand'; }
    }
    // Large integers get commas
    if (Number.isInteger(num) || Math.abs(num - Math.round(num)) < 0.001) {
      return Math.round(num).toLocaleString('en-US');
    }
    // Other decimals — 2 places max
    return num.toLocaleString('en-US', { maximumFractionDigits: 2 });
  }
  return String(val);
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. ROBUST SQL EXTRACTION
// Handles markdown code blocks, SQL: prefixes, explanatory text around SQL
// ─────────────────────────────────────────────────────────────────────────────
function extractSQL(rawText) {
  let text = (rawText || '').trim();

  // Handle structured format: extract SQL from ### SQL section
  const sqlSectionMatch = text.match(/###\s*SQL\s*\n([\s\S]*?)(?=\n###|\n\*\*|$)/i);
  if (sqlSectionMatch) {
    text = sqlSectionMatch[1].trim();
  }

  // Strip markdown code fences (```sql ... ``` or ``` ... ```)
  const codeBlockMatch = text.match(/```(?:sql|postgresql|pg)?\s*\n?([\s\S]*?)```/i);
  if (codeBlockMatch) {
    text = codeBlockMatch[1].trim();
  }

  // Strip "SQL:" or "Here is the SQL:" prefix
  text = text.replace(/^(?:here\s+is\s+)?(?:the\s+)?(?:sql|query)\s*:\s*/i, '').trim();

  // If there's explanatory text after the SQL (often starts with a blank line then text),
  // try to isolate just the SELECT statement
  const selectMatch = text.match(/((?:WITH|SELECT)\s[\s\S]*?)(;?\s*$)/i);
  if (selectMatch) {
    text = selectMatch[1].trim();
    // Remove trailing semicolons (PG doesn't need them and they can cause issues)
    text = text.replace(/;\s*$/, '').trim();
  }

  // Remove any remaining triple-backtick artifacts
  text = text.replace(/```/g, '').trim();

  return text;
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. CASE-FIX POST-PROCESSING
// Uses value dictionaries from enrichment to fix string case mismatches in SQL.
// e.g., WHERE "type" = 'vydaj' → WHERE "type" = 'VYDAJ'
// ─────────────────────────────────────────────────────────────────────────────
async function loadValueDictionaries(appId) {
  const result = await query(
    `SELECT ac.column_name, ac.value_mapping, at.table_name
     FROM app_columns ac
     JOIN app_tables at ON ac.table_id = at.id
     WHERE at.app_id = $1 AND ac.value_mapping IS NOT NULL`,
    [appId]
  );

  // Build lookup: { "table.column": { "lowercase_value": "exact_value", ... } }
  const dictionaries = {};
  for (const row of result.rows) {
    try {
      const vm = typeof row.value_mapping === 'string' ? JSON.parse(row.value_mapping) : row.value_mapping;
      if (vm && typeof vm === 'object' && Object.keys(vm).length > 0) {
        const key = `${row.table_name}.${row.column_name}`;
        dictionaries[key] = {};
        // Map: lowercase original value → actual value
        for (const val of Object.keys(vm)) {
          dictionaries[key][String(val).toLowerCase()] = String(val);
        }
        // Also store just by column name for simpler matching
        if (!dictionaries[row.column_name]) dictionaries[row.column_name] = {};
        for (const val of Object.keys(vm)) {
          dictionaries[row.column_name][String(val).toLowerCase()] = String(val);
        }
      }
    } catch (e) { /* skip parse errors */ }
  }
  return dictionaries;
}

/**
 * For SQLite: add COLLATE NOCASE to string literal comparisons in WHERE/ON/HAVING clauses.
 * SQLite is case-sensitive for = comparisons by default.
 * This deterministically adds COLLATE NOCASE after quoted string comparisons.
 * Pattern: = 'value' → = 'value' COLLATE NOCASE  (also != and <>)
 * Skips if COLLATE NOCASE is already present.
 */
function addCollateNocase(sql) {
  if (!sql) return sql;
  // Match: comparison_operator followed by a single-quoted string, NOT already followed by COLLATE
  // Handles: = 'value', != 'value', <> 'value', = 'value with spaces'
  return sql.replace(/(=\s*'[^']*')(?!\s*COLLATE)/gi, '$1 COLLATE NOCASE');
}

/**
 * Fix COLLATE NOCASE breaking strings that contain apostrophes.
 * The LLM + addCollateNocase can produce:
 *   WHERE name = 'Ancestor' COLLATE NOCASE's Chosen'
 * This should be:
 *   WHERE name = 'Ancestor''s Chosen' COLLATE NOCASE
 *
 * The regex iteratively finds 'text' COLLATE NOCASE's more' patterns
 * and merges the split string, moving COLLATE NOCASE to the end.
 */
function fixApostropheCollate(sql) {
  if (!sql) return sql;
  let result = sql;
  const maxIterations = 10;
  for (let i = 0; i < maxIterations; i++) {
    const next = result.replace(
      /'([^']*)'\s*COLLATE\s+NOCASE's\s*([^']*)'/g,
      "'$1''s $2' COLLATE NOCASE"
    );
    if (next === result) break;
    result = next;
  }
  return result;
}

/**
 * Fix abbreviated year ranges in string literals.
 * The LLM sometimes generates '2014-15' when the database stores '2014-2015'.
 * Expands: '2014-15' → '2014-2015'
 * Skips months: '2009-04' stays unchanged (04 <= 12, so it's April).
 */
function fixYearFormat(sql) {
  if (!sql) return sql;
  return sql.replace(/'(\d{4})-(\d{2})'/g, (match, fullStart, abbrevEnd) => {
    // If abbrevEnd is 01-12, it's likely a month — leave unchanged
    if (parseInt(abbrevEnd, 10) <= 12) return match;
    const century = fullStart.substring(0, 2);
    return `'${fullStart}-${century}${abbrevEnd}'`;
  });
}

/**
 * Fix 2-digit years in LIKE patterns.
 * The LLM sometimes generates '81-11-%' when the database stores '1981-11-%'.
 * Heuristic: years > 25 use 19xx, others use 20xx.
 */
function fix2DigitYearLike(sql) {
  if (!sql) return sql;
  return sql.replace(/'(\d{2})-(\d{2}-[%\d]+')/g, (match, yy, rest) => {
    const century = parseInt(yy, 10) > 25 ? '19' : '20';
    return `'${century}${yy}-${rest}`;
  });
}

function applyCaseFix(sql, valueDictionaries) {
  if (!sql || Object.keys(valueDictionaries).length === 0) return sql;

  let fixedSql = sql;
  let fixesApplied = [];

  // Match patterns: "column_name" = 'value' or "column_name" IN ('val1', 'val2')
  // Also handles: "column_name" LIKE 'value%', "column_name" != 'value'
  const stringLiteralPattern = /"(\w+)"\s*(?:=|!=|<>|LIKE|ILIKE|IN\s*\()\s*'([^']+)'/gi;

  let match;
  while ((match = stringLiteralPattern.exec(sql)) !== null) {
    const colName = match[1];
    const originalValue = match[2];
    const lowerValue = originalValue.toLowerCase();

    // Check if we have a dictionary for this column
    const dict = valueDictionaries[colName];
    if (dict && dict[lowerValue] && dict[lowerValue] !== originalValue) {
      const correctValue = dict[lowerValue];
      fixedSql = fixedSql.replace(
        new RegExp(`'${escapeRegex(originalValue)}'`, 'g'),
        `'${correctValue}'`
      );
      fixesApplied.push(`${originalValue} → ${correctValue}`);
    }
  }

  // Also fix values inside IN (...) clauses
  const inClausePattern = /"(\w+)"\s*IN\s*\(([^)]+)\)/gi;
  while ((match = inClausePattern.exec(fixedSql)) !== null) {
    const colName = match[1];
    const valuesStr = match[2];
    const dict = valueDictionaries[colName];
    if (!dict) continue;

    const values = valuesStr.match(/'([^']+)'/g);
    if (!values) continue;

    for (const quotedVal of values) {
      const val = quotedVal.slice(1, -1); // strip quotes
      const lowerVal = val.toLowerCase();
      if (dict[lowerVal] && dict[lowerVal] !== val) {
        fixedSql = fixedSql.replace(`'${val}'`, `'${dict[lowerVal]}'`);
        fixesApplied.push(`${val} → ${dict[lowerVal]}`);
      }
    }
  }

  if (fixesApplied.length > 0) {
    console.log(`  Case-fix applied: ${fixesApplied.join(', ')}`);
  }

  return fixedSql;
}

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. FEW-SHOT QPD (Query Pattern Dictionary) INJECTION
// Retrieves relevant proven query patterns from test_queries with thumbs_up
// feedback and injects them as examples in the prompt.
// ─────────────────────────────────────────────────────────────────────────────
async function getFewShotExamples(appId, question, maxExamples = 3) {
  try {
    // Stopwords for relevance scoring
    const STOPWORDS = new Set(['the', 'what', 'how', 'many', 'which', 'are', 'was', 'were', 'has',
      'have', 'does', 'for', 'from', 'with', 'that', 'this', 'all', 'and', 'the', 'show', 'list',
      'give', 'tell', 'find', 'get', 'please', 'name', 'number', 'total', 'each', 'per']);
    const questionWords = new Set(
      question.toLowerCase()
        .replace(/[^a-z0-9\s]/g, '')
        .split(/\s+/)
        .filter(w => w.length > 2 && !STOPWORDS.has(w))
    );

    function scoreRelevance(text) {
      const words = new Set(
        (text || '').toLowerCase().replace(/[^a-z0-9\s]/g, '').split(/\s+/).filter(w => w.length > 2)
      );
      let overlap = 0;
      for (const w of questionWords) {
        if (words.has(w)) overlap++;
      }
      return overlap;
    }

    // Source 1: User-verified queries from test_queries (highest priority)
    const verifiedResult = await query(
      `SELECT nl_query, generated_sql, execution_result
       FROM test_queries
       WHERE app_id = $1 AND feedback = 'thumbs_up'
       ORDER BY created_at DESC
       LIMIT 20`,
      [appId]
    );

    // Source 2: Seeded query patterns from query_patterns (QPD)
    const patternsResult = await query(
      `SELECT nl_template AS nl_query, sql_template AS generated_sql, tables_used, confidence
       FROM query_patterns
       WHERE app_id = $1 AND status = 'active' AND sql_template IS NOT NULL
       ORDER BY confidence DESC, created_at DESC
       LIMIT 50`,
      [appId]
    );

    // Score all candidates
    const allCandidates = [];

    // Verified queries get a 2x boost
    for (const row of verifiedResult.rows) {
      let wasSuccessful = true;
      try {
        const execResult = typeof row.execution_result === 'string'
          ? JSON.parse(row.execution_result) : row.execution_result;
        if (execResult?.error || execResult?.row_count === 0) wasSuccessful = false;
      } catch (e) { /* assume success */ }
      const overlap = scoreRelevance(row.nl_query);
      allCandidates.push({
        nl_query: row.nl_query,
        generated_sql: row.generated_sql,
        score: (wasSuccessful ? overlap : overlap * 0.5) * 2.0,
        source: 'verified',
      });
    }

    // Seeded patterns (from BIRD QPD or builder-generated)
    for (const row of patternsResult.rows) {
      const overlap = scoreRelevance(row.nl_query);
      allCandidates.push({
        nl_query: row.nl_query,
        generated_sql: row.generated_sql,
        score: overlap * (parseFloat(row.confidence) || 0.8),
        source: 'qpd',
      });
    }

    // Sort by relevance and take top N
    allCandidates.sort((a, b) => b.score - a.score);
    const topExamples = allCandidates.slice(0, maxExamples).filter(e => e.score > 0);

    if (topExamples.length === 0) return '';

    const exampleLines = ['PROVEN QUERY PATTERNS (these queries are verified correct — use them as templates):'];
    for (const ex of topExamples) {
      exampleLines.push(`Q: ${ex.nl_query}`);
      exampleLines.push(`SQL: ${ex.generated_sql}`);
      exampleLines.push('');
    }
    exampleLines.push('Use these patterns as guidance for similar queries. Match their style for JOINs, aggregations, column selection, and value literals.');
    exampleLines.push('');

    return exampleLines.join('\n');
  } catch (err) {
    console.warn('Failed to load few-shot examples:', err.message);
    return '';
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 3b. CONTEXT DOCUMENT INJECTION (Merged Enrichment — ported from benchmark)
// Loads uploaded context documents and formats them for injection into the
// NL2SQL prompt. This was the single biggest accuracy driver in benchmarking:
// adding external business context alongside BOKG descriptions.
// Strategy: BOKG context is primary; context docs supplement with domain
// knowledge, business rules, value encodings, and column semantics that
// the automated enrichment may have missed.
// ─────────────────────────────────────────────────────────────────────────────
async function getContextDocumentSection(appId, maxChars = 8000) {
  try {
    const result = await query(
      `SELECT filename, extracted_text FROM context_documents
       WHERE app_id = $1 AND extracted_text IS NOT NULL AND extracted_text != ''
       ORDER BY uploaded_at`,
      [appId]
    );

    if (result.rows.length === 0) return '';

    // Build context section, respecting token budget
    const sections = [];
    let totalChars = 0;

    for (const doc of result.rows) {
      const text = (doc.extracted_text || '').trim();
      if (!text) continue;

      // Truncate individual docs if needed to stay within budget
      const remaining = maxChars - totalChars;
      if (remaining <= 200) break; // Not enough room for another doc

      const truncated = text.length > remaining
        ? text.substring(0, remaining) + '\n[... truncated for length]'
        : text;

      sections.push(`--- ${doc.filename} ---\n${truncated}`);
      totalChars += truncated.length;
    }

    if (sections.length === 0) return '';

    return `\nBUSINESS CONTEXT DOCUMENTS (reference material uploaded by the data steward):
Use this information to understand column meanings, business rules, value encodings,
and domain terminology. When this context contradicts the schema descriptions above,
prefer the context documents — they contain domain expert knowledge.

${sections.join('\n\n')}

`;
  } catch (err) {
    console.warn('Failed to load context documents:', err.message);
    return '';
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. SCHEMA LINKING (for schemas > 15 tables)
// Pre-selects relevant tables/columns based on the question before building
// the full BOKG context, reducing noise for the SQL generator.
// ─────────────────────────────────────────────────────────────────────────────
// Stopwords for keyword schema linking — shared between table and column linking
const SCHEMA_LINK_STOPWORDS = new Set([
  'the', 'a', 'an', 'is', 'are', 'was', 'were', 'what', 'which', 'who',
  'how', 'many', 'much', 'does', 'do', 'did', 'has', 'have', 'had', 'been',
  'be', 'will', 'would', 'could', 'should', 'can', 'may', 'might', 'shall',
  'of', 'in', 'on', 'at', 'to', 'for', 'with', 'by', 'from', 'as', 'into',
  'through', 'during', 'before', 'after', 'and', 'or', 'but', 'not', 'no',
  'if', 'then', 'than', 'that', 'this', 'these', 'those', 'it', 'its',
  'all', 'each', 'every', 'any', 'some', 'most', 'more', 'less', 'other',
  'out', 'up', 'down', 'over', 'under', 'between', 'about', 'above', 'below',
  'please', 'list', 'find', 'show', 'give', 'tell', 'get', 'make',
  'number', 'total', 'average', 'count', 'percentage', 'among', 'per'
]);

function tokenizeQuestion(text) {
  return new Set(
    (text || '').toLowerCase()
      .match(/[a-z][a-z_]+/g || [])
      ?.filter(w => !SCHEMA_LINK_STOPWORDS.has(w) && w.length > 2) || []
  );
}

const COLUMN_LINK_THRESHOLD = 20; // Tables with more columns than this get column-level filtering

async function schemaLink(appId, question, tableLinkThreshold = null) {
  const threshold = tableLinkThreshold || SCHEMA_LINK_THRESHOLD;
  // Get table count
  const tableCountResult = await query(
    'SELECT COUNT(*) as cnt FROM app_tables WHERE app_id = $1',
    [appId]
  );
  const tableCount = parseInt(tableCountResult.rows[0].cnt);

  // If small schema, no linking needed — return null to use full context
  if (tableCount <= threshold) return null;

  // For larger schemas: use keyword matching + semantic similarity + relationship traversal
  // Run all three data fetches in parallel for zero added latency
  const { semanticSchemaLink } = require('../services/embedding-service');
  const [allTables, fkCounts, semanticMatches] = await Promise.all([
    query(
      `SELECT at.id, at.table_name, at.entity_name, at.description, at.row_count,
              STRING_AGG(ac.column_name || ' ' || COALESCE(ac.business_name, '') || ' ' || COALESCE(ac.description, ''), ' ') as col_text
       FROM app_tables at
       LEFT JOIN app_columns ac ON at.id = ac.table_id
       WHERE at.app_id = $1
       GROUP BY at.id, at.table_name, at.entity_name, at.description, at.row_count`,
      [appId]
    ),
    // Graph centrality: count incoming FK references per table.
    // Parent/driving tables (orders, gl_account) have more children pointing to them,
    // making this a better tiebreaker than row count (which favors detail tables).
    query(
      `SELECT to_table_id, COUNT(*) as incoming_fk_count
       FROM app_relationships WHERE app_id = $1
       GROUP BY to_table_id`,
      [appId]
    ),
    // Semantic similarity: embed the question, find tables whose metadata is semantically close.
    // Returns [] gracefully if embeddings aren't generated yet or OpenAI is unavailable.
    semanticSchemaLink(appId, question, 15, 0.25).catch(() => [])
  ]);

  // Build centrality lookup
  const centralityMap = {};
  for (const row of fkCounts.rows) {
    centralityMap[row.to_table_id] = parseInt(row.incoming_fk_count) || 0;
  }

  // Build semantic similarity lookup (table_id → similarity score 0..1)
  const semanticMap = {};
  for (const match of semanticMatches) {
    semanticMap[match.table_id] = match.similarity;
  }
  if (semanticMatches.length > 0) {
    console.log(`[SchemaLink] Semantic matches for "${question.substring(0, 50)}": ${semanticMatches.slice(0, 5).map(m => `${m.table_name}(${m.similarity.toFixed(3)})`).join(', ')}`);
  }

  // Tokenize the question with stopword filtering
  const qWords = tokenizeQuestion(question);
  const questionLower = question.toLowerCase();

  // Score each table based on keyword overlap with metadata
  const scored = allTables.rows.map(t => {
    const tableName = (t.table_name || '').toLowerCase();
    const entityName = (t.entity_name || '').toLowerCase();
    const searchText = [
      t.table_name, t.entity_name, t.description, t.col_text
    ].filter(Boolean).join(' ').toLowerCase();

    let score = 0;

    // Direct table name mention in question = strong signal
    if (questionLower.includes(tableName)) score += 5;

    // Split table name into component words for fuzzy matching
    const tableWords = tableName.split('_').filter(w => w.length > 2);

    for (const word of qWords) {
      if (searchText.includes(word)) {
        score++;
        // Bonus for table/entity name match
        if (tableName.includes(word)) score += 2;
        if (entityName.includes(word)) score += 2;
      }
      // Fuzzy match: handle plurals/stems — "orders" matches "order", "categories" matches "category"
      // Check if question word shares a stem (first N-1 chars) with any table name word
      const wordStem = word.length > 4 ? word.substring(0, word.length - 1) : word;
      for (const tw of tableWords) {
        const twStem = tw.length > 4 ? tw.substring(0, tw.length - 1) : tw;
        if (tw.startsWith(wordStem) || word.startsWith(twStem)) {
          score += 2; // Stem match in table name is a solid signal
          break;
        }
      }
    }

    // Graph centrality tiebreaker: parent/driving tables have more incoming FK references.
    // This scales to any schema size without requiring row counts (no COUNT(*) at startup).
    const centrality = centralityMap[t.id] || 0;
    if (score > 0 && centrality > 0) {
      score += Math.min(centrality, 3); // max +3 points for highly-referenced tables
    }

    // Semantic similarity boost: vector embeddings catch vocabulary gaps that keywords miss.
    // "spend" → AP_INVOICES, "stock" → INVENTORY_ITEM, etc.
    // Scale: similarity 0.25-0.50 maps to +2..+8 points (significant but not overwhelming).
    // Tables with NO keyword match but strong semantic match still get surfaced.
    const similarity = semanticMap[t.id] || 0;
    if (similarity > 0) {
      const semanticBoost = Math.round(similarity * 16); // 0.25→4, 0.35→6, 0.50→8
      score += semanticBoost;
    }

    return { table_id: t.id, table_name: t.table_name, score, centrality, similarity };
  });

  // Sort by score descending, then by similarity, then by centrality
  scored.sort((a, b) => b.score - a.score || b.similarity - a.similarity || b.centrality - a.centrality);

  // Take top tables (at least 2, up to 10) that scored > 0
  let selectedIds = scored.filter(t => t.score > 0).slice(0, 10).map(t => t.table_id);

  // Always include at least 2 tables (most queries need a join)
  if (selectedIds.length < 2) {
    selectedIds = scored.slice(0, 5).map(t => t.table_id);
  }

  // BFS join path expansion: find shortest paths between all selected tables,
  // automatically including intermediate/bridge tables needed for multi-hop joins.
  // Replaces the old 1-hop expansion which missed multi-hop paths.
  try {
    const { findJoinPaths } = require('../services/join-path-service');
    const { bridgeTableIds, joinPaths } = await findJoinPaths(appId, selectedIds);

    if (bridgeTableIds.length > 0) {
      console.log(`  BFS join expansion: adding ${bridgeTableIds.length} bridge tables:`,
        bridgeTableIds.map(id => scored.find(s => s.table_id === id)?.table_name || `id:${id}`).join(', '));
      for (const bridgeId of bridgeTableIds) {
        if (!selectedIds.includes(bridgeId)) {
          selectedIds.push(bridgeId);
        }
      }
    }

    // Stash join paths on the function for buildBOKGContext to pick up
    schemaLink._lastJoinPaths = joinPaths;
  } catch (e) {
    console.warn('  BFS join expansion failed (falling back to 1-hop):', e.message);
    // Fallback: old 1-hop expansion
    const relResult = await query(
      `SELECT DISTINCT
         CASE WHEN from_table_id = ANY($1::int[]) THEN to_table_id ELSE from_table_id END as connected_id
       FROM app_relationships
       WHERE app_id = $2 AND (from_table_id = ANY($1::int[]) OR to_table_id = ANY($1::int[]))`,
      [selectedIds, appId]
    );
    for (const row of relResult.rows) {
      if (!selectedIds.includes(row.connected_id)) {
        selectedIds.push(row.connected_id);
      }
    }
    schemaLink._lastJoinPaths = null;
  }

  console.log(`  Schema linking: ${tableCount} tables → ${selectedIds.length} relevant tables selected`);
  return selectedIds;
}

/**
 * Column-level schema linking: for wide tables, select only relevant columns
 * to reduce prompt bloat. Always includes PKs and FKs.
 *
 * @param {string} question - The user's NL question
 * @param {Array} columns - Array of column objects from the database
 * @param {string} tableName - The table these columns belong to
 * @returns {Set|null} Set of column names to include, or null if no filtering needed
 */
function columnLink(question, columns, tableName, colLinkThreshold = null) {
  const threshold = colLinkThreshold || COLUMN_LINK_THRESHOLD;
  if (!columns || columns.length <= threshold) {
    return null; // Small table — include everything
  }

  const qWords = tokenizeQuestion(question);
  const questionLower = question.toLowerCase();
  const alwaysInclude = new Set();

  // Phase 1: Always include PKs and FKs
  for (const col of columns) {
    const colName = col.column_name || '';
    if (col.is_pk) { alwaysInclude.add(colName); continue; }
    if (col.is_fk) { alwaysInclude.add(colName); continue; }
    // Heuristic: columns ending in _id are likely join keys
    if (colName.toLowerCase().endsWith('_id') || colName.toLowerCase().endsWith('id')) {
      alwaysInclude.add(colName);
    }
    // PK/FK hints in description
    const descLower = ((col.description || '') + ' ' + (col.business_name || '')).toLowerCase();
    if (descLower.includes('primary key') || descLower.includes('unique identifier') ||
        descLower.includes('foreign key') || descLower.includes('references')) {
      alwaysInclude.add(colName);
    }
  }

  // Phase 2: Score remaining columns by question relevance
  const colScores = [];
  for (const col of columns) {
    const colName = col.column_name || '';
    if (alwaysInclude.has(colName)) continue;

    let score = 0;
    const colNameLower = colName.toLowerCase().replace(/_/g, ' ');
    const bnameLower = (col.business_name || '').toLowerCase();
    const descLower = (col.description || '').toLowerCase();
    const searchText = `${colNameLower} ${bnameLower} ${descLower}`;

    for (const word of qWords) {
      if (searchText.includes(word)) {
        score += 2;
        if (colNameLower.includes(word)) score += 3; // Direct column name match
        if (bnameLower.includes(word)) score += 2;   // Business name match
      }
    }

    // Bonus: numeric columns when question asks about amounts/counts/averages
    const numericWords = ['much', 'many', 'total', 'average', 'sum', 'count',
      'highest', 'lowest', 'maximum', 'minimum', 'percent', 'ratio',
      'rate', 'amount', 'price', 'cost', 'salary', 'balance', 'revenue'];
    if (numericWords.some(w => questionLower.includes(w))) {
      if (['amount', 'numeric', 'count', 'total', 'price', 'balance', 'rate', 'percentage', 'salary', 'cost']
          .some(t => descLower.includes(t))) {
        score += 1;
      }
    }

    colScores.push({ colName, score });
  }

  // Phase 3: Select top columns to fill remaining slots
  const remainingSlots = threshold - alwaysInclude.size;
  if (remainingSlots <= 0) return alwaysInclude;

  colScores.sort((a, b) => b.score - a.score);
  for (const { colName } of colScores.slice(0, remainingSlots)) {
    alwaysInclude.add(colName);
  }

  const omitted = columns.length - alwaysInclude.size;
  if (omitted > 0) {
    console.log(`  Column linking [${tableName}]: ${columns.length} columns → ${alwaysInclude.size} selected (${omitted} omitted)`);
  }
  return alwaysInclude;
}

/**
 * Build BOKG context for NL-to-SQL prompt
 * Retrieves enriched entity metadata, column descriptions, value dictionaries,
 * and relationship info to give the LLM maximum context for SQL generation.
 *
 * @param {number} appId
 * @param {number[]|null} linkedTableIds - If provided, only include these tables (schema linking)
 * @param {string|null} question - If provided, enables column-level filtering for wide tables
 */
async function buildBOKGContext(appId, linkedTableIds = null, question = null, colLinkThreshold = null) {
  // Get tables with entity metadata (optionally filtered by schema linking)
  let tablesQuery = `SELECT id, table_name, entity_name, description, entity_metadata, row_count
     FROM app_tables WHERE app_id = $1`;
  const tablesParams = [appId];

  if (linkedTableIds && linkedTableIds.length > 0) {
    tablesQuery += ` AND id = ANY($2::int[])`;
    tablesParams.push(linkedTableIds);
  }
  tablesQuery += ` ORDER BY table_name`;

  const tablesResult = await query(tablesQuery, tablesParams);

  // Get enriched columns (for selected tables only if schema-linked)
  let colsQuery = `SELECT ac.column_name, ac.data_type, ac.is_pk, ac.is_fk, ac.fk_reference,
            ac.business_name, ac.description, ac.value_mapping, ac.column_role,
            at.table_name
     FROM app_columns ac
     JOIN app_tables at ON ac.table_id = at.id
     WHERE at.app_id = $1`;
  const colsParams = [appId];

  if (linkedTableIds && linkedTableIds.length > 0) {
    colsQuery += ` AND at.id = ANY($2::int[])`;
    colsParams.push(linkedTableIds);
  }
  colsQuery += ` ORDER BY at.table_name, ac.column_name`;

  const colsResult = await query(colsQuery, colsParams);

  // Get relationships (include all for completeness — they're small)
  const relsResult = await query(
    `SELECT ft.table_name as from_table, ar.from_column,
            tt.table_name as to_table, ar.to_column, ar.rel_type, ar.cardinality
     FROM app_relationships ar
     JOIN app_tables ft ON ar.from_table_id = ft.id
     JOIN app_tables tt ON ar.to_table_id = tt.id
     WHERE ar.app_id = $1`,
    [appId]
  );

  // Build relationship lookup: table.column → target_table.target_column
  const relLookup = {};
  for (const rel of relsResult.rows) {
    relLookup[`${rel.from_table}.${rel.from_column}`] = `${rel.to_table}.${rel.to_column}`;
  }

  // Build context string
  const sections = [];

  sections.push('DATABASE SCHEMA AND BUSINESS CONTEXT:');
  sections.push('');

  for (const table of tablesResult.rows) {
    const meta = table.entity_metadata || {};
    const tableCols = colsResult.rows.filter(c => c.table_name === table.table_name);

    // Column-level filtering: for wide tables, only include relevant columns
    const selectedCols = question ? columnLink(question, tableCols, table.table_name, colLinkThreshold) : null;

    sections.push(`TABLE: "${table.table_name}" (${table.row_count || 0} rows)`);
    if (table.description) sections.push(`  Description: ${table.description}`);
    if (meta.entity_type) sections.push(`  Entity Type: ${meta.entity_type}`);
    sections.push(`  Columns:`);

    let omittedCount = 0;
    for (const col of tableCols) {
      // Skip columns filtered out by column linking
      if (selectedCols && !selectedCols.has(col.column_name)) {
        omittedCount++;
        continue;
      }

      let colLine = `    - "${col.column_name}" ${col.data_type}`;
      if (col.is_pk) colLine += ' [PK]';
      if (col.is_fk) {
        // Use relationship lookup for accurate FK target
        const fkTarget = relLookup[`${table.table_name}.${col.column_name}`] || col.fk_reference || '?';
        colLine += ` [FK → ${fkTarget}]`;
      }
      // Add role annotation — this tells the SQL generator how to use the column
      if (col.column_role) {
        const roleLabels = {
          surrogate_key: 'SURROGATE-KEY: use for JOINs only, never SELECT for display',
          natural_key: 'DISPLAY-KEY: use this to identify/display this entity to users',
          measure: 'MEASURE',
          dimension: 'DIMENSION',
          description_col: 'DISPLAY-TEXT',
          flag: 'FLAG',
          date: 'DATE',
          technical: 'TECHNICAL: rarely needed in queries',
          fk_only: 'FK-ONLY: use for JOINs only',
        };
        const label = roleLabels[col.column_role] || col.column_role;
        colLine += ` [${label}]`;
      }
      if (col.business_name) colLine += ` — ${col.business_name}`;
      sections.push(colLine);

      if (col.description) {
        sections.push(`      ${col.description}`);
      }

      // Include value dictionary for coded columns
      if (col.value_mapping) {
        try {
          const vm = typeof col.value_mapping === 'string' ? JSON.parse(col.value_mapping) : col.value_mapping;
          if (vm && typeof vm === 'object' && Object.keys(vm).length > 0 && Object.keys(vm).length <= 30) {
            const entries = Object.entries(vm).map(([k, v]) => `${k}=${v}`).join(', ');
            sections.push(`      Values: ${entries}`);
          }
        } catch (e) { /* ignore parse errors */ }
      }
    }
    if (omittedCount > 0) {
      sections.push(`    (${omittedCount} other columns omitted — not relevant to this question)`);
    }
    sections.push('');
  }

  // Add relationships as explicit join paths
  if (relsResult.rows.length > 0) {
    // Filter to only relationships involving selected tables
    const selectedTableNames = new Set(tablesResult.rows.map(t => t.table_name));
    const relevantRels = relsResult.rows.filter(
      r => selectedTableNames.has(r.from_table) || selectedTableNames.has(r.to_table)
    );

    if (relevantRels.length > 0) {
      sections.push('RELATIONSHIPS (JOIN PATHS):');
      sections.push('Use these to determine how to JOIN tables. Use the shortest path.');
      for (const rel of relevantRels) {
        sections.push(`  "${rel.from_table}"."${rel.from_column}" → "${rel.to_table}"."${rel.to_column}" (${rel.rel_type}, ${rel.cardinality || 'many_to_one'})`);
      }
      sections.push('');
    }
  }

  // Add SYNONYMS section — helps LLM understand business language → column/table mapping
  try {
    const synsResult = await query(
      `SELECT s.term, ac.column_name, at.table_name, ac.business_name, s.column_id
       FROM app_synonyms s
       LEFT JOIN app_columns ac ON s.column_id = ac.id
       JOIN app_tables at ON s.table_id = at.id
       WHERE s.app_id = $1 AND s.status = 'active'
       ORDER BY at.table_name, COALESCE(ac.column_name, ''), s.term`,
      [appId]
    );

    if (synsResult.rows.length > 0) {
      // Separate table-level and column-level synonyms
      const synGroups = {};
      const tableSynGroups = {};
      for (const s of synsResult.rows) {
        if (s.column_id) {
          // Column-level synonym
          const key = `"${s.table_name}"."${s.column_name}"`;
          if (!synGroups[key]) synGroups[key] = { column: key, business_name: s.business_name, terms: [] };
          synGroups[key].terms.push(s.term);
        } else {
          // Table-level synonym (column_id = NULL)
          const key = `"${s.table_name}"`;
          if (!tableSynGroups[key]) tableSynGroups[key] = { table: key, terms: [] };
          tableSynGroups[key].terms.push(s.term);
        }
      }

      sections.push('SYNONYMS (BUSINESS LANGUAGE):');

      // Table-level synonyms first — these tell the LLM which table to query
      // Cross-reference with computed measures so the LLM sees the formula inline
      if (Object.keys(tableSynGroups).length > 0) {
        const tableMeasureLookup = {};
        for (const table of tablesResult.rows) {
          const meta = table.entity_metadata || {};
          if (meta.computed_measures && meta.computed_measures.length > 0) {
            tableMeasureLookup[`"${table.table_name}"`] = meta.computed_measures;
          }
        }

        sections.push('These terms are BUSINESS METRICS that represent DOLLAR AMOUNTS by default.');
        sections.push('CRITICAL RULE: When a DOLLAR VALUE formula is shown below, ALWAYS use it — even if the user does not say "amount" or "total".');
        sections.push('For example, "show me bookings by month" means the DOLLAR VALUE of bookings per month, NOT a count of orders.');
        sections.push('Only use COUNT(*) if the user explicitly says "how many", "count", or "number of".');
        sections.push('Only show individual rows if the user explicitly asks to "list" or "detail" records.');
        for (const [key, group] of Object.entries(tableSynGroups)) {
          const measures = tableMeasureLookup[key];
          if (measures && measures.length > 0) {
            const m = measures[0]; // Primary computed measure
            // Formula already contains aggregation (e.g. SUM(...)) — don't double-wrap
            let formula = `ALWAYS USE: ${m.formula}`;
            if (m.join_table) formula += ` (requires JOIN to "${m.join_table}" ON ${m.join_on})`;
            if (m.description) formula += ` — ${m.description}`;
            sections.push(`  ${group.terms.join(', ')} → ${key} — ${formula}`);
          } else {
            sections.push(`  ${group.terms.join(', ')} → ${key}`);
          }
        }
      }

      // Column-level synonyms
      if (Object.keys(synGroups).length > 0) {
        sections.push('When the user says any of these terms, they mean the specified column:');
        for (const [key, group] of Object.entries(synGroups)) {
          sections.push(`  ${group.terms.join(', ')} → ${key}${group.business_name ? ` (${group.business_name})` : ''}`);
        }
      }
      sections.push('');
    }
  } catch (e) {
    // Synonyms table may not exist yet — skip gracefully
  }

  // Add DISPLAY KEY GUIDANCE — dynamically generated from column_role metadata
  // This tells the LLM which columns to SELECT for display vs. which to use only for JOINs
  const displayKeyRules = [];
  const tableColsByRole = {};

  for (const col of colsResult.rows) {
    if (!tableColsByRole[col.table_name]) tableColsByRole[col.table_name] = {};
    if (col.column_role) {
      if (!tableColsByRole[col.table_name][col.column_role]) {
        tableColsByRole[col.table_name][col.column_role] = [];
      }
      tableColsByRole[col.table_name][col.column_role].push({
        name: col.column_name,
        business_name: col.business_name
      });
    }
  }

  for (const [tableName, roles] of Object.entries(tableColsByRole)) {
    const surrogateKeys = roles['surrogate_key'] || [];
    const naturalKeys = roles['natural_key'] || [];
    const descCols = roles['description_col'] || [];
    const fkOnly = roles['fk_only'] || [];

    if (naturalKeys.length > 0 && surrogateKeys.length > 0) {
      const nkList = naturalKeys.map(c => `"${c.name}" (${c.business_name || c.name})`).join(', ');
      const skList = surrogateKeys.map(c => `"${c.name}"`).join(', ');
      displayKeyRules.push(`  ${tableName}: Display ${nkList} to identify records. Use ${skList} for JOINs only.`);
    } else if (naturalKeys.length > 0) {
      const nkList = naturalKeys.map(c => `"${c.name}" (${c.business_name || c.name})`).join(', ');
      displayKeyRules.push(`  ${tableName}: Display ${nkList} to identify records.`);
    }
    // Add description columns as display helpers
    if (descCols.length > 0 && (naturalKeys.length > 0 || surrogateKeys.length > 0)) {
      const dcList = descCols.map(c => `"${c.name}"`).join(', ');
      displayKeyRules.push(`  ${tableName}: Include ${dcList} for human-readable context.`);
    }
  }

  if (displayKeyRules.length > 0) {
    sections.push('ENTITY DISPLAY KEYS (critical for user-facing results):');
    sections.push('When the user asks to "show" or "list" or "display" an entity, use the DISPLAY-KEY column (natural key), NOT the surrogate ID.');
    sections.push('Surrogate keys (*_ID) should appear ONLY in JOIN/WHERE conditions, never in SELECT for display.');
    for (const rule of displayKeyRules) {
      sections.push(rule);
    }
    sections.push('');
  }

  // Add COMPUTED MEASURES section — derived business metrics that aren't stored as columns
  // This is critical: users ask "how much" or "dollar amount" but the answer requires
  // multiplying price × quantity across a JOIN. Without this, the LLM falls back to COUNT(*).
  const computedMeasureLines = [];
  for (const table of tablesResult.rows) {
    const meta = table.entity_metadata || {};
    if (meta.computed_measures && meta.computed_measures.length > 0) {
      for (const cm of meta.computed_measures) {
        let line = `  ${table.table_name}: "${cm.name}" = ${cm.formula}`;
        if (cm.join_table) {
          line += ` (requires JOIN to "${cm.join_table}" ON ${cm.join_on})`;
        }
        if (cm.description) {
          line += ` — ${cm.description}`;
        }
        computedMeasureLines.push(line);
      }
    }
  }
  if (computedMeasureLines.length > 0) {
    sections.push('COMPUTED MEASURES (critical — use these instead of COUNT(*) when users ask for amounts/values/totals):');
    sections.push('These business metrics are NOT stored as columns — they must be calculated using the formulas below.');
    sections.push('When a user asks for "dollar amount", "total value", "sales amount", etc., use the appropriate computed measure.');
    sections.push('NEVER fall back to COUNT(*) when a computed measure exists for the requested metric.');
    for (const line of computedMeasureLines) {
      sections.push(line);
    }
    sections.push('');
  }

  // Add BFS-computed join paths if available
  // These give the LLM explicit multi-hop join instructions so it doesn't have to
  // figure out intermediate tables from raw relationships
  if (schemaLink._lastJoinPaths && schemaLink._lastJoinPaths.length > 0) {
    try {
      const { formatJoinPathsContext } = require('../services/join-path-service');
      const joinPathsText = formatJoinPathsContext(schemaLink._lastJoinPaths);
      if (joinPathsText) {
        sections.push(joinPathsText);
      }
    } catch (e) {
      // join path service not available — skip
    }
  }

  let context = sections.join('\n');

  // ── Prompt size guard ──
  // Claude has a 200K token context window. The BOKG context is the largest part
  // of the NL2SQL prompt. Estimate tokens (~4 chars/token) and truncate progressively
  // to stay safely under the limit, leaving room for system prompt + few-shot examples.
  const MAX_CONTEXT_TOKENS = 150000; // leave 50K for system prompt + examples + response
  const estimatedTokens = Math.ceil(context.length / 4);

  if (estimatedTokens > MAX_CONTEXT_TOKENS) {
    console.warn(`[BOKG] Context too large (${estimatedTokens} est. tokens). Truncating...`);

    // Strategy: rebuild with fewer columns per table (drop descriptions, value mappings)
    const truncatedSections = [];
    let currentTokens = 0;
    const TOKEN_BUDGET = MAX_CONTEXT_TOKENS * 4; // back to chars

    for (const section of sections) {
      currentTokens += section.length + 1; // +1 for newline
      if (currentTokens > TOKEN_BUDGET) {
        truncatedSections.push('(... additional schema context truncated to fit token limit ...)');
        break;
      }
      truncatedSections.push(section);
    }

    context = truncatedSections.join('\n');
    console.log(`[BOKG] Truncated context: ${Math.ceil(context.length / 4)} est. tokens`);
  }

  return context;
}

// ─────────────────────────────────────────────────────────────────────────────
// NL-to-SQL SYSTEM PROMPT (shared across initial generation and retries)
// ─────────────────────────────────────────────────────────────────────────────
// App-specific SQL hints — keyed by app ID or app name pattern.
// These get injected into the NL2SQL system prompt so generic rules
// stay shared while app-specific guidance (date ranges, coded values,
// column quirks) scales per application.
const APP_SQL_HINTS = {
  // BIRD benchmark databases (financial, Czech banking data)
  bird: `
- For Czech coded values: VYDAJ=withdrawal/debit, PRIJEM=credit, VYBER=cash withdrawal, PREVOD=remittance/bank transfer
- DATA DATE RANGE: The data spans 1993-1998 but NOT every table has data for every year. Different tables have different max years (e.g., accounts go through 1997, transactions through 1998). When the user says "last year", "this year", or "recent", do NOT hardcode a specific year. Instead, use a subquery to find the max year in the PRIMARY table being queried: e.g., WHERE EXTRACT(YEAR FROM "date"::date) = (SELECT MAX(EXTRACT(YEAR FROM "date"::date)) FROM "account"). This ensures correct results regardless of which table's date range applies. NEVER use the current calendar year (2025/2026) — the data is historical.`,

  // Oracle EBS (OEBS) — enterprise data with current dates
  oebs: `
- DATA DATE RANGE: This is live enterprise data. Date columns contain real dates (2024-2026). Use CURRENT_DATE for relative time queries ("this quarter", "last 12 months", "year to date").
- Some records have future dates (forward-dated bookings, scheduled deliveries) and some have NULL dates. ALWAYS exclude both when querying historical periods.
- BOOKED_DATE, ORDERED_DATE, CREATION_DATE are common date columns on order/transaction tables.`,

  // Default — generic guidance for unknown apps
  default: `
- DATA DATE RANGE: Check the actual data to determine the date range. For relative time queries, use CURRENT_DATE unless the data appears to be historical.`
};

function getAppSqlHints(appId, appName) {
  const nameLower = (appName || '').toLowerCase();
  if (nameLower.includes('oebs') || nameLower.includes('oracle') || nameLower.includes('ebs')) return APP_SQL_HINTS.oebs;
  if (nameLower.includes('bird') || nameLower.includes('financial') || nameLower.includes('czech')) return APP_SQL_HINTS.bird;
  // Check app ID — BIRD apps are typically IDs 1-3, OEBS is 4+
  // But this is fragile; better to match on name
  return APP_SQL_HINTS.default;
}

const NL2SQL_BASE_PROMPT = `You are an expert SQL query generator for PostgreSQL databases.
You translate natural language questions into precise, executable SQL queries.

IMPORTANT RULES:
- Write PostgreSQL-compatible SQL only
- Use the Knowledge Graph context to understand cryptic column names and table relationships
- Use the RELATIONSHIPS (JOIN PATHS) section. Use the shortest join path — do NOT add unnecessary intermediate tables.
- Return ONLY the SQL query, no explanation, no markdown, no code blocks
- ALWAYS double-quote ALL table and column names in the SQL. Every identifier must be in double quotes: SELECT "account_id", "date" FROM "trans" WHERE "type" = 'VYDAJ'. This is mandatory because PostgreSQL is case-sensitive and some names are reserved words.
- Use the exact column names from the schema — do not invent columns. The schema shows the real column names (e.g., "account_id" not "accountId", "birth_date" not "birthDate").
- NEVER INVENT COLUMNS: If a column name suggested by the user's question does not appear in the schema, DO NOT assume it exists. Instead, search the column descriptions and business names for matching concepts. For example, if the user says "standard cost" but no STANDARD_COST column exists, look for columns whose description mentions "standard cost", "item cost", or "unit cost" — the actual column might be LIST_PRICE_PER_UNIT or similar. Using a non-existent column will cause an error.
- COLUMN ALIAS AWARENESS: Column descriptions often include "Also known as:" aliases. When the user's terminology doesn't match any column name exactly, scan the descriptions for the user's terms. The correct column is often named differently from what users expect.
- When the schema includes value dictionaries (coded values), use the exact codes in WHERE clauses
- Temperature is 0 — be deterministic and precise
- DATE HANDLING: Date columns are stored as TEXT. Always cast to date before date operations: "date"::date, EXTRACT(YEAR FROM "date"::date), "date"::date >= '1995-01-01'. Never use EXTRACT or date functions on raw text columns.
- TIME-BOUNDED QUERIES (critical): When the user asks for "last N months/quarters/years", ALWAYS use BOTH a lower AND upper bound. "Last N months" means the N months ending with the CURRENT month (inclusive), so use INTERVAL 'N-1 months' back from the start of the current month:
  * "last 12 months" → WHERE date::date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '11 months' AND date::date < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
    (This gives exactly 12 months: current month + 11 prior months)
  * "last 4 quarters" → WHERE date::date >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '9 months' AND date::date < DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months'
  * "this quarter" → WHERE date::date >= DATE_TRUNC('quarter', CURRENT_DATE) AND date::date < DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3 months'
  * "this year" → WHERE date::date >= DATE_TRUNC('year', CURRENT_DATE) AND date::date < DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1 year'
  Without the upper bound, future-dated records are included, giving wrong results. ALSO exclude NULL dates: AND date IS NOT NULL.
- DATE GROUPING (critical): When grouping "by month", "by quarter", or "by year", ALWAYS use DATE_TRUNC in both SELECT and GROUP BY:
  * "by month" → SELECT DATE_TRUNC('month', "date"::date) AS "month", ... GROUP BY DATE_TRUNC('month', "date"::date) ORDER BY "month"
  * "by quarter" → SELECT DATE_TRUNC('quarter', "date"::date) AS "quarter", ... GROUP BY DATE_TRUNC('quarter', "date"::date) ORDER BY "quarter"
  * "by year" → SELECT DATE_TRUNC('year', "date"::date) AS "year", ... GROUP BY DATE_TRUNC('year', "date"::date) ORDER BY "year"
  NEVER use TO_CHAR, EXTRACT, or string formatting for date grouping. DATE_TRUNC returns proper timestamps that the UI can format correctly. TO_CHAR or EXTRACT produce strings/numbers that lose date semantics.

COLUMN DISCIPLINE (critical):
- Your SELECT clause must contain ONLY the columns the question explicitly asks for. No extras.
- "List all withdrawals" → SELECT "trans_id" only (not amount, date, balance, etc.)
- "List the district" → SELECT "A2" only (not "district_id" or other columns). The district name column is "A2", NOT "A1" (there is no A1).
- "Calculate the percentage" → SELECT the percentage only (not the district name alongside it)
- If the question asks for one thing, return one column. If two things, two columns. Never add "helpful" extras.
- Only add columns that are explicitly requested or strictly necessary for the answer.
- ONLY use column names that appear in the schema. NEVER invent columns (no "A1", no "name", no "region" unless they are listed).

DISPLAY NAMES (critical for readability):
- ALWAYS prefer DISPLAY-KEY columns over SURROGATE-KEY columns in SELECT. If the schema marks a column as [DISPLAY-KEY], use it when users ask to "show", "list", or identify entities.
- SURROGATE-KEY columns (usually ending in _ID) should appear ONLY in JOIN and WHERE conditions, NOT in SELECT for display — unless no DISPLAY-KEY exists.
- When listing or grouping by an entity, include the human-readable name/label column (DISPLAY-KEY or DISPLAY-TEXT), not the internal ID.
- Check the ENTITY DISPLAY KEYS section in the Knowledge Graph for table-specific guidance on which columns to display vs. use for joins.
- If a question says "list items" or "by customer" or "per organization", use the business identifier column, not the _ID column.

COMPUTED MEASURES (critical for dollar amounts, values, and totals):
- When the user asks for "dollar amount", "value", "sales amount", "how much", "total cost", or any monetary metric,
  check the COMPUTED MEASURES section in the Knowledge Graph FIRST.
- If a computed measure exists, use the provided formula (e.g., UNIT_SELLING_PRICE * ORDERED_QUANTITY) instead of
  looking for a single amount column. If no single column stores the answer, the formula tells you how to calculate it.
- NEVER fall back to COUNT(*) when the user asks for dollar amounts — use the computed measure formula with SUM().
- If a computed measure specifies a join_table, you MUST JOIN to that table to compute the metric.
  Example: "sales order dollar amounts" requires joining headers to lines and computing SUM(price * qty) on the lines.

AGGREGATION PATTERNS:
- "Top N by district/region/category" → SELECT DISTINCT the category name, ORDER BY name ASC, LIMIT N. Do NOT return individual rows with amounts.
- For "oldest/youngest/lowest/highest" questions, use ORDER BY ... LIMIT 1 instead of subqueries with MIN/MAX
- "List accounts of [person]" → return ALL matching accounts, do NOT add LIMIT 1 unless the question says "one" or "single"
- When computing averages for comparison ("amount less than the average"), the average should be computed over ALL rows in the table for that time period, NOT just the filtered subset
- Return at most 100 rows unless the question implies aggregation or asks for "all"

COMPUTATION:
- For age calculations use: EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM "birth_date"::date). Do NOT use julianday.
- Use CAST(... AS NUMERIC) before division to avoid integer division
- When computing AVG with a JOIN, use the JOIN approach (not a subquery with IN) so that the average reflects the actual row distribution
- NEVER nest aggregate functions: SUM(AVG(...)), AVG(SUM(...)), etc. are illegal in PostgreSQL. If you need to aggregate an aggregate, use a CTE or subquery: WITH avg_costs AS (SELECT item_id, AVG(cost) as avg_cost FROM ... GROUP BY item_id) SELECT SUM(avg_cost) FROM avg_costs
- For date comparisons: ALWAYS cast text to date first — "date"::date >= '1995-01-01'. Never compare text strings as dates.
- For year extraction: EXTRACT(YEAR FROM "date"::date) — never EXTRACT from raw text

JOIN GUIDANCE:
- Always check the RELATIONSHIPS section for the correct join columns
- Use the shortest path between tables — do not add intermediate tables unless necessary
- For many-to-one relationships (FK → PK), the FK table has many rows per PK row

PERFORMANCE:
- The "trans" table has over 1 million rows. When joining with trans, always add WHERE filters to limit the scan.
- For questions about balances, use the LATEST transaction per account (subquery with MAX or DISTINCT ON) rather than scanning all transactions.
- Use LIMIT 100 on result sets unless the user asks for "all" or the query is an aggregation.
- Prefer aggregation (COUNT, SUM, AVG) over returning raw rows when the question asks "how many" or "what is the total".`;

// ── SQLite dialect prompt ──────────────────────────────────────────────────────
// Used when the target database is SQLite (e.g. BIRD benchmark, local databases).
// Mirrors the PostgreSQL prompt but with SQLite-specific syntax rules.
const NL2SQL_SQLITE_PROMPT = `You are an expert SQL query generator for SQLite databases.
You translate natural language questions into precise, executable SQL queries.

IMPORTANT RULES:
- Write SQLite-compatible SQL only
- Use the Knowledge Graph context to understand cryptic column names and table relationships
- Use the RELATIONSHIPS (JOIN PATHS) section. Use the shortest join path — do NOT add unnecessary intermediate tables.
- Return ONLY the SQL query, no explanation, no markdown, no code blocks
- Use the exact column names from the schema — do not invent columns. The schema shows the real column names.
- NEVER INVENT COLUMNS: If a column name suggested by the user's question does not appear in the schema, DO NOT assume it exists. Instead, search the column descriptions and business names for matching concepts.
- COLUMN ALIAS AWARENESS: Column descriptions often include "Also known as:" aliases. When the user's terminology doesn't match any column name exactly, scan the descriptions for the user's terms.
- When the schema includes value dictionaries (coded values), use the exact codes and exact case from the dictionary in WHERE clauses.
- CASE SENSITIVITY: SQLite string comparisons are case-sensitive by default. When comparing text columns where the case of the stored value is uncertain, add COLLATE NOCASE to the comparison: WHERE column = 'value' COLLATE NOCASE. This is especially important for status fields, category fields, names, and any text column where user input may differ in case from stored values.
- VALUE FORMAT MATCHING: When filtering on text columns, use the exact format shown in value dictionaries or column descriptions. For year ranges, check if data uses '2014-2015', '2014/2015', or '2014-15'. For seasons, check if data uses '2009/2010' format. Never assume the format — use what the schema/dictionary shows.
- Temperature is 0 — be deterministic and precise

SQLite-SPECIFIC SYNTAX (critical):
- Do NOT use double quotes for identifiers unless they contain spaces or are reserved words. Prefer unquoted or backtick-quoted identifiers.
- Do NOT use :: type casts. Use CAST(x AS type) instead.
- Do NOT use EXTRACT(). Use strftime() instead:
  * Year: strftime('%Y', date_column)
  * Month: strftime('%m', date_column)
  * Day: strftime('%d', date_column)
- Do NOT use DATE_TRUNC(). Use strftime() for date grouping:
  * By month: strftime('%Y-%m', date_column)
  * By year: strftime('%Y', date_column)
  * By quarter: (strftime('%Y', date_column) || '-Q' || ((CAST(strftime('%m', date_column) AS INTEGER) - 1) / 3 + 1))
- Do NOT use INTERVAL. Use date() with modifiers:
  * date('now', '-12 months'), date('now', 'start of year'), date(column, '+1 month')
- Do NOT use CURRENT_DATE as a function. Use date('now').
- Do NOT use NOW(). Use datetime('now').
- Do NOT use LEFT(str, n). Use SUBSTR(str, 1, n).
- Do NOT use ILIKE. Use LIKE (SQLite LIKE is case-insensitive for ASCII).
- Do NOT use TRUE/FALSE. Use 1/0.
- Do NOT use BOOLEAN logic like (column = TRUE). Use (column = 1).
- For conditional counting use IIF(condition, 1, 0) or CASE WHEN ... THEN 1 ELSE 0 END.
- For division, use CAST(numerator AS REAL) to avoid integer division.
- For CONCAT, use || operator: first_name || ' ' || last_name.
- LIMIT and OFFSET work the same as PostgreSQL.
- SQLite supports WITH (CTE), UNION, EXCEPT, INTERSECT, window functions.
- For string matching: Use LIKE with % wildcards. SQLite LIKE is case-insensitive for ASCII letters.
- For BETWEEN on dates: dates stored as TEXT in YYYY-MM-DD format can be compared directly as strings.

COLUMN DISCIPLINE (critical):
- Your SELECT clause must contain ONLY the columns the question explicitly asks for. No extras.
- If the question asks for one thing, return one column. If two things, two columns. Never add "helpful" extras.
- Only add columns that are explicitly requested or strictly necessary for the answer.
- ONLY use column names that appear in the schema. NEVER invent columns.

DISPLAY NAMES (critical for readability):
- ALWAYS prefer DISPLAY-KEY columns over SURROGATE-KEY columns in SELECT.
- SURROGATE-KEY columns (usually ending in _ID) should appear ONLY in JOIN and WHERE conditions, NOT in SELECT for display.
- Check the ENTITY DISPLAY KEYS section in the Knowledge Graph for table-specific guidance.

COMPUTED MEASURES (critical for dollar amounts, values, and totals):
- When the user asks for "dollar amount", "value", "sales amount", "how much", "total cost", or any monetary metric,
  check the COMPUTED MEASURES section in the Knowledge Graph FIRST.
- If a computed measure exists, use the provided formula instead of looking for a single amount column.
- NEVER fall back to COUNT(*) when the user asks for dollar amounts — use the computed measure formula with SUM().

AGGREGATION PATTERNS:
- For "oldest/youngest/lowest/highest" questions, use ORDER BY ... LIMIT 1 instead of subqueries with MIN/MAX
- When computing averages for comparison, the average should be computed over ALL rows, NOT just the filtered subset
- Return at most 100 rows unless the question implies aggregation or asks for "all"
- When counting "how many X have Y in N instances", carefully distinguish between: counting the number of items vs counting the number of instances. Read the question twice.
- For "abnormal" ranges in medical/scientific data, use INCLUSIVE boundaries: <= and >= (not < and >), unless the question explicitly says "below" or "above" which implies exclusive.

COMPUTATION:
- For age calculations: CAST(strftime('%Y', 'now') AS INTEGER) - CAST(strftime('%Y', birth_date) AS INTEGER)
- Use CAST(... AS REAL) before division to avoid integer division
- NEVER nest aggregate functions: SUM(AVG(...)) is illegal. Use a CTE instead.
- For date comparisons: dates as TEXT in YYYY-MM-DD format compare correctly as strings.

JOIN GUIDANCE:
- Always check the RELATIONSHIPS section for the correct join columns
- Use the shortest path between tables — do not add intermediate tables unless necessary`;

// Build the full NL2SQL prompt by combining base rules + app-specific hints
function buildNL2SQLPrompt(appName, dialect = 'postgresql') {
  const hints = getAppSqlHints(null, appName);
  const basePrompt = dialect === 'sqlite' ? NL2SQL_SQLITE_PROMPT : NL2SQL_BASE_PROMPT;
  return basePrompt + '\n\nAPP-SPECIFIC RULES:' + hints;
}

// Legacy alias for backward compatibility
const NL2SQL_SYSTEM_PROMPT = NL2SQL_BASE_PROMPT + '\n\nAPP-SPECIFIC RULES:' + APP_SQL_HINTS.default;

/**
 * Call Claude API for NL-to-SQL generation
 * Supports multi-turn messages for retry with error feedback
 */
async function callNL2SQL(systemPrompt, messages, model = null, temperature = 0) {
  const API_KEY = process.env.ANTHROPIC_API_KEY;
  if (!API_KEY) throw new Error('ANTHROPIC_API_KEY not set');

  const useModel = model || DEFAULT_LLM_MODEL;

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: useModel,
      max_tokens: 2048,
      temperature,
      system: systemPrompt,
      messages,
    }),
  });

  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`Claude API error ${response.status}: ${errText}`);
  }

  const data = await response.json();
  const rawText = data.content[0].text.trim();
  const sql = extractSQL(rawText);
  const usage = data.usage || {};

  return {
    sql,
    rawText,
    token_usage: {
      input_tokens: usage.input_tokens || 0,
      output_tokens: usage.output_tokens || 0,
      total_tokens: (usage.input_tokens || 0) + (usage.output_tokens || 0),
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. EXECUTION-BASED RETRY
// When SQL fails or returns 0 rows, feeds the error back to Claude and retries.
// Uses multi-turn conversation so Claude sees its own SQL + the error.
// ─────────────────────────────────────────────────────────────────────────────
async function generateAndExecuteWithRetry(appId, question, bokgContext, appName, fewShotSection, valueDictionaries, contextSection = '', model = null, options = {}) {
  const dialect = options.dialect || 'postgresql';
  const dialectLabel = dialect === 'sqlite' ? 'SQLite' : 'PostgreSQL';
  const userPrompt = `${bokgContext}
${contextSection}${fewShotSection}APPLICATION: ${appName}

QUESTION: ${question}

Generate a ${dialectLabel} SELECT query to answer this question.

Your response MUST follow this exact format:

### Approach
[1-3 sentences explaining your strategy: which tables you'll use, what joins, aggregations, and filters you'll apply, and why]

### SQL
[The SQL query — just the raw SQL, no code blocks]

### Assumptions
[Bullet list of assumptions you're making about the question]

### Why this SQL
[2-4 sentences explaining why you chose this specific query structure, joins, and columns]`;

  // Conversation history for multi-turn retry
  const messages = [{ role: 'user', content: userPrompt }];

  // Aggregate token usage across retries
  const totalTokens = { input_tokens: 0, output_tokens: 0, total_tokens: 0 };
  let finalSql = '';
  let finalRows = [];
  let finalColumns = [];
  let finalExecError = null;
  let finalExecTime = null;
  let retryCount = 0;
  let rawExplanation = '';
  const retryLog = []; // Track what went wrong on each attempt

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    // Generate SQL — use app-specific prompt with tailored date handling, coded values, etc.
    const genResult = await callNL2SQL(buildNL2SQLPrompt(appName, dialect), messages, model);
    totalTokens.input_tokens += genResult.token_usage.input_tokens;
    totalTokens.output_tokens += genResult.token_usage.output_tokens;
    totalTokens.total_tokens += genResult.token_usage.total_tokens;

    let sql = genResult.sql;

    // Apply case-fix post-processing
    sql = applyCaseFix(sql, valueDictionaries);
    // SQLite-specific post-processing
    if (dialect === 'sqlite') {
      sql = addCollateNocase(sql);
      sql = fixApostropheCollate(sql);  // Fix COLLATE NOCASE breaking apostrophe strings
    }
    // Dialect-general date fixes
    sql = fixYearFormat(sql);       // '2014-15' → '2014-2015' (skip months)
    sql = fix2DigitYearLike(sql);   // '81-11-%' → '1981-11-%'
    finalSql = sql;

    // Save raw explanation on first successful generation
    if (rawExplanation === '' && genResult.rawText) {
      rawExplanation = genResult.rawText;
    }

    // Add assistant response to conversation history (for potential retry)
    messages.push({ role: 'assistant', content: genResult.rawText });

    // DRY RUN: skip execution — return generated SQL only (for SQLite benchmarking)
    if (options.dryRun) {
      return {
        sql: finalSql,
        rows: [],
        columns: [],
        execError: null,
        execTime: null,
        token_usage: totalTokens,
        retryCount: 0,
        retryLog: [],
        rawExplanation,
        dryRun: true,
      };
    }

    // Execute the SQL
    try {
      const execStart = Date.now();
      const result = await executeOnSourceData(appId, sql);
      finalExecTime = Date.now() - execStart;
      finalRows = result.rows || [];
      finalColumns = result.columns || (finalRows.length > 0 ? Object.keys(finalRows[0]) : []);
      finalExecError = null;

      // Check for empty results — might indicate wrong query
      if (finalRows.length === 0 && attempt < MAX_RETRIES) {
        console.log(`  NL2SQL attempt ${attempt + 1}: SQL executed but returned 0 rows, retrying...`);
        retryLog.push({ attempt: attempt + 1, reason: 'zero_rows', sql: sql.substring(0, 500) });
        messages.push({
          role: 'user',
          content: `The query executed successfully but returned 0 rows. This likely means the query logic is wrong — perhaps a wrong column value, an incorrect JOIN, or an overly restrictive WHERE clause. Please analyze the issue and generate a corrected SQL query. The database definitely has data for this kind of question.

Common fixes:
- Check if string values match the exact case in the value dictionary
- Check if the JOIN path is correct
- Check if the WHERE conditions are too restrictive
- Check if you're using the right date range

Output ONLY the corrected SQL, nothing else.`
        });
        retryCount++;
        continue;
      }

      // Success with rows — stop retrying
      break;

    } catch (execErr) {
      finalExecError = execErr.message;

      if (attempt < MAX_RETRIES) {
        console.log(`  NL2SQL attempt ${attempt + 1}: execution error, retrying... Error: ${execErr.message.substring(0, 200)}`);
        retryLog.push({ attempt: attempt + 1, reason: 'exec_error', error: execErr.message.substring(0, 300), sql: sql.substring(0, 500) });
        const retryGuidance = dialect === 'sqlite'
          ? `The SQL failed with this SQLite error:
${execErr.message}

Please fix the SQL and try again. Common SQLite issues:
- Do NOT use double-quoted identifiers — use backticks or bare names
- Do NOT use EXTRACT() — use strftime('%Y', date_col) etc.
- Do NOT use ::cast syntax — use CAST(col AS type)
- Do NOT use DATE_TRUNC — use strftime for date truncation
- Use IIF(cond, true_val, false_val) instead of CASE for simple conditionals
- Use SUBSTR() not SUBSTRING()
- Check the schema for exact column names — don't invent columns

Output ONLY the corrected SQL, nothing else.`
          : `The SQL failed with this PostgreSQL error:
${execErr.message}

Please fix the SQL and try again. Common issues:
- Column names must be double-quoted: "column_name"
- Table names must be double-quoted: "table_name"
- Text columns need ::date cast before date functions
- Check the schema for exact column names — don't invent columns
- Reserved words (like "order") MUST be double-quoted

Output ONLY the corrected SQL, nothing else.`;
        messages.push({
          role: 'user',
          content: retryGuidance
        });
        retryCount++;
        continue;
      }
      // Final attempt also failed — return the error
      break;
    }
  }

  return {
    sql: finalSql,
    rows: finalRows,
    columns: finalColumns,
    execError: finalExecError,
    execTime: finalExecTime,
    token_usage: totalTokens,
    retryCount,
    retryLog,
    rawExplanation,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. SELF-CONSISTENCY VOTING
// Generates N candidates at temperature > 0, executes each, and picks the
// result that the majority of candidates agree on (by execution output hash).
// Falls back to single-shot (temperature=0) if N=1 or not requested.
// ─────────────────────────────────────────────────────────────────────────────
async function generateWithSelfConsistency(appId, question, bokgContext, appName, fewShotSection, valueDictionaries, contextSection = '', model = null, options = {}) {
  const numCandidates = options.selfConsistency || 1;
  const dialect = options.dialect || 'postgresql';

  // If SC not requested (N=1), or dryRun (can't vote without execution), use standard pipeline
  if (numCandidates <= 1 || options.dryRun) {
    return generateAndExecuteWithRetry(appId, question, bokgContext, appName, fewShotSection, valueDictionaries, contextSection, model, options);
  }

  console.log(`[SC Voting] Generating ${numCandidates} candidates for: "${question.substring(0, 80)}..."`);

  // Build the prompt once (same for all candidates)
  const dialectLabel = dialect === 'sqlite' ? 'SQLite' : 'PostgreSQL';
  const userPrompt = `${bokgContext}
${contextSection}${fewShotSection}APPLICATION: ${appName}

QUESTION: ${question}

Generate a ${dialectLabel} SELECT query to answer this question.

Your response MUST follow this exact format:

### Approach
[1-3 sentences explaining your strategy: which tables you'll use, what joins, aggregations, and filters you'll apply, and why]

### SQL
[The SQL query — just the raw SQL, no code blocks]

### Assumptions
[Bullet list of assumptions you're making about the question]

### Why this SQL
[2-4 sentences explaining why you chose this specific query structure, joins, and columns]`;

  const systemPrompt = buildNL2SQLPrompt(appName, dialect);

  // Generate N candidates in parallel at temperature 0.7
  const candidatePromises = [];
  for (let i = 0; i < numCandidates; i++) {
    candidatePromises.push(
      callNL2SQL(systemPrompt, [{ role: 'user', content: userPrompt }], model, 0.7)
        .then(result => ({ success: true, ...result }))
        .catch(err => ({ success: false, error: err.message }))
    );
  }
  const candidates = await Promise.all(candidatePromises);

  // Aggregate token usage
  const totalTokens = { input_tokens: 0, output_tokens: 0, total_tokens: 0 };
  for (const c of candidates) {
    if (c.success && c.token_usage) {
      totalTokens.input_tokens += c.token_usage.input_tokens;
      totalTokens.output_tokens += c.token_usage.output_tokens;
      totalTokens.total_tokens += c.token_usage.total_tokens;
    }
  }

  // Execute each candidate SQL and collect results
  const execResults = [];
  for (let i = 0; i < candidates.length; i++) {
    const c = candidates[i];
    if (!c.success || !c.sql) {
      execResults.push({ index: i, sql: null, error: c.error || 'no SQL generated', rows: [], hash: null });
      continue;
    }

    let sql = applyCaseFix(c.sql, valueDictionaries);
    if (dialect === 'sqlite') {
      sql = addCollateNocase(sql);
    }

    try {
      const result = await executeOnSourceData(appId, sql);
      const rows = result.rows || [];
      // Create a hash of the result for voting — stringify first 20 rows sorted
      const hashInput = JSON.stringify(rows.slice(0, 20));
      const hash = simpleHash(hashInput);
      execResults.push({ index: i, sql, rows, columns: result.columns || (rows.length > 0 ? Object.keys(rows[0]) : []), hash, error: null, rawText: c.rawText });
    } catch (execErr) {
      execResults.push({ index: i, sql, error: execErr.message, rows: [], hash: null });
    }
  }

  // Vote: group by result hash, pick the largest group
  const hashGroups = {};
  for (const r of execResults) {
    if (r.hash !== null && r.rows.length > 0) {
      if (!hashGroups[r.hash]) hashGroups[r.hash] = [];
      hashGroups[r.hash].push(r);
    }
  }

  let winner = null;
  let maxVotes = 0;
  for (const [hash, group] of Object.entries(hashGroups)) {
    if (group.length > maxVotes) {
      maxVotes = group.length;
      winner = group[0]; // Pick first candidate in the winning group
    }
  }

  // If no candidate produced rows, fall back to the first candidate that at least generated SQL
  if (!winner) {
    console.log(`[SC Voting] No candidate returned rows — falling back to first SQL`);
    winner = execResults.find(r => r.sql) || execResults[0];
  }

  console.log(`[SC Voting] Winner: candidate ${winner.index} with ${maxVotes}/${numCandidates} votes, ${winner.rows.length} rows`);

  return {
    sql: winner.sql || '',
    rows: winner.rows || [],
    columns: winner.columns || [],
    execError: winner.error || null,
    execTime: null,
    token_usage: totalTokens,
    retryCount: 0,
    retryLog: [],
    rawExplanation: winner.rawText || '',
    scVotes: maxVotes,
    scCandidates: numCandidates,
  };
}

/**
 * Simple string hash for comparing execution results.
 */
function simpleHash(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash |= 0; // Convert to 32bit integer
  }
  return hash.toString(36);
}

// ─────────────────────────────────────────────────────────────────────────────
// CONVERSATION ROUTER — Multi-turn voice conversation endpoint
// Takes full conversation history, decides whether to clarify or execute query.
// Powers the "Claudia" voice agent experience in the Data Ask interface.
// ─────────────────────────────────────────────────────────────────────────────
router.post('/:appId/converse', async (req, res) => {
  try {
    const { appId } = req.params;
    const { messages: conversationHistory, userName, assistantName = 'Claudia', voice: clientVoice, requestCount = 0, collect_only = false, phase = 'greeting', resultsOnScreen = [], pendingQueries = [] } = req.body;

    if (!conversationHistory || !Array.isArray(conversationHistory) || conversationHistory.length === 0) {
      return res.status(400).json({ error: 'Conversation history (messages array) is required' });
    }

    // Check source data is available
    const dataAvailable = await hasSourceData(appId);
    if (!dataAvailable) {
      return res.status(400).json({ error: 'Source data not loaded. Run the pipeline first.' });
    }

    const API_KEY = process.env.ANTHROPIC_API_KEY;
    if (!API_KEY) throw new Error('ANTHROPIC_API_KEY not set');

    const startTime = Date.now();

    // Get app context
    const appResult = await query('SELECT name, config FROM applications WHERE id = $1', [appId]);
    const appName = appResult.rows[0]?.name || 'Unknown';
    const appConfig = appResult.rows[0]?.config || {};
    const qeConfig = appConfig.query_engine || {};

    // Build BOKG catalog for context (same as classify endpoint)
    const tablesResult = await query(
      `SELECT t.table_name, t.entity_name, t.entity_metadata, t.description
       FROM app_tables t WHERE t.app_id = $1`, [appId]);

    const bokgCatalog = [];
    for (const t of tablesResult.rows) {
      const entityName = t.entity_name || t.table_name;
      const meta = t.entity_metadata || {};
      const questions = meta.sample_questions || [];
      const desc = t.description || meta.description || '';
      bokgCatalog.push(`  - ${entityName}: ${desc}`);
      for (const q of questions.slice(0, 3)) {
        bokgCatalog.push(`    Q: "${q}"`);
      }
    }

    // Get validated query patterns
    let validatedPatterns = [];
    try {
      const qpdResult = await query(
        `SELECT nl_query FROM test_queries
         WHERE app_id = $1 AND feedback = 'thumbs_up'
         ORDER BY created_at DESC LIMIT 15`, [appId]);
      validatedPatterns = qpdResult.rows.map(r => r.nl_query);
    } catch (e) { /* ignore */ }

    // Check for uploaded documents (for UNSTRUCTURED routing)
    let docCatalog = '';
    let hasDocuments = false;
    try {
      const docsResult = await query(
        `SELECT ds.filename, dc.name as collection_name, ds.file_type
         FROM doc_sources ds
         LEFT JOIN doc_collections dc ON ds.collection_id = dc.id
         WHERE ds.app_id = $1 AND ds.status = 'ready'
         ORDER BY ds.created_at DESC LIMIT 20`, [appId]);
      if (docsResult.rows.length > 0) {
        hasDocuments = true;
        const docList = docsResult.rows.map(d => `  - ${d.filename} (${d.file_type})`).join('\n');
        docCatalog = `\n=== UPLOADED DOCUMENTS (for process/policy/procedure questions) ===\n${docList}`;
      }
    } catch (e) { /* ignore — doc tables may not exist yet */ }

    const displayName = userName || 'there';

    // Conversational agent system prompt
    const conversePrompt = `You are ${assistantName}, a VOICE assistant for Solix Data Ask. You are SPEAKING ALOUD to the user — your responses are played as audio through text-to-speech. You ARE a voice assistant. Never say you can't speak, can't hear, or communicate only through text — you DO speak and the user DOES hear you.

PERSONALITY:
- Warm, professional, concise — like a knowledgeable colleague
- Use the user's name naturally (they are "${displayName}")
- Keep responses SHORT (under 30 words) — you are being read aloud
- Never mention SQL, databases, tables, or technical details
- Speak in plain business language
- Never describe yourself as a text-based or chat-based assistant — you are a VOICE assistant

YOUR ROLE:
You have access to a Business Object Knowledge Graph for the "${appName}" system. You can look up data about the business objects listed below.

WHEN TO CLARIFY vs WHEN TO ACT:

CLARIFY when the user uses BROAD terms that map to MULTIPLE metrics:
- "sales numbers" / "sales data" → could mean bookings, revenue, or order counts — ASK which one
- "financial data" → could mean AP, AR, GL — ASK which area
- "show me some numbers" → too vague — ASK what they need
- "how are we doing" → too vague — ASK what area

ACT IMMEDIATELY when the user names a SPECIFIC metric, concept, or question:
- "bookings for the quarter" → run it (query: "What are the total bookings for this quarter?")
- "revenue this month" → run it (query: "What is the total revenue this quarter?")
- "how many items in the item master" → run it
- "top vendors by spend" → run it
- "AR aging report" → run it
- "overdue invoices" → run it
- "booking trend by month" → run it (query: "Show me the booking trend by month for 2025")
- "how many open orders" → run it
- "what bank accounts do we use" → run it (specific question about a specific object)
- ANY question starting with "what", "how many", "which", "show me" about a SPECIFIC entity → run it immediately

CONVERSATION FLOW:
1. Listen to what the user wants
2. If the request maps to ONE specific metric → type "query" immediately
3. If the request contains MULTIPLE specific metrics → type "multi_query" with an array of queries
4. If the request is broad/ambiguous → type "clarify" with a SHORT conversational question offering 2-3 choices
5. When clarifying, frame it naturally: "Would you like to see the bookings, revenue, or order counts?" — not a formal list
6. When the user responds to a clarification, ACT on their choice immediately — DO NOT re-clarify the same thing. SHORT ANSWERS ARE NORMAL in voice conversations:
   - "both" → user wants ALL the options you offered (or the top two if you listed 3+). Use multi_query with the appropriate query templates. Inherit the time scope from the original request.
   - "the first two" / "first one" / "all of them" / "all three" → map to the options you listed, use multi_query
   - "yeah bookings" / "revenue" / "sure, bookings" → single query for that metric. Inherit the time scope from the original request.
   - ALWAYS carry forward the time scope (e.g., "for the quarter") from the user's ORIGINAL request into the queries — the user should NOT have to repeat it.
7. If the user gives context (e.g., "I have a meeting"), acknowledge it briefly and stay focused
8. Your "spoken" field MUST be SHORT (under 10 words) and MUST NOT contain:
   - The user's question or any paraphrase of it (NEVER echo back what they asked — voice transcripts may be truncated)
   - "Anything else?" or "before I pull that up?" or any variation (the system adds this automatically)
   Good examples: "Sure thing.", "Let me pull that up.", "On it.", "Coming right up."
   BAD examples: "Got it — what bank accounts do we use. Anything else?", "Sure, let me check the bookings for you, anything else before I run that?"
10. IMPORTANT: The conversation is a COLLECTING phase. Each turn, the user adds ONE MORE request. Only return the NEW query from THIS turn, not queries from previous turns. Do NOT re-state or re-include queries that appeared earlier in the conversation.

SPEECH RECOGNITION CONTEXT:
The user is speaking through voice-to-text. Sometimes the beginning of their utterance is lost or garbled. When you see a FRAGMENT like:
- "for this month to date" → the user likely said "Include [something] for this month to date" — ask what metric they want for that time range
- "for the quarter" or "by month" → fragment of a longer request — ask what metric
- "revenue for the quarter" when they already have "Revenue" queued → they may be ADDING a time-scoped variant (e.g., collections or bookings for the quarter)
- A single metric name with no verb → treat as a follow-up addition (e.g., "bookings" means "also include bookings")
Always try to make sense of partial input before saying you don't understand. If it sounds like a metric + time scope, generate a query. If truly unclear, ask ONE short clarifying question.

ANTI-REPETITION RULES:
- NEVER repeat yourself. If you already asked a clarifying question, do NOT ask it again.
- If you don't understand, say so differently each time: vary your wording.
- If the user's response to a clarification is still unclear, offer a DIFFERENT approach (e.g., list specific options, or ask them to rephrase).
- NEVER say "I'm sorry, could you say that again?" more than once per conversation. After that, try a different tactic.
- Check the conversation history: if your last response was a clarification, the user's next message is their ANSWER to that clarification — act on it.

MULTI-QUERY DETECTION:
When the user asks for MULTIPLE things in ONE SINGLE message (e.g., "give me bookings, AR aging, and collections"), use type "multi_query" with ALL the queries from THAT message only. Signs:
- "and" or commas connecting distinct metrics: "bookings, AR aging, and collections"
- "also" / "as well" / "plus": "show me bookings, also AR aging"
- numbered requests: "first bookings, then AR aging"
- "along with" / "together with": "revenue along with order counts"
IMPORTANT: If the user says "also show me revenue" as a FOLLOW-UP message, that is a SINGLE new query — use type "query", NOT multi_query. Multi_query is ONLY for multiple items in the SAME message.
For each query in the array, use a QUERY TEMPLATE if one matches.

QUEUE REPLACEMENT vs QUEUE ADDITION (COLLECTING phase only):
When the user says something that REPLACES the current queue rather than adding to it, set "replace": true in the multi_query response. This tells the system to CLEAR the existing queue and use ONLY the new queries.
REPLACEMENT signals — use "replace": true:
- "change that to X and Y" / "make it X and Y instead" / "swap those for X and Y"
- "no, I wanted X and Y" / "actually X and Y" / "instead give me X and Y"
- "not revenue, order counts" / "replace revenue with order counts"
- ANY correction of what's already queued — the user wants a DIFFERENT set
ADDITION signals — do NOT set "replace" (default behavior):
- "also add X" / "and throw in X" / "plus X"
- "show me X too" / "one more thing, X"
- A brand new question with no reference to the existing queue

CRITICAL — QUERY FIELD RULES:
When you set type "query", the "query" field is sent to a SQL generation engine.

RULE ZERO — PASS THROUGH COMPLETE QUESTIONS:
If the user's message is already a clear data question (starts with "show me", "what is", "how many", "which", "list", etc. and specifies what they want), use their EXACT WORDS as the query field. Do NOT rephrase, summarize, or match to a template. The SQL engine handles natural language perfectly.
Examples of pass-through:
- User: "show me the on-hand quantity in each org for item 145" → query: "show me the on-hand quantity in each org for item 145"
- User: "what bank accounts do we use for making payments" → query: "what bank accounts do we use for making payments"
- User: "list all overdue invoices by customer" → query: "list all overdue invoices by customer"

Only rephrase when:
- The user gave a SHORT answer to a clarification (e.g., "both", "bookings") — compose a full question
- The user's intent is vague and you need to map it to a specific business object

Additional rules:
1. Be a complete, self-contained question (never reference "the above" or conversation context)
2. If composing a query (not passing through), check PROVEN QUERIES below for matching patterns
3. Include time scope if the user mentioned one (e.g., "this quarter", "for 2025", "last month")
4. NEVER drop grouping/breakdown words: "in each org", "by month", "per vendor", "by customer" must be preserved

QUERY TEMPLATES — Use the PROVEN QUERIES and SAMPLE QUESTIONS from the BOKG catalog below.
COPY the exact phrasing when the user's intent matches — these have been validated against the actual data.
If no proven query matches exactly, compose a clear question using the business object names and sample questions from the catalog.
When the user specifies a time scope (e.g., "for the quarter", "last 12 months"), append it to the query template.

RESPONSE FORMAT — You MUST respond with a valid JSON object (no markdown, no code fences):

For SINGLE data query (numbers, metrics, aggregations):
{
  "type": "query",
  "spoken": "Brief acknowledgment (under 30 words)",
  "query": "The natural language question (use a QUERY TEMPLATE when possible)",
  "confidence": "high" | "medium"
}

For MULTIPLE queries (user asked for several things at once):
{
  "type": "multi_query",
  "spoken": "Brief acknowledgment like 'Let me pull all of those for you.'",
  "replace": false,
  "queries": [
    { "query": "First question text", "label": "Short label like 'Bookings'" },
    { "query": "Second question text", "label": "AR Aging" },
    { "query": "Third question text", "label": "Collections", "needs_clarification": true, "clarify_spoken": "For collections, did you mean cash collected or collection rate?" }
  ]
}
NOTE: Set "replace": true when the user is CORRECTING/REPLACING queued items (e.g., "change that to X and Y"). Default is false (add to queue).

For DOCUMENT/KNOWLEDGE questions (processes, policies, procedures, definitions — NOT data queries):
{
  "type": "doc_query",
  "spoken": "Brief acknowledgment like 'Let me look that up in the documentation.'",
  "query": "The question about a process/policy/procedure",
  "label": "Short label like 'AP Approval Workflow'"
}

For user is done collecting queries:
{
  "type": "confirm_done",
  "spoken": "Alright, let me pull those up."
}

For compile results into briefing/report:
{
  "type": "briefing",
  "spoken": "Sure, let me put that together for you."
}

For drill into / modify existing result:
{
  "type": "follow_up",
  "spoken": "Sure, breaking that down by quarter.",
  "ref": 1,
  "query": "Show revenue broken down by quarter",
  "label": "Revenue by Quarter"
}

For fix/correct a previous query:
{
  "type": "correction",
  "spoken": "Got it, revenue not bookings.",
  "ref": 1,
  "query": "What is the total revenue for this quarter?",
  "label": "Revenue"
}

For conversational (non-data) response:
{
  "type": "chitchat",
  "spoken": "I'm doing great! I can look up any business data for you — just ask."
}

For hybrid structured + unstructured request:
{
  "type": "hybrid",
  "spoken": "Let me pull the data and check the docs.",
  "queries": [
    { "query": "Show overdue invoices", "label": "Overdue Invoices", "engine": "structured" },
    { "query": "Explain our collection policy", "label": "Collection Policy", "engine": "unstructured" }
  ]
}

For other types:
{ "type": "clarify", "spoken": "Short question with 2-3 choices" }
{ "type": "greeting", "spoken": "Warm greeting using their name" }
{ "type": "farewell", "spoken": "Brief goodbye" }

RULES FOR NEW RESPONSE TYPES:

type "confirm_done": Use when the user signals they're done adding queries. Phrases like "that's all", "go ahead", "run it", "do it", "I'm good", "no more", "execute", "proceed", "let's go", "that'll do", "nothing else". Also covers "yes" or "no" when in collecting phase with pending queries. IMPORTANT: if user says "thank you that's all" during COLLECTING phase, it means "done collecting" NOT "goodbye".

type "briefing": Use when user asks for results to be compiled into a document/report/briefing/summary, or says things like "put that together", "package this up", "summarize for my meeting", "wrap this up for me", "create a report". Only valid when phase is 'reviewing' and results are on screen.

type "follow_up": Use when user references a displayed result and wants to modify/drill down into it. User might say "break that down by quarter", "show me just the top 5", "filter for overdue only", "drill into that", "more detail on the second one". Set "ref" to the result number (1-indexed from resultsOnScreen). If there's only one result or user says "that", ref = 1.

type "correction": Use when user wants to fix something they previously asked. "I meant revenue not bookings", "actually by month not by quarter", "no, for all customers not just top 10".
  - During REVIEWING phase: Set "ref" to which displayed result to replace. Use the correction JSON format with a single "query" field.
  - During COLLECTING phase: The user is correcting a QUEUED item, not an on-screen result. You MUST return a "multi_query" (NOT "correction") containing the FULL corrected set of all queries the user now wants. Example: user queued [bookings, revenue], then says "No, I wanted bookings and order counts" → return multi_query with [bookings query, order counts query]. This replaces the entire queue with the corrected set.

type "chitchat": Use for conversational exchanges that aren't data requests. "How are you?", "What can you do?", "Tell me about yourself", "What time is it?", "Who made you?". Keep responses brief and warm, then gently redirect to data.

type "hybrid": Use when a single request needs BOTH structured data AND document/knowledge search. Each entry in the queries array gets an "engine" field ("structured" or "unstructured"). Example: "show me overdue invoices and explain our collection policy" → one structured query for invoices + one unstructured query for collection policy.

PHASE-SPECIFIC BEHAVIOR:
- In COLLECTING phase: "that's all" / "go ahead" / "I'm done" → confirm_done (NOT farewell)
- In COLLECTING phase: "thank you" → confirm_done if there are pending queries
- In REVIEWING phase: "that's all" / "I'm done" / "thank you" / "bye" → farewell
- In REVIEWING phase: "also show me X" or new data question → query (starts a new collecting cycle)
- In REVIEWING phase: "put that together" / "create a briefing" → briefing
- In REVIEWING phase: "break that down by X" → follow_up
- In GREETING phase: treat everything as a new query/clarify

EXISTING RULES (keep as-is):
- type "greeting": Use when the user says hello/good morning/hi. Greet them by name and ask what they need.
- type "clarify": Use when the request is broad. Ask ONE short question with 2-3 natural choices. Example: "Sure! Would you like the bookings, revenue, or order counts?"
- type "query": Use when you know ONE specific metric from THIS message. Copy a QUERY TEMPLATE if one matches. Keep "spoken" to under 10 words — the system adds follow-up prompts automatically. Good examples: "Sure thing.", "On it.", "Bookings coming right up." Do NOT say "anything else?" — the system handles that.
- type "multi_query": Use ONLY when the user asks for MULTIPLE distinct metrics IN THE SAME MESSAGE. Each entry gets its own query template. Keep "spoken" brief: "Let me pull all of those." Do NOT re-include queries from earlier turns.
- type "doc_query": Use when the user asks about processes, workflows, procedures, policies, definitions, or how something works — questions best answered from documentation rather than database queries. ${hasDocuments ? 'Documents ARE available — use this type for knowledge questions.' : 'No documents uploaded yet — use type "clarify" to suggest the user ask a data question instead, or explain that documentation search is not yet available for this application.'}
- type "farewell": Use when the user says thanks/bye. Keep it warm and brief.

=== AVAILABLE BUSINESS OBJECTS AND SAMPLE QUESTIONS ===
${bokgCatalog.join('\n')}

${validatedPatterns.length > 0 ? `=== PROVEN QUERIES (HIGHEST PRIORITY — copy these EXACTLY when the user's intent matches) ===
These queries have been validated by users and produce correct results. Use them verbatim whenever possible:
${validatedPatterns.map(p => `  "${p}"`).join('\n')}` : ''}
${docCatalog}

=== CONVERSATION PHASE CONTEXT ===
CONVERSATION PHASE: ${phase}
${phase === 'reviewing' && resultsOnScreen.length > 0 ? `
RESULTS CURRENTLY DISPLAYED:
${resultsOnScreen.map((r, i) => `  ${i+1}. "${r.label}" — ${r.row_count || 0} rows (${r.type === 'document' ? 'UNSTRUCTURED' : 'STRUCTURED'})`).join('\n')}
The user may reference these results by number ("the second one") or by description ("the aging report").` : ''}
${phase === 'collecting' && pendingQueries.length > 0 ? `
QUERIES ALREADY QUEUED:
${pendingQueries.map(q => `  - "${typeof q === 'string' ? q : q.label || q.query}"`).join('\n')}
"That's all" or "go ahead" means done collecting → use type "confirm_done"` : ''}`;

    // Server-side STT acronym correction — catches variants the browser STT may produce
    // that the client-side fix might miss (e.g. "our aging" instead of "are aging")
    const fixBusinessAcronyms = (text) => {
      return text
        .replace(/\b(?:are|our|r)\s+(aging)/gi, 'AR $1')
        .replace(/\b(?:are|our|r)\s+(report)/gi, 'AR $1')
        .replace(/\b(?:are|our|r)\s+(balance)/gi, 'AR $1')
        .replace(/\b(?:are|our|r)\s+(receivable)/gi, 'AR $1')
        .replace(/\baccounts\s+receivable/gi, 'AR')
        .replace(/\b(?:aye|a)\s+(pee|p)\b/gi, 'AP')
        .replace(/\baccounts\s+payable/gi, 'AP')
        .replace(/\b(?:gee|g)\s+(ell|l)\b/gi, 'GL')
        .replace(/\bgeneral\s+ledger/gi, 'GL')
        .replace(/\b(?:pee|p)\s+(oh|o)\b/gi, 'PO')
        .replace(/\bpurchase\s+order/gi, 'PO')
        .replace(/\bd\s*s\s*o\b/gi, 'DSO');
    };

    // Convert conversation history to Claude messages format with acronym correction on user messages
    const claudeMessages = conversationHistory.map(m => ({
      role: m.role === 'claudia' ? 'assistant' : 'user',
      content: m.role === 'user' ? fixBusinessAcronyms(m.text) : m.text
    }));

    // Add assistant prefill to force JSON output (prevents Haiku from returning plain text)
    claudeMessages.push({ role: 'assistant', content: '{' });

    // Call Claude for conversation response
    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model: 'claude-haiku-4-5-20251001',  // Haiku for fast conversational routing (~5x faster than Sonnet)
        max_tokens: 512,
        temperature: 0.3,
        system: conversePrompt,
        messages: claudeMessages,
      }),
    });

    if (!response.ok) {
      const errText = await response.text();
      throw new Error(`Claude API error ${response.status}: ${errText}`);
    }

    const data = await response.json();
    // Prepend the '{' we used as assistant prefill
    const rawText = '{' + data.content[0].text.trim();
    const usage = data.usage || {};

    // Parse response — Haiku sometimes returns plain text instead of JSON
    let parsed;
    try {
      parsed = JSON.parse(rawText);
    } catch (e) {
      const jsonMatch = rawText.match(/\{[\s\S]*\}/);
      if (jsonMatch) {
        try { parsed = JSON.parse(jsonMatch[0]); } catch { parsed = null; }
      }
      if (!parsed) {
        console.warn('[Converse] JSON parse failed. Raw LLM output:', rawText.substring(0, 200));
        const lastUserMsg = (conversationHistory[conversationHistory.length - 1]?.text || '').toLowerCase().replace(/[.,!?]/g, '');
        const farewellRx = /\b(thank|thanks|bye|goodbye|that's all|that's it|i'm good|i'm done|no more|nothing else|stop|exit|end)\b/i;

        if (farewellRx.test(lastUserMsg)) {
          parsed = { type: 'farewell', spoken: "You're welcome! Have a great day." };
        } else if (rawText.length > 5 && !rawText.startsWith('{')) {
          // LLM returned conversational text instead of JSON — use it as a spoken clarify response
          // Strip any markdown or quotes, keep it short
          const cleanText = rawText.replace(/[*_`#]/g, '').replace(/\n/g, ' ').trim();
          const spoken = cleanText.length > 150 ? cleanText.substring(0, 147) + '...' : cleanText;
          console.log('[Converse] Using raw LLM text as clarify:', spoken.substring(0, 80));
          parsed = { type: 'clarify', spoken };
        } else {
          parsed = { type: 'clarify', spoken: "I didn't quite catch that. Could you try asking in a different way?" };
        }
      }
    }

    // Log token usage
    try {
      const costEstimate = ((usage.input_tokens || 0) * COST_PER_INPUT_TOKEN) + ((usage.output_tokens || 0) * COST_PER_OUTPUT_TOKEN);
      await query(
        `INSERT INTO token_usage (app_id, stage, table_name, input_tokens, output_tokens, total_tokens, model, cost_estimate)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [appId, 'converse', null, usage.input_tokens || 0, usage.output_tokens || 0,
         (usage.input_tokens || 0) + (usage.output_tokens || 0), 'claude-haiku-4-5-20251001', costEstimate]
      );
    } catch (tokenErr) {
      console.warn('Failed to log converse token usage:', tokenErr.message);
    }

    console.log('[Converse] type:', parsed.type, 'spoken:', parsed.spoken?.substring(0, 60),
      parsed.query ? `query: "${parsed.query.substring(0, 60)}"` : '', collect_only ? '(COLLECT ONLY)' : '');

    // ── COLLECT-ONLY MODE: classify but don't execute ──
    // Used by conversation mode to gather all requests before batch-executing

    // Non-query types in collect-only mode: pass through to client with TTS
    if (collect_only && ['confirm_done', 'briefing', 'follow_up', 'correction', 'chitchat', 'hybrid', 'farewell', 'greeting', 'clarify'].includes(parsed.type)) {
      const spokenText = parsed.spoken || '';
      const ttsBase64 = spokenText ? await generateTTSBase64(spokenText, clientVoice || 'nova') : null;
      return res.json({
        type: parsed.type,
        spoken: spokenText,
        audio_base64: ttsBase64 || undefined,
        // Pass through fields that specific types need
        ...(parsed.query && { query: parsed.query }),
        ...(parsed.label && { label: parsed.label }),
        ...(parsed.ref && { ref: parsed.ref }),
        ...(parsed.queries && { queries: parsed.queries }),
        ...(parsed.engine && { engine: parsed.engine }),
        token_usage: {
          input_tokens: usage.input_tokens || 0,
          output_tokens: usage.output_tokens || 0,
          total_tokens: (usage.input_tokens || 0) + (usage.output_tokens || 0),
        },
      });
    }

    if (collect_only && (parsed.type === 'query' || parsed.type === 'multi_query' || parsed.type === 'doc_query')) {
      // Helper: extract a short friendly label from a full NL query
      const friendlyLabel = (queryText) => {
        // Try to find a keyword match from known patterns
        const labelMap = [
          [/booking/i, 'Bookings'], [/revenue/i, 'Revenue'], [/collection/i, 'Collections'],
          [/AR\s*aging|accounts?\s*receivable\s*aging/i, 'AR Aging'], [/overdue\s*invoice/i, 'Overdue Invoices'],
          [/outstanding\s*receivable/i, 'Outstanding Receivables'], [/vendor|spend/i, 'Vendor Spend'],
          [/open\s*order|unshipped/i, 'Open Orders'], [/on.?hand\s*quantit/i, 'Onhand Quantity'], [/inventory/i, 'Inventory'],
          [/item\s*(master|count)/i, 'Item Master'], [/DSO|days\s*sales/i, 'DSO'],
          [/AP|accounts?\s*payable/i, 'AP'], [/GL|general\s*ledger/i, 'GL'],
          [/bank\s*account/i, 'Bank Accounts'], [/supplier|purchase\s*order/i, 'Purchase Orders'],
          [/customer|ship.?to/i, 'Customers'], [/payment/i, 'Payments'],
        ];
        for (const [rx, label] of labelMap) {
          if (rx.test(queryText)) return label;
        }
        // Fallback: strip question prefixes and truncate at word boundary
        let fallback = queryText.replace(/^(what|show|how|give|tell|can you)\s+(is|are|me|the|us)\s*/i, '')
          .replace(/\?$/, '').trim();
        if (fallback.length > 40) {
          fallback = fallback.substring(0, 40).replace(/\s+\S*$/, '') + '…';
        }
        return fallback || 'your query';
      };

      const queries = [];
      if (parsed.type === 'doc_query') {
        queries.push({ query: parsed.query, label: parsed.label || friendlyLabel(parsed.query), intent: 'UNSTRUCTURED' });
      } else if (parsed.type === 'query') {
        queries.push({ query: parsed.query, label: friendlyLabel(parsed.query) });
      } else if (parsed.queries) {
        for (const q of parsed.queries) {
          if (!q.needs_clarification && q.query) {
            queries.push({ query: q.query, label: q.label || friendlyLabel(q.query) });
          }
        }
      }

      // Use the LLM's spoken field if available (short, natural ack like "Sure thing.")
      // Fall back to label-based template only if LLM didn't provide one
      // Strip "anything else?" variants — the client handles that in collecting mode
      const labels = queries.map(q => q.label).join(' and ');
      let llmSpoken = (parsed.spoken || '').trim()
        .replace(/\.?\s*anything else[^.?]*\??/i, '')
        .replace(/\.?\s*before I pull[^.?]*\??/i, '')
        .replace(/\.?\s*want me to[^.?]*\??/i, '')
        .trim();
      const ackText = llmSpoken && llmSpoken.length > 3 && llmSpoken.length < 60 ? llmSpoken : `Got it — ${labels}.`;
      const ackTTS = await generateTTSBase64(ackText, clientVoice || 'nova');

      // Include any items needing clarification
      const needsClarification = parsed.type === 'multi_query' && parsed.queries
        ? parsed.queries.filter(q => q.needs_clarification) : [];

      return res.json({
        type: 'queued',
        spoken: ackText,
        audio_base64: ackTTS || undefined,
        queries,
        replace: parsed.replace === true,  // true = clear queue and set, false = add to queue
        needs_clarification: needsClarification.length > 0 ? needsClarification : undefined,
        token_usage: {
          input_tokens: usage.input_tokens || 0,
          output_tokens: usage.output_tokens || 0,
          total_tokens: (usage.input_tokens || 0) + (usage.output_tokens || 0),
        },
      });
    }

    // If type is "doc_query", route to unstructured RAG engine
    if (parsed.type === 'doc_query' && parsed.query) {
      try {
        const { answerFromDocuments } = require('../services/unstructured-engine');
        const docResult = await answerFromDocuments(parsed.query, { appId, conversationHistory: [] });
        const answerText = docResult.answer || 'I could not find relevant information in the uploaded documents.';

        // Generate TTS for the answer (truncate long answers for speech)
        const spokenAnswer = answerText.length > 300
          ? answerText.substring(0, 297).replace(/\s+\S*$/, '') + '…'
          : answerText;
        const ttsBase64 = await generateTTSBase64(spokenAnswer, clientVoice || 'nova');

        return res.json({
          type: 'doc_answer',
          spoken: spokenAnswer,
          audio_base64: ttsBase64 || undefined,
          answer: answerText,
          citations: docResult.citations || [],
          label: parsed.label || 'Document Search',
          intent: 'UNSTRUCTURED',
          confidence: docResult.confidence || 'medium',
          generationTime: `${Date.now() - startTime}ms`,
          token_usage: {
            input_tokens: usage.input_tokens || 0,
            output_tokens: usage.output_tokens || 0,
            total_tokens: (usage.input_tokens || 0) + (usage.output_tokens || 0),
          },
        });
      } catch (docErr) {
        console.error('[Converse] Doc query error:', docErr.message);
        const errSpoken = "I couldn't search the documents right now. Could you try a data question instead?";
        const errTTS = await generateTTSBase64(errSpoken, clientVoice || 'nova');
        return res.json({
          type: 'doc_answer',
          spoken: errSpoken,
          audio_base64: errTTS || undefined,
          answer: `Document search error: ${docErr.message}`,
          citations: [],
          intent: 'UNSTRUCTURED',
          confidence: 'low',
          error: docErr.message,
        });
      }
    }

    // If type is "query", check if we need disambiguation or can go straight to NL2SQL
    if (parsed.type === 'query' && parsed.query) {
      try {
        // Speed optimization: if Claudia's query matches a proven template exactly,
        // skip the ENTIRE NL2SQL pipeline — just re-execute the cached SQL
        let queryToRun = parsed.query;
        let result = null;

        const templateCheck = await query(
          `SELECT nl_query, generated_sql FROM test_queries
           WHERE app_id = $1 AND feedback = 'thumbs_up'
           AND LOWER(nl_query) = LOWER($2) LIMIT 1`, [appId, parsed.query]
        );

        if (templateCheck.rows.length > 0 && templateCheck.rows[0].generated_sql) {
          // FAST PATH: proven query with cached SQL — skip classify + NL2SQL entirely
          queryToRun = templateCheck.rows[0].nl_query;
          const cachedSQL = templateCheck.rows[0].generated_sql;
          console.log('[Converse] FAST PATH — cached SQL for:', queryToRun.substring(0, 60));

          const execStart = Date.now();
          try {
            const execResult = await query(cachedSQL);
            result = {
              sql: cachedSQL,
              rows: execResult.rows || [],
              columns: execResult.rows.length > 0 ? Object.keys(execResult.rows[0]) : [],
              execTime: Date.now() - execStart,
              execError: null,
              retryCount: 0,
              token_usage: { input_tokens: 0, output_tokens: 0 },
              rawExplanation: 'Cached proven query — no LLM call needed',
            };
          } catch (execErr) {
            console.warn('[Converse] Cached SQL exec failed, falling back to full pipeline:', execErr.message);
            result = null; // fall through to full pipeline
          }
        }

        if (!result) {
          // FULL PATH: need classify + NL2SQL pipeline
          let skipClassify = false;

          // Check for exact template match (for skipping classify only, SQL wasn't cached or failed)
          if (templateCheck.rows.length > 0) {
            skipClassify = true;
            queryToRun = templateCheck.rows[0].nl_query;
          }

          if (!skipClassify) {
            const { classification: classResult } = await classifyIntent(appId, parsed.query);
            console.log('[Converse→Classify] query:', parsed.query.substring(0, 60),
              '→ confidence:', classResult.confidence, 'disambig:', classResult.disambiguation_needed);

            if (classResult.disambiguation_needed && classResult.slot_questions && classResult.slot_questions.length > 0) {
              const sq = classResult.slot_questions[0];
              const options = sq.options || [];
              let spoken;
              if (options.length >= 2 && options.length <= 4) {
                const lastOption = options[options.length - 1];
                const otherOptions = options.slice(0, -1);
                spoken = `Sure! Would you like ${otherOptions.join(', ')}, or ${lastOption}?`;
              } else if (options.length > 4) {
                spoken = `I can help with that! ${sq.question}`;
              } else {
                spoken = sq.question || "Could you be more specific about what you'd like to see?";
              }

              console.log('[Converse] Disambiguation triggered — returning clarify with options:', options.join(', '));

              return res.json({
                type: 'clarify',
                spoken,
                disambiguation: {
                  slot_questions: classResult.slot_questions,
                  suggestions: classResult.suggestions,
                },
                token_usage: {
                  input_tokens: (usage.input_tokens || 0),
                  output_tokens: (usage.output_tokens || 0),
                  total_tokens: (usage.input_tokens || 0) + (usage.output_tokens || 0),
                },
              });
            }

            if (classResult.suggestions && classResult.suggestions.length > 0) {
              const bestSuggestion = classResult.suggestions.find(s => s.match_quality === 'exact' || s.match_quality === 'close');
              if (bestSuggestion) {
                console.log('[Converse] Using classify suggestion:', bestSuggestion.prompt.substring(0, 60));
                queryToRun = bestSuggestion.prompt;
              }
            }
          }

          // Parallelize pipeline prep steps (saves ~1-2s vs sequential)
          const [linkedTableIds, valueDictionaries, fewShotSection, contextSection] = await Promise.all([
            schemaLink(appId, queryToRun, qeConfig.schema_link_threshold),
            loadValueDictionaries(appId),
            getFewShotExamples(appId, queryToRun),
            getContextDocumentSection(appId),
          ]);
          // buildBOKGContext depends on linkedTableIds, so it runs after schemaLink
          const bokgContext = await buildBOKGContext(appId, linkedTableIds, queryToRun, qeConfig.column_link_threshold);

          result = await generateAndExecuteWithRetry(
            appId, queryToRun, bokgContext, appName, fewShotSection, valueDictionaries, contextSection, qeConfig.model
          );
        }

        const genTime = Date.now() - startTime;

        // Build column business names using buildColumnBusinessNames helper
        const columnBusinessNames = await buildColumnBusinessNames(appId, result.columns, result.sql);

        // Generate spoken answer from results
        let spokenAnswer = parsed.spoken || 'Here are your results.';
        if (!result.execError && result.rows.length > 0) {
          const count = result.rows.length;
          const cols = result.columns || [];
          if (count === 1 && cols.length <= 3) {
            const parts = cols.map(c => {
              const name = columnBusinessNames[c.toLowerCase()] || c.replace(/_/g, ' ');
              return `${name} is ${result.rows[0][c] ?? 'not available'}`;
            });
            spokenAnswer = (parsed.spoken ? parsed.spoken + ' ' : '') + parts.join(', and ') + '.';
          } else if (count <= 5) {
            spokenAnswer = (parsed.spoken ? parsed.spoken + ' ' : '') + `I found ${count} result${count !== 1 ? 's' : ''}. The details are on your screen.`;
          } else {
            spokenAnswer = (parsed.spoken ? parsed.spoken + ' ' : '') + `I found ${count} results. They're displayed on your screen now.`;
          }
        } else if (result.execError) {
          spokenAnswer = "I ran into an issue with that query. Could you try rephrasing?";
        }

        // NOTE: "anything else?" is now handled client-side based on conversation phase.
        // Do NOT append it here — it would conflict with the collect-then-execute flow.

        // Save to test_queries (no TTS here — client fetches TTS while data renders immediately)
        let queryId = null;
        try {
          const confidence = result.execError ? 0.3 : (result.retryCount > 0 ? 0.6 : 0.8);
          const saveResult = await query(
            `INSERT INTO test_queries (app_id, user_id, nl_query, generated_sql, execution_result, confidence, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, NOW()) RETURNING id`,
            [appId, req.user.id, queryToRun, result.sql,
             JSON.stringify({ rows: result.rows.slice(0, 10), error: result.execError, row_count: result.rows.length }),
             confidence]
          );
          queryId = saveResult.rows[0]?.id || null;
        } catch (saveErr) {
          console.warn('[Converse] Failed to save query:', saveErr.message);
        }

        return res.json({
          type: 'answer',
          spoken: spokenAnswer,
          query: queryToRun,
          sql: result.sql,
          queryId,
          rows: result.rows.slice(0, 200),
          columns: result.columns,
          column_business_names: columnBusinessNames,
          row_count: result.rows.length,
          executionTime: result.execTime ? `${result.execTime}ms` : null,
          generationTime: `${Date.now() - startTime}ms`,
          error: result.execError,
          token_usage: {
            input_tokens: (usage.input_tokens || 0) + (result.token_usage?.input_tokens || 0),
            output_tokens: (usage.output_tokens || 0) + (result.token_usage?.output_tokens || 0),
            total_tokens: (usage.input_tokens || 0) + (usage.output_tokens || 0) +
                          (result.token_usage?.input_tokens || 0) + (result.token_usage?.output_tokens || 0),
          },
          retryCount: result.retryCount,
          schemaLinked: true,
          fewShotUsed: typeof fewShotSection !== 'undefined' && fewShotSection.length > 0,
          explanation: result.rawExplanation || '',
        });
      } catch (queryErr) {
        console.error('[Converse] Query execution failed:', queryErr.message);
        return res.json({
          type: 'clarify',
          spoken: "I had trouble looking that up. Could you try asking in a different way?",
          token_usage: { input_tokens: usage.input_tokens || 0, output_tokens: usage.output_tokens || 0, total_tokens: (usage.input_tokens || 0) + (usage.output_tokens || 0) },
        });
      }
    }

    // ── MULTI-QUERY: process multiple queries in parallel ──
    if (parsed.type === 'multi_query' && parsed.queries && parsed.queries.length > 0) {
      try {
        const runnableQueries = parsed.queries.filter(q => !q.needs_clarification && q.query);
        const needsClarification = parsed.queries.filter(q => q.needs_clarification);

        console.log('[Converse] Multi-query:', runnableQueries.length, 'runnable,', needsClarification.length, 'need clarification');

        // Process all runnable queries in parallel
        const results = await Promise.all(runnableQueries.map(async (qItem) => {
          try {
            let queryToRun = qItem.query;

            // FAST PATH: check for cached proven SQL first
            const templateCheck = await query(
              `SELECT nl_query, generated_sql FROM test_queries
               WHERE app_id = $1 AND feedback = 'thumbs_up'
               AND LOWER(nl_query) = LOWER($2) LIMIT 1`, [appId, qItem.query]
            );

            let result = null;
            if (templateCheck.rows.length > 0 && templateCheck.rows[0].generated_sql) {
              queryToRun = templateCheck.rows[0].nl_query;
              const cachedSQL = templateCheck.rows[0].generated_sql;
              console.log('[Converse/Multi] FAST PATH for:', qItem.label, '→', queryToRun.substring(0, 50));
              try {
                const execStart = Date.now();
                const execResult = await query(cachedSQL);
                result = {
                  sql: cachedSQL,
                  rows: execResult.rows || [],
                  columns: execResult.rows.length > 0 ? Object.keys(execResult.rows[0]) : [],
                  execTime: Date.now() - execStart,
                  execError: null,
                  retryCount: 0,
                  token_usage: { input_tokens: 0, output_tokens: 0 },
                };
              } catch (execErr) {
                console.warn('[Converse/Multi] Cached SQL failed for:', qItem.label, execErr.message);
                result = null;
              }
            }

            if (!result) {
              // FULL PATH: NL2SQL pipeline with parallel prep
              if (templateCheck.rows.length > 0) queryToRun = templateCheck.rows[0].nl_query;

              const [linkedTableIds, valueDictionaries, fewShotSection, contextSection] = await Promise.all([
                schemaLink(appId, queryToRun, qeConfig.schema_link_threshold),
                loadValueDictionaries(appId),
                getFewShotExamples(appId, queryToRun),
                getContextDocumentSection(appId),
              ]);
              const bokgContext = await buildBOKGContext(appId, linkedTableIds, queryToRun, qeConfig.column_link_threshold);

              result = await generateAndExecuteWithRetry(
                appId, queryToRun, bokgContext, appName, fewShotSection, valueDictionaries, contextSection, qeConfig.model
              );
            }

            const columnBusinessNames = await buildColumnBusinessNames(appId, result.columns, result.sql);

            // Save to test_queries
            let queryId = null;
            try {
              const confidence = result.execError ? 0.3 : (result.retryCount > 0 ? 0.6 : 0.8);
              const saveResult = await query(
                `INSERT INTO test_queries (app_id, user_id, nl_query, generated_sql, execution_result, confidence, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, NOW()) RETURNING id`,
                [appId, req.user.id, queryToRun, result.sql,
                 JSON.stringify({ rows: result.rows.slice(0, 10), error: result.execError, row_count: result.rows.length }),
                 confidence]
              );
              queryId = saveResult.rows[0]?.id || null;
            } catch (saveErr) {
              console.warn('[Converse/Multi] Failed to save query:', saveErr.message);
            }

            return {
              label: qItem.label || queryToRun,
              query: queryToRun,
              sql: result.sql,
              queryId,
              rows: result.rows.slice(0, 200),
              columns: result.columns,
              column_business_names: columnBusinessNames,
              row_count: result.rows.length,
              executionTime: result.execTime ? `${result.execTime}ms` : null,
              error: result.execError,
              retryCount: result.retryCount,
              token_usage: result.token_usage || {},
            };
          } catch (qErr) {
            console.error('[Converse/Multi] Query failed:', qItem.label, qErr.message);
            return {
              label: qItem.label || qItem.query,
              query: qItem.query,
              error: qErr.message,
              rows: [],
              columns: [],
              row_count: 0,
            };
          }
        }));

        // Build spoken summary from results
        const summaryParts = results.map(r => {
          if (r.error) return `I couldn't get the ${r.label}.`;
          if (r.row_count === 1 && r.columns.length <= 3) {
            // Single-value result — read it out
            const vals = r.columns.map(c => {
              const name = (r.column_business_names && r.column_business_names[c.toLowerCase()]) || c.replace(/_/g, ' ');
              return `${name} is ${formatSpokenValue(r.rows[0][c], c)}`;
            });
            return `For ${r.label}: ${vals.join(', ')}.`;
          }
          return `For ${r.label}, I found ${r.row_count} result${r.row_count !== 1 ? 's' : ''}.`;
        });

        let spokenSummary = parsed.spoken ? parsed.spoken + ' ' : '';
        spokenSummary += summaryParts.join(' ');

        // If any queries need clarification, append that
        if (needsClarification.length > 0) {
          const clarifyPart = needsClarification.map(q => q.clarify_spoken || `I need a bit more detail on ${q.label}.`).join(' ');
          spokenSummary += ' ' + clarifyPart;
        }
        // NOTE: "anything else?" handled client-side based on conversation phase

        // Aggregate token usage
        const totalTokens = results.reduce((acc, r) => ({
          input: acc.input + (r.token_usage?.input_tokens || 0),
          output: acc.output + (r.token_usage?.output_tokens || 0),
        }), { input: usage.input_tokens || 0, output: usage.output_tokens || 0 });

        // No TTS here — client fetches TTS while data renders immediately
        return res.json({
          type: 'multi_answer',
          spoken: spokenSummary.trim(),
          results,
          result_count: runnableQueries.length,  // how many things we actually looked up
          needs_clarification: needsClarification.length > 0 ? needsClarification : undefined,
          generationTime: `${Date.now() - startTime}ms`,
          token_usage: {
            input_tokens: totalTokens.input,
            output_tokens: totalTokens.output,
            total_tokens: totalTokens.input + totalTokens.output,
          },
        });
      } catch (multiErr) {
        console.error('[Converse] Multi-query processing failed:', multiErr.message);
        return res.json({
          type: 'clarify',
          spoken: "I had trouble pulling all of those. Could you try asking one at a time?",
          token_usage: { input_tokens: usage.input_tokens || 0, output_tokens: usage.output_tokens || 0, total_tokens: (usage.input_tokens || 0) + (usage.output_tokens || 0) },
        });
      }
    }

    // Non-query responses (clarify, greeting, farewell) — include inline TTS
    const nonQuerySpoken = parsed.spoken || "Could you tell me more about what you're looking for?";
    const nonQueryTTS = await generateTTSBase64(nonQuerySpoken, clientVoice || 'nova');
    res.json({
      type: parsed.type || 'clarify',
      spoken: nonQuerySpoken,
      audio_base64: nonQueryTTS || undefined,
      token_usage: {
        input_tokens: usage.input_tokens || 0,
        output_tokens: usage.output_tokens || 0,
        total_tokens: (usage.input_tokens || 0) + (usage.output_tokens || 0),
      },
    });
  } catch (err) {
    console.error('[Converse] Error:', err);
    res.status(500).json({ error: err.message || 'Conversation failed' });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// BATCH EXECUTE — run multiple queued queries from conversation mode
// ─────────────────────────────────────────────────────────────────────────────
router.post('/:appId/converse/execute', async (req, res) => {
  try {
    const { appId } = req.params;
    const { queries, assistantName = 'Claudia', voice: clientVoice } = req.body;

    if (!queries || !Array.isArray(queries) || queries.length === 0) {
      return res.status(400).json({ error: 'queries array is required' });
    }

    const dataAvailable = await hasSourceData(appId);
    if (!dataAvailable) {
      return res.status(400).json({ error: 'Source data not loaded. Run the pipeline first.' });
    }

    const startTime = Date.now();
    const appResult = await query('SELECT name, config FROM applications WHERE id = $1', [appId]);
    const appName = appResult.rows[0]?.name || 'Unknown';
    const appConfig = appResult.rows[0]?.config || {};
    const qeConfig = appConfig.query_engine || {};

    console.log('[BatchExec] Processing', queries.length, 'queued queries');

    // Process all queries in parallel (same logic as multi_query handler)
    const results = await Promise.all(queries.map(async (qItem) => {
      try {
        // ── DOCUMENT QUERY PATH: route to unstructured RAG engine ──
        if (qItem.intent === 'UNSTRUCTURED') {
          try {
            const { answerFromDocuments } = require('../services/unstructured-engine');
            const docResult = await answerFromDocuments(qItem.query, { appId, conversationHistory: [] });
            return {
              label: qItem.label || 'Document Search',
              query: qItem.query,
              intent: 'UNSTRUCTURED',
              answer: docResult.answer || 'No relevant documents found.',
              citations: docResult.citations || [],
              confidence: docResult.confidence || 'medium',
              rows: [], columns: [], row_count: 0,
              token_usage: docResult.tokenUsage || {},
            };
          } catch (docErr) {
            console.error('[BatchExec] Doc query failed:', qItem.label, docErr.message);
            return {
              label: qItem.label || qItem.query,
              query: qItem.query,
              intent: 'UNSTRUCTURED',
              answer: `Document search error: ${docErr.message}`,
              citations: [],
              error: docErr.message,
              rows: [], columns: [], row_count: 0,
            };
          }
        }

        // ── STRUCTURED QUERY PATH: NL2SQL pipeline ──
        let queryToRun = qItem.query;

        // FAST PATH: check for cached proven SQL first
        const templateCheck = await query(
          `SELECT nl_query, generated_sql FROM test_queries
           WHERE app_id = $1 AND feedback = 'thumbs_up'
           AND LOWER(nl_query) = LOWER($2) LIMIT 1`, [appId, qItem.query]
        );

        let result = null;
        if (templateCheck.rows.length > 0 && templateCheck.rows[0].generated_sql) {
          queryToRun = templateCheck.rows[0].nl_query;
          const cachedSQL = templateCheck.rows[0].generated_sql;
          console.log('[BatchExec] FAST PATH for:', qItem.label, '→', queryToRun.substring(0, 50));
          try {
            const execStart = Date.now();
            const execResult = await query(cachedSQL);
            result = {
              sql: cachedSQL,
              rows: execResult.rows || [],
              columns: execResult.rows.length > 0 ? Object.keys(execResult.rows[0]) : [],
              execTime: Date.now() - execStart,
              execError: null,
              retryCount: 0,
              token_usage: { input_tokens: 0, output_tokens: 0 },
            };
          } catch (execErr) {
            console.warn('[BatchExec] Cached SQL failed for:', qItem.label, execErr.message);
            result = null;
          }
        }

        if (!result) {
          // FULL PATH: NL2SQL pipeline
          if (templateCheck.rows.length > 0) queryToRun = templateCheck.rows[0].nl_query;

          const [linkedTableIds, valueDictionaries, fewShotSection, contextSection] = await Promise.all([
            schemaLink(appId, queryToRun, qeConfig.schema_link_threshold),
            loadValueDictionaries(appId),
            getFewShotExamples(appId, queryToRun),
            getContextDocumentSection(appId),
          ]);
          const bokgContext = await buildBOKGContext(appId, linkedTableIds, queryToRun, qeConfig.column_link_threshold);

          result = await generateAndExecuteWithRetry(
            appId, queryToRun, bokgContext, appName, fewShotSection, valueDictionaries, contextSection, qeConfig.model
          );
        }

        const columnBusinessNames = await buildColumnBusinessNames(appId, result.columns, result.sql);

        // Save to test_queries
        let queryId = null;
        try {
          const confidence = result.execError ? 0.3 : (result.retryCount > 0 ? 0.6 : 0.8);
          const saveResult = await query(
            `INSERT INTO test_queries (app_id, user_id, nl_query, generated_sql, execution_result, confidence, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, NOW()) RETURNING id`,
            [appId, req.user.id, queryToRun, result.sql,
             JSON.stringify({ rows: result.rows.slice(0, 10), error: result.execError, row_count: result.rows.length }),
             confidence]
          );
          queryId = saveResult.rows[0]?.id || null;
        } catch (saveErr) {
          console.warn('[BatchExec] Failed to save query:', saveErr.message);
        }

        return {
          label: qItem.label || queryToRun,
          query: queryToRun,
          sql: result.sql,
          queryId,
          rows: result.rows.slice(0, 200),
          columns: result.columns,
          column_business_names: columnBusinessNames,
          row_count: result.rows.length,
          executionTime: result.execTime ? `${result.execTime}ms` : null,
          error: result.execError,
          retryCount: result.retryCount,
          token_usage: result.token_usage || {},
        };
      } catch (qErr) {
        console.error('[BatchExec] Query failed:', qItem.label, qErr.message);
        return {
          label: qItem.label || qItem.query,
          query: qItem.query,
          error: qErr.message,
          rows: [], columns: [], row_count: 0,
        };
      }
    }));

    // Build spoken summary from results
    const summaryParts = results.map(r => {
      if (r.error) return `I couldn't get the ${r.label}.`;
      // Document results: summarize the answer text
      if (r.intent === 'UNSTRUCTURED' && r.answer) {
        const shortAnswer = r.answer.length > 100 ? r.answer.substring(0, 97).replace(/\s+\S*$/, '') + '…' : r.answer;
        return `For ${r.label}: ${shortAnswer}`;
      }
      if (r.row_count === 1 && r.columns.length <= 3) {
        const vals = r.columns.map(c => {
          const name = (r.column_business_names && r.column_business_names[c.toLowerCase()]) || c.replace(/_/g, ' ');
          return `${name} is ${formatSpokenValue(r.rows[0][c], c)}`;
        });
        return `For ${r.label}: ${vals.join(', ')}.`;
      }
      return `For ${r.label}, I found ${r.row_count} result${r.row_count !== 1 ? 's' : ''}.`;
    });

    let spokenSummary = `Here's everything you asked for. ` + summaryParts.join(' ');

    // Aggregate token usage
    const totalTokens = results.reduce((acc, r) => ({
      input: acc.input + (r.token_usage?.input_tokens || 0),
      output: acc.output + (r.token_usage?.output_tokens || 0),
    }), { input: 0, output: 0 });

    return res.json({
      type: 'multi_answer',
      spoken: spokenSummary.trim(),
      results,
      result_count: results.length,
      generationTime: `${Date.now() - startTime}ms`,
      token_usage: {
        input_tokens: totalTokens.input,
        output_tokens: totalTokens.output,
        total_tokens: totalTokens.input + totalTokens.output,
      },
    });
  } catch (err) {
    console.error('[BatchExec] Error:', err);
    res.status(500).json({ error: err.message || 'Batch execution failed' });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// BRIEFING DOCUMENT GENERATION — creates an HTML briefing from conversation results
// ─────────────────────────────────────────────────────────────────────────────
router.post('/:appId/converse/briefing', async (req, res) => {
  try {
    const { results, userName = 'User', assistantName = 'Katy', userRequest } = req.body;
    if (!results || !Array.isArray(results) || results.length === 0) {
      return res.status(400).json({ error: 'results array is required' });
    }

    const now = new Date();
    const dateStr = now.toLocaleDateString('en-US', { weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' });
    const timeStr = now.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });

    // Currency detection: check column name, business name, AND the result label
    const CURRENCY_COL_RE = /amount|total|balance|revenue|booking|payment|invoice|price|cost|value|paid|due|outstanding|sum|collect|receipt|cash/i;
    const isCurrencyCol = (colName, bizName, label) => {
      return CURRENCY_COL_RE.test(colName) || CURRENCY_COL_RE.test(bizName || '') || CURRENCY_COL_RE.test(label || '');
    };
    const fmtCurrency = (val) => `$${val.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

    // Build data sections for the briefing
    const sections = results.map(r => {
      const label = r.label || 'Query Result';
      const columns = r.columns || [];
      const rows = r.rows || [];
      const bizNames = r.column_business_names || {};

      // Format column headers using business names
      const headers = columns.map(c => bizNames[c.toLowerCase()] || c.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()));

      // Helper: coerce value to number if it looks numeric (PG returns NUMERIC/BIGINT as strings)
      const asNum = (v) => {
        if (typeof v === 'number') return v;
        if (typeof v === 'string' && /^-?\d[\d,]*\.?\d*$/.test(v.trim())) return parseFloat(v.replace(/,/g, ''));
        return null;
      };

      // Build table rows HTML
      const tableRows = rows.slice(0, 50).map(row => {
        const cells = columns.map(c => {
          const val = row[c];
          if (val === null || val === undefined) return '<td>—</td>';
          const num = asNum(val);
          if (num !== null) {
            const currency = isCurrencyCol(c, bizNames[c.toLowerCase()], label);
            if (currency) return `<td style="text-align:right">${fmtCurrency(num)}</td>`;
            return `<td style="text-align:right">${num.toLocaleString('en-US')}</td>`;
          }
          return `<td>${String(val)}</td>`;
        }).join('');
        return `<tr>${cells}</tr>`;
      }).join('');

      // Key metrics summary for single-row results
      let keyMetrics = '';
      if (rows.length === 1 && columns.length <= 5) {
        keyMetrics = columns.map(c => {
          const name = bizNames[c.toLowerCase()] || c.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase());
          const val = rows[0][c];
          const num = asNum(val);
          if (num !== null) {
            const currency = isCurrencyCol(c, bizNames[c.toLowerCase()], label);
            const formatted = currency ? fmtCurrency(num) : num.toLocaleString('en-US');
            return `<div class="metric"><span class="metric-label">${name}</span><span class="metric-value">${formatted}</span></div>`;
          }
          return `<div class="metric"><span class="metric-label">${name}</span><span class="metric-value">${val}</span></div>`;
        }).join('');
      }

      return { label, headers, tableRows, keyMetrics, rowCount: r.row_count || rows.length };
    });

    // Build the full HTML briefing
    const sectionsHtml = sections.map(s => `
      <div class="section">
        <h2>${s.label}</h2>
        ${s.keyMetrics ? `<div class="metrics-row">${s.keyMetrics}</div>` : ''}
        ${s.rowCount > 1 || !s.keyMetrics ? `
        <table>
          <thead><tr>${s.headers.map(h => `<th>${h}</th>`).join('')}</tr></thead>
          <tbody>${s.tableRows}</tbody>
        </table>
        ${s.rowCount > 50 ? `<p class="note">Showing 50 of ${s.rowCount} rows</p>` : ''}
        ` : ''}
      </div>
    `).join('');

    const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Data Briefing — ${dateStr}</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f8f9fa; color: #1a1a2e; padding: 40px; max-width: 900px; margin: 0 auto; }
  .header { border-bottom: 3px solid #6366f1; padding-bottom: 20px; margin-bottom: 30px; }
  .header h1 { font-size: 28px; color: #1a1a2e; margin-bottom: 4px; }
  .header .subtitle { color: #666; font-size: 14px; }
  .header .meta { display: flex; gap: 20px; margin-top: 12px; font-size: 13px; color: #888; }
  .section { background: white; border-radius: 8px; padding: 24px; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
  .section h2 { font-size: 18px; color: #6366f1; margin-bottom: 16px; border-bottom: 1px solid #e5e7eb; padding-bottom: 8px; }
  .metrics-row { display: flex; gap: 24px; flex-wrap: wrap; }
  .metric { flex: 1; min-width: 150px; background: #f0f1ff; border-radius: 8px; padding: 16px; text-align: center; }
  .metric-label { display: block; font-size: 12px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
  .metric-value { display: block; font-size: 24px; font-weight: 700; color: #1a1a2e; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { background: #f8f9fa; font-weight: 600; text-align: left; padding: 10px 12px; border-bottom: 2px solid #e5e7eb; color: #374151; }
  td { padding: 8px 12px; border-bottom: 1px solid #f0f0f0; }
  tr:hover { background: #f8f9fa; }
  .note { font-size: 12px; color: #888; margin-top: 8px; font-style: italic; }
  .footer { text-align: center; margin-top: 40px; padding-top: 20px; border-top: 1px solid #e5e7eb; color: #999; font-size: 12px; }
  @media print { body { padding: 20px; } .section { box-shadow: none; border: 1px solid #e5e7eb; } }
</style>
</head>
<body>
  <div class="header">
    <h1>Data Briefing</h1>
    <div class="subtitle">Prepared by ${assistantName} for ${userName}</div>
    <div class="meta">
      <span>${dateStr}</span>
      <span>${timeStr}</span>
      <span>${results.length} data point${results.length !== 1 ? 's' : ''}</span>
    </div>
  </div>
  ${sectionsHtml}
  <div class="footer">
    Generated by Data Ask &mdash; Solix Technologies<br>
    ${dateStr} at ${timeStr}
  </div>
</body>
</html>`;

    return res.json({ html, generatedAt: now.toISOString() });
  } catch (err) {
    console.error('[Briefing] Error:', err);
    res.status(500).json({ error: err.message || 'Briefing generation failed' });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// SHARED CLASSIFY LOGIC — used by both /classify route and /converse route
// ─────────────────────────────────────────────────────────────────────────────
async function classifyIntent(appId, question) {
  // Get app context for classification
  const appResult = await query('SELECT name FROM applications WHERE id = $1', [appId]);
  const appName = appResult.rows[0]?.name || 'Unknown';

  // Get table/column info with entity_metadata (business questions, sample questions)
  const tablesResult = await query(
    `SELECT t.table_name, t.entity_name, t.entity_metadata, t.description
     FROM app_tables t WHERE t.app_id = $1`, [appId]);

  // Build BOKG catalog: business objects with their questions
  const bokgCatalog = [];
  const allBusinessQuestions = [];
  for (const t of tablesResult.rows) {
    const entityName = t.entity_name || t.table_name;
    const meta = t.entity_metadata || {};
    const questions = meta.sample_questions || [];
    const desc = t.description || meta.description || '';
    bokgCatalog.push(`  - ${entityName}: ${desc}`);
    for (const q of questions) {
      bokgCatalog.push(`    Q: "${q}"`);
      allBusinessQuestions.push({ prompt: q, object: entityName });
    }
  }

  // Get validated query patterns from QPD (proven queries with thumbs-up)
  let validatedPatterns = [];
  try {
    const qpdResult = await query(
      `SELECT nl_query FROM test_queries
       WHERE app_id = $1 AND feedback = 'thumbs_up'
       ORDER BY created_at DESC LIMIT 20`, [appId]);
    validatedPatterns = qpdResult.rows.map(r => r.nl_query);
  } catch (e) { /* ignore */ }

  const API_KEY = process.env.ANTHROPIC_API_KEY;
  if (!API_KEY) throw new Error('ANTHROPIC_API_KEY not set');

  const classifyPrompt = `You are an intent classifier for the Data Ask natural language query system.
You have access to a Knowledge Graph that covers the following business objects and questions for the "${appName}" database.

YOUR TASK:
Given a user question, return a JSON object with this exact structure:
{
  "confidence": "high" | "medium" | "low",
  "interpretation": "One sentence describing what you understand the user is asking",
  "disambiguation_needed": true | false,
  "disambiguation_reason": "Why disambiguation is needed (or empty if not needed)",
  "slots": {
    "metric": {"value": "...", "status": "filled" | "ambiguous" | "missing"},
    "entity": {"value": "...", "status": "filled" | "ambiguous" | "missing"},
    "time_period": {"value": "...", "status": "filled" | "ambiguous" | "missing"},
    "scope": {"value": "...", "status": "filled" | "ambiguous" | "missing"},
    "output_format": {"value": "...", "status": "filled" | "ambiguous" | "missing"}
  },
  "suggestions": [
    {
      "prompt": "A specific, runnable question from the BOKG",
      "object": "BusinessObject",
      "match_quality": "exact" | "close" | "related"
    }
  ],
  "slot_questions": [
    {
      "slot": "metric",
      "question": "Would you like bookings, revenue, or order counts?",
      "options": ["Bookings", "Revenue", "Order counts"]
    },
    {
      "slot": "time_period",
      "question": "For what time period?",
      "options": ["This quarter", "This year", "Last 12 months"]
    },
    {
      "slot": "output_format",
      "question": "How would you like it broken down?",
      "options": ["By month", "By quarter", "Summary total"]
    }
  ]
}

RULES:
1. CONFIDENCE LEVELS:
   - "high": The question specifies WHAT to retrieve or compute AND which business object(s)
     are involved. The question is translatable to a specific SQL query without guessing.
     Examples of high:
     "Show me standard cost × quantity on hand by item for top 10 items" (clear metric + entity + format),
     "How many accounts were opened each year" (metric + entity + grouping),
     "Total invoice amount for 2024" (metric + entity + time),
     "Top 10 customers by revenue" (metric + entity + format),
     "List all open purchase orders" (entity + filter).
     A time period is NOT required for high confidence — many valid queries don't need one.
   - "medium": The right business object is identifiable but the question is vague about
     WHAT specifically to show. The system could generate multiple different valid queries.
     Examples: "show me sales" (totals? counts? by what?), "what about invoices" (what metric?).
     Set disambiguation_needed=true. Include suggestions AND slot_questions.
   - "low": Vague, cross-object, completely ambiguous, or out of scope.
     Examples: "show me data", "what do we have", "who is the president".
     Set disambiguation_needed=true. Include broad suggestions.

   IMPORTANT: A question that ONLY names a business object without ANY indication of what to
   show is "medium". "show me sales" → medium. But a question that names an object AND
   specifies what to compute/display IS "high" even without a time filter.

2. SLOT DEFINITIONS:
   - metric: What is being measured or retrieved (e.g., "total amount", "count of records",
     "balance", "value"). Status is "filled" if the user specified a clear measurable,
     "ambiguous" if the metric could mean multiple things, "missing" if they just said
     "show me data" or similar.
   - entity: The main business object or filter subject (e.g., "accounts", "invoices",
     "customers", "transactions"). Status "filled" if named, "missing" if generic.
   - time_period: The time range or period. Status "filled" if explicit, "ambiguous"
     if vague ("recent", "lately"), "missing" if absent.
   - scope: Organizational or structural scope (e.g., department, region, category).
     Status "filled" if specified, "missing" if not — but missing is OK for many queries.
   - output_format: How results should be structured (e.g., "top 10", "by month",
     "summary", "detail list"). Status "filled" if indicated, "missing" if not —
     reasonable defaults exist for most queries.

3. SUGGESTIONS:
   - ALWAYS pull suggestions from the BOKG business questions listed below.
   - Match quality "exact" = user's question matches a BOKG question almost word-for-word.
   - Match quality "close" = same intent/metric with minor differences.
   - Match quality "related" = same business object, different specific question.
   - Include 2-4 suggestions, ranked by relevance.

4. SLOT QUESTIONS — ALWAYS GENERATE MULTIPLE LAYERS:
   - Generate slot_questions for EACH slot with status "ambiguous" or "missing" where
     the answer would change which query is generated.
   - For medium-confidence questions, you should typically produce 2-3 slot questions:
     one for metric/what, one for time_period, and optionally one for output_format/grouping.
   - Provide 3-4 concrete options per slot grounded in what the BOKG can actually answer.
   - Do NOT ask about scope unless the user's question specifically implies it matters.
   - Frame the question conversationally, as if speaking to a colleague:
     "Would you like bookings, revenue, or order counts?" — NOT generic labels like
     "Total revenue amount" or "Number of orders booked".
   - Use the SAME business terminology the voice assistant would use. Options should
     be short, natural phrases (2-4 words each), not formal descriptions.

   DISAMBIGUATION EXAMPLES — use these as templates for common broad terms:
   - "sales" / "sales numbers" / "sales data" → question: "Would you like bookings, revenue, or order counts?", options: ["Bookings", "Revenue", "Order counts"]
   - "financial data" / "financials" → question: "Which area — AP, AR, or GL?", options: ["Accounts Payable", "Accounts Receivable", "General Ledger"]
   - "inventory" / "inventory data" → question: "Would you like on-hand quantities, item details, or inventory value?", options: ["On-hand quantities", "Item details", "Inventory value"]
   - "purchasing" / "procurement" → question: "Would you like purchase orders, vendor spend, or receiving activity?", options: ["Purchase orders", "Vendor spend", "Receiving activity"]
   - "customers" / "customer data" → question: "Would you like a customer list, AR balances, or order history?", options: ["Customer list", "AR balances", "Order history"]
   These are examples — adapt to the actual BOKG objects available. The key principle:
   offer 2-3 SPECIFIC metrics the user likely means, using plain business language.

5. SPECIAL CASES:
   - If the question asks about individual people (names, PII), set confidence="high"
     and include a note in interpretation that PII rules apply. Suggest aggregate alternatives.
   - If the question is completely out of scope (not about this database's data), set
     confidence="low" and explain in disambiguation_reason.

RESPOND WITH ONLY THE JSON OBJECT. No markdown, no explanation, no code fences.

=== AVAILABLE BUSINESS OBJECTS AND QUESTIONS ===
${bokgCatalog.join('\n')}

${validatedPatterns.length > 0 ? `=== VALIDATED QUERY PATTERNS (highest-confidence suggestions) ===\n${validatedPatterns.map(p => `  "${p}"`).join('\n')}` : ''}`;

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1024,
      temperature: 0,
      system: classifyPrompt,
      messages: [{ role: 'user', content: question }],
    }),
  });

  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`Claude API error ${response.status}: ${errText}`);
  }

  const data = await response.json();
  const rawText = data.content[0].text.trim();
  const usage = data.usage || {};

  // Try to parse JSON response
  let classification;
  try {
    classification = JSON.parse(rawText);
  } catch (e) {
    const jsonMatch = rawText.match(/\{[\s\S]*\}/);
    if (jsonMatch) {
      classification = JSON.parse(jsonMatch[0]);
    } else {
      classification = { confidence: 'medium', disambiguation_needed: false, interpretation: question, clarifying_questions: [], suggestions: [] };
    }
  }

  // Server-side slot validation: override LLM confidence based on slot specificity
  if (classification.slots) {
    const metricSlot = classification.slots.metric;
    const entitySlot = classification.slots.entity;
    const timePeriodSlot = classification.slots.time_period;
    const formatSlot = classification.slots.output_format;

    // Count how many slots are filled
    const filledCount = [metricSlot, entitySlot, timePeriodSlot, formatSlot]
      .filter(s => s && s.status === 'filled').length;

    // UPGRADE: If query is specific enough (metric filled or ambiguous + 2 other slots),
    // override LLM's disambiguation_needed to false. Queries like "Show me bookings
    // for last 12 months by month" (metric=ambiguous, time=filled, format=filled)
    // are specific enough for SQL generation — the LLM sometimes over-disambiguates.
    if (metricSlot && metricSlot.status !== 'missing' && filledCount >= 2) {
      if (classification.disambiguation_needed) {
        console.log('[Classify] Override: query has', filledCount, 'filled slots — suppressing disambiguation');
        classification.disambiguation_needed = false;
        classification.confidence = 'high';
      }
    }

    // DOWNGRADE: Metric completely missing → force disambiguation
    if (metricSlot && metricSlot.status === 'missing') {
      if (classification.confidence === 'high') {
        classification.confidence = 'medium';
        classification.disambiguation_needed = true;
        if (!classification.disambiguation_reason) {
          classification.disambiguation_reason = 'The question needs more specificity about what metric or measurement you want.';
        }
      }
    }

    // Both metric and entity missing → very vague
    if (metricSlot && entitySlot &&
        metricSlot.status === 'missing' && entitySlot.status === 'missing') {
      classification.confidence = 'low';
      classification.disambiguation_needed = true;
    }
  } else {
    const wordCount = question.trim().split(/\s+/).length;
    if (wordCount <= 4 && classification.confidence === 'high') {
      classification.confidence = 'medium';
      classification.disambiguation_needed = true;
      classification.disambiguation_reason = classification.disambiguation_reason ||
        'This is a broad question. Let me help you be more specific.';
    }
  }

  // Log token usage
  try {
    const costEstimate = ((usage.input_tokens || 0) * 3 / 1000000) + ((usage.output_tokens || 0) * 15 / 1000000);
    await query(
      `INSERT INTO token_usage (app_id, stage, table_name, input_tokens, output_tokens, total_tokens, model, cost_estimate)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
      [appId, 'classify', null, usage.input_tokens || 0, usage.output_tokens || 0,
       (usage.input_tokens || 0) + (usage.output_tokens || 0), 'claude-sonnet-4-20250514', costEstimate]
    );
  } catch (tokenErr) {
    console.warn('Failed to log classify token usage:', tokenErr.message);
  }

  console.log('[Classify] Question:', question, '→ confidence:', classification.confidence,
    'disambig:', classification.disambiguation_needed,
    'metric:', classification.slots?.metric?.status,
    'entity:', classification.slots?.entity?.status);

  return { classification, usage };
}

// POST /api/test/:appId/classify - Classify question intent and detect ambiguity
router.post('/:appId/classify', async (req, res) => {
  try {
    const { appId } = req.params;
    const { question } = req.body;
    if (!question) return res.status(400).json({ error: 'Question is required' });

    const { classification, usage } = await classifyIntent(appId, question);

    res.json({
      status: 'ok',
      version: '3d-v2',
      classification,
      token_usage: {
        input_tokens: usage.input_tokens || 0,
        output_tokens: usage.output_tokens || 0,
        total_tokens: (usage.input_tokens || 0) + (usage.output_tokens || 0),
      }
    });
  } catch (err) {
    console.error('Classification error:', err);
    res.status(500).json({ error: err.message || 'Classification failed' });
  }
});

// POST /api/test/:appId/nl2sql - NL-to-SQL with BOKG context + Phase 2 improvements
router.post('/:appId/nl2sql', async (req, res) => {
  try {
    const { appId } = req.params;
    const { question } = req.body;

    if (!question) {
      return res.status(400).json({ error: 'Question is required' });
    }

    // Check source data is available
    const dataAvailable = await hasSourceData(appId);
    if (!dataAvailable) {
      return res.status(400).json({ error: 'Source data not loaded. Run the pipeline first.' });
    }

    // Get app info + config
    const appResult = await query('SELECT name, config FROM applications WHERE id = $1', [appId]);
    const appName = appResult.rows[0]?.name || 'Unknown';
    const appConfig = appResult.rows[0]?.config || {};
    const qeConfig = appConfig.query_engine || {};

    const startTime = Date.now();

    // Phase 2: Schema linking for large schemas (configurable thresholds)
    const linkedTableIds = await schemaLink(appId, question, qeConfig.schema_link_threshold);

    // Build BOKG context (with optional schema linking + column filtering)
    const bokgContext = await buildBOKGContext(appId, linkedTableIds, question, qeConfig.column_link_threshold);

    // Phase 2: Load value dictionaries for case-fix
    const valueDictionaries = await loadValueDictionaries(appId);

    // Phase 2: Load few-shot examples from proven queries
    const fewShotSection = await getFewShotExamples(appId, question);

    // Merged Enrichment: Load context documents for prompt injection
    const contextSection = await getContextDocumentSection(appId);

    // Phase 2: Generate + execute with retry loop (configurable model)
    const result = await generateAndExecuteWithRetry(
      appId, question, bokgContext, appName, fewShotSection, valueDictionaries, contextSection, qeConfig.model
    );

    const genTime = Date.now() - startTime;

    // Log token usage
    try {
      const costEstimate = (result.token_usage.input_tokens * COST_PER_INPUT_TOKEN) +
                           (result.token_usage.output_tokens * COST_PER_OUTPUT_TOKEN);
      await query(
        `INSERT INTO token_usage (app_id, stage, table_name, input_tokens, output_tokens, total_tokens, model, cost_estimate)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [appId, 'nl2sql', null, result.token_usage.input_tokens, result.token_usage.output_tokens,
         result.token_usage.total_tokens, 'claude-sonnet-4-20250514', costEstimate]
      );
    } catch (tokenErr) {
      console.warn('Failed to log NL2SQL token usage:', tokenErr.message);
    }

    // Save to test_queries table
    let queryId = null;
    try {
      const confidence = result.execError ? 0.3 : (result.retryCount > 0 ? 0.6 : 0.8);
      const saveResult = await query(
        `INSERT INTO test_queries (app_id, user_id, nl_query, generated_sql, execution_result, confidence, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, NOW())
         RETURNING id`,
        [appId, req.user.id, question, result.sql,
         JSON.stringify({ rows: result.rows.slice(0, 10), error: result.execError, row_count: result.rows.length }),
         confidence]
      );
      queryId = saveResult.rows[0]?.id || null;
    } catch (saveErr) {
      console.warn('Failed to save test query:', saveErr.message);
    }

    // Build column business names using shared helper
    const columnBusinessNames = await buildColumnBusinessNames(appId, result.columns, result.sql);

    res.json({
      sql: result.sql,
      queryId,
      rows: result.rows.slice(0, 200),
      columns: result.columns,
      column_business_names: columnBusinessNames,
      row_count: result.rows.length,
      executionTime: result.execTime ? `${result.execTime}ms` : null,
      generationTime: `${genTime}ms`,
      error: result.execError,
      token_usage: result.token_usage,
      retryCount: result.retryCount,
      retryLog: result.retryLog || [],
      schemaLinked: linkedTableIds !== null,
      fewShotUsed: fewShotSection.length > 0,
      contextUsed: contextSection.length > 0,
      explanation: result.rawExplanation || '',
      modelUsed: qeConfig.model || DEFAULT_LLM_MODEL,
      displaySettings: {
        show_token_cost: qeConfig.show_token_cost !== false, // default true
        show_sql_details: qeConfig.show_sql_details !== false, // default true
      },
    });
  } catch (err) {
    console.error('NL2SQL error:', err);
    res.status(500).json({ error: err.message || 'NL-to-SQL failed' });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// QPD SEEDING — Generate + validate SQL for entity-level sample questions
// Seeds verified query patterns into test_queries as the warm-start QPD.
// Runs per business object (entity), not per table.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * POST /api/test/:appId/seed-qpd
 * Collects sample questions from entity_metadata (grouped by business object),
 * generates SQL via the full NL2SQL pipeline (with retry + case-fix),
 * executes each query, and seeds successful ones into test_queries.
 *
 * This creates the warm-start QPD so every published BOKG has proven patterns
 * from day one. User thumbs-up/thumbs-down feedback refines it over time.
 */
router.post('/:appId/seed-qpd', async (req, res) => {
  try {
    const { appId } = req.params;

    // Check source data is available
    const dataAvailable = await hasSourceData(appId);
    if (!dataAvailable) {
      return res.status(400).json({ error: 'Source data not loaded. Run the pipeline first.' });
    }

    // Get app info
    const appResult = await query('SELECT name FROM applications WHERE id = $1', [appId]);
    const appName = appResult.rows[0]?.name || 'Unknown';

    // Clear previous system-seeded QPD entries for this app
    await query(
      `DELETE FROM test_queries WHERE app_id = $1 AND feedback = 'thumbs_up'
       AND nl_query IN (
         SELECT nl_query FROM test_queries WHERE app_id = $1 AND confidence = 0.90
       )
       AND confidence = 0.90`,
      [appId]
    );

    // Collect sample questions grouped by business object (entity_name)
    // Multiple tables may share an entity_name — we collect questions per entity, not per table
    const entitiesResult = await query(
      `SELECT entity_name, table_name, entity_metadata
       FROM app_tables
       WHERE app_id = $1 AND entity_metadata IS NOT NULL
       ORDER BY entity_name, table_name`,
      [appId]
    );

    // Group by entity (business object)
    const entityQuestions = {};
    for (const row of entitiesResult.rows) {
      const entityKey = row.entity_name || row.table_name;
      const meta = row.entity_metadata || {};
      const questions = meta.sample_questions || [];

      if (!entityQuestions[entityKey]) {
        entityQuestions[entityKey] = { tables: [], questions: new Set() };
      }
      entityQuestions[entityKey].tables.push(row.table_name);
      for (const q of questions) {
        entityQuestions[entityKey].questions.add(q);
      }
    }

    console.log(`QPD Seeding: ${Object.keys(entityQuestions).length} business objects found`);

    // Build BOKG context once (reused for all questions)
    const bokgContext = await buildBOKGContext(appId);
    const valueDictionaries = await loadValueDictionaries(appId);
    // NOTE: Context injection removed from QPD seeding — adds overhead without meaningful accuracy gain for seed questions
    const results = {
      entities: Object.keys(entityQuestions).length,
      total_questions: 0,
      seeded: 0,
      failed: 0,
      details: [],
    };

    // Collect all questions with their entity names for parallel processing
    const allTasks = [];
    for (const [entityName, entityData] of Object.entries(entityQuestions)) {
      const questions = Array.from(entityData.questions);
      results.total_questions += questions.length;
      for (const question of questions) {
        allTasks.push({ entityName, question });
      }
    }

    // Process seed questions in parallel batches (concurrency=5 for faster seeding)
    const CONCURRENCY = 5;
    for (let i = 0; i < allTasks.length; i += CONCURRENCY) {
      const batch = allTasks.slice(i, i + CONCURRENCY);
      const batchResults = await Promise.allSettled(batch.map(async ({ entityName, question }) => {
        console.log(`  QPD seed: [${entityName}] "${question.substring(0, 60)}..."`);

        const genResult = await generateAndExecuteWithRetry(
          appId, question, bokgContext, appName, '', valueDictionaries
        );

        if (!genResult.execError && genResult.rows.length > 0) {
          await query(
            `INSERT INTO test_queries (app_id, user_id, nl_query, generated_sql, execution_result, confidence, feedback, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())`,
            [appId, req.user.id, question, genResult.sql,
             JSON.stringify({ rows: genResult.rows.slice(0, 5), row_count: genResult.rows.length, source: 'qpd_seed', entity: entityName }),
             0.90, 'thumbs_up']
          );
          console.log(`    ✓ Seeded (${genResult.rows.length} rows, ${genResult.retryCount} retries)`);
          return { entityName, question, status: 'seeded', rows: genResult.rows.length, retries: genResult.retryCount };
        } else {
          const reason = genResult.execError || 'returned 0 rows';
          console.log(`    ✗ Failed: ${reason.substring(0, 100)}`);
          return { entityName, question, status: 'failed', reason, sql: genResult.sql };
        }
      }));

      // Tally batch results
      for (const settled of batchResults) {
        if (settled.status === 'fulfilled') {
          const r = settled.value;
          if (r.status === 'seeded') { results.seeded++; } else { results.failed++; }
          results.details.push(r);
        } else {
          results.failed++;
          const task = batch[batchResults.indexOf(settled)];
          results.details.push({ entity: task.entityName, question: task.question, status: 'error', reason: settled.reason?.message || 'Unknown error' });
          console.log(`    ✗ Error: ${(settled.reason?.message || '').substring(0, 100)}`);
        }
      }

      // Log token usage for the batch (aggregate)
      // Note: individual token tracking removed for parallel mode — tracked via the API-level token_usage table
    }

    console.log(`QPD Seeding complete: ${results.seeded} seeded, ${results.failed} failed out of ${results.total_questions} questions`);

    res.json({
      message: `QPD seeded: ${results.seeded} verified patterns from ${results.entities} business objects`,
      ...results,
    });
  } catch (err) {
    console.error('QPD seeding error:', err);
    res.status(500).json({ error: err.message || 'QPD seeding failed' });
  }
});

// GET /api/test/:appId/qpd-status — Check current QPD state
router.get('/:appId/qpd-status', async (req, res) => {
  try {
    const { appId } = req.params;
    const result = await query(
      `SELECT
         COUNT(*) FILTER (WHERE feedback = 'thumbs_up') as approved,
         COUNT(*) FILTER (WHERE feedback = 'thumbs_down') as rejected,
         COUNT(*) FILTER (WHERE feedback IS NULL) as pending,
         COUNT(*) FILTER (WHERE confidence = 0.90 AND feedback = 'thumbs_up') as system_seeded,
         COUNT(*) FILTER (WHERE confidence != 0.90 AND feedback = 'thumbs_up') as user_verified,
         COUNT(*) as total
       FROM test_queries WHERE app_id = $1`,
      [appId]
    );
    res.json({ qpd: result.rows[0] });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/test/:appId/query - legacy endpoint (kept for compatibility)
router.post('/:appId/query', async (req, res) => {
  try {
    const { appId } = req.params;
    const { nl_query } = req.body;
    if (!nl_query) return res.status(400).json({ error: 'Natural language query required' });
    // Redirect to nl2sql
    req.body.question = nl_query;
    return router.handle(req, res);
  } catch (err) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/test/:appId/query-history
router.get('/:appId/query-history', async (req, res) => {
  try {
    const { appId } = req.params;
    const result = await query(
      `SELECT id, nl_query, generated_sql, execution_result, confidence, feedback, created_at
       FROM test_queries WHERE app_id = $1 ORDER BY created_at DESC LIMIT 50`,
      [appId]
    );
    res.json({ queries: result.rows });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/test/:appId/query/:queryId/feedback
// Thumbs-up promotes a query into the QPD (few-shot pool).
// Thumbs-down demotes it: for system-seeded entries (confidence=0.90), this effectively
// removes them from the few-shot pool. For user queries, it records the negative signal.
// This creates the self-improvement loop: the QPD evolves based on real user feedback.
router.post('/:appId/query/:queryId/feedback', async (req, res) => {
  try {
    const { appId, queryId } = req.params;
    const { feedback } = req.body;
    if (!['thumbs_up', 'thumbs_down'].includes(feedback)) {
      return res.status(400).json({ error: 'Feedback must be thumbs_up or thumbs_down' });
    }
    const result = await query(
      'UPDATE test_queries SET feedback = $1 WHERE id = $2 AND app_id = $3 RETURNING *',
      [feedback, queryId, appId]
    );

    // For thumbs_down on system-seeded entries, log it so we know the QPD needs improvement
    if (feedback === 'thumbs_down' && result.rows[0]) {
      const entry = result.rows[0];
      const isSystemSeeded = parseFloat(entry.confidence) === 0.90;
      if (isSystemSeeded) {
        console.log(`QPD feedback: system-seeded query demoted — "${(entry.nl_query || '').substring(0, 60)}"`);
      }
    }

    res.json({ query: result.rows[0], message: `Feedback: ${feedback}` });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

// DELETE /api/test/:appId/query/:queryId — delete a saved query
router.delete('/:appId/query/:queryId', async (req, res) => {
  try {
    const { appId, queryId } = req.params;
    await query('DELETE FROM test_queries WHERE id = $1 AND app_id = $2', [queryId, appId]);
    res.json({ message: 'Query deleted' });
  } catch (err) {
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/test/:appId/import-qpd — Import sample questions as pre-seeded QPD
// Used to bootstrap QPD from prototype NL query patterns (OEBS, SAP)
router.post('/:appId/import-qpd', async (req, res) => {
  try {
    const { appId } = req.params;
    const { questions } = req.body;  // Array of { nl_query, sql_template?, objects? }
    if (!questions || !Array.isArray(questions) || questions.length === 0) {
      return res.status(400).json({ error: 'questions array required' });
    }

    let imported = 0;
    let skipped = 0;
    for (const q of questions) {
      if (!q.nl_query || typeof q.nl_query !== 'string') { skipped++; continue; }
      try {
        await query(
          `INSERT INTO test_queries (app_id, user_id, nl_query, generated_sql, execution_result, confidence, feedback, created_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
           ON CONFLICT DO NOTHING`,
          [appId, req.user.id, q.nl_query, q.sql_template || '',
           JSON.stringify({ source: 'prototype_import', objects: q.objects || [] }),
           0.85, 'thumbs_up']
        );
        imported++;
      } catch (e) {
        skipped++;
      }
    }

    res.json({ imported, skipped, total: questions.length });
  } catch (err) {
    console.error('Import QPD error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// ── TTS HELPER — generates base64 audio for inline responses ──
async function generateTTSBase64(text, voice = 'nova') {
  const OPENAI_KEY = (process.env.OPENAI_API_KEY || '').trim();
  if (!OPENAI_KEY || !text) return null;
  try {
    const allowedVoices = ['alloy', 'echo', 'fable', 'onyx', 'nova', 'shimmer'];
    const safeVoice = allowedVoices.includes(voice) ? voice : 'nova';
    const response = await fetch('https://api.openai.com/v1/audio/speech', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${OPENAI_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: 'tts-1', input: text, voice: safeVoice, response_format: 'mp3', speed: 1.0 }),
    });
    if (!response.ok) return null;
    const arrayBuffer = await response.arrayBuffer();
    return Buffer.from(arrayBuffer).toString('base64');
  } catch (e) {
    console.warn('[TTS/inline] Error:', e.message);
    return null;
  }
}

// TEXT-TO-SPEECH ENDPOINT — uses OpenAI TTS for high-quality voice synthesis
// Returns audio/mpeg stream that the client plays directly
// ─────────────────────────────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
// SPEECH-TO-TEXT (STT) via OpenAI gpt-4o-transcribe
// Accepts audio blob (webm/opus from MediaRecorder), returns transcription.
// ─────────────────────────────────────────────────────────────────────────────
router.post('/transcribe', audioUpload.single('audio'), async (req, res) => {
  try {
    const OPENAI_KEY = (process.env.OPENAI_API_KEY || '').trim();
    if (!OPENAI_KEY) {
      return res.status(501).json({ error: 'STT not configured (no OPENAI_API_KEY)' });
    }
    if (!req.file || !req.file.buffer || req.file.buffer.length === 0) {
      return res.status(400).json({ error: 'No audio data received' });
    }

    const audioSize = req.file.buffer.length;
    console.log(`[STT] Received ${audioSize} bytes, mime: ${req.file.mimetype}`);

    // Build multipart form for OpenAI Whisper / gpt-4o-transcribe
    const FormData = (await import('form-data')).default;
    const form = new FormData();

    // Determine file extension from mime type
    const mimeToExt = {
      'audio/webm': 'webm',
      'audio/webm;codecs=opus': 'webm',
      'audio/ogg': 'ogg',
      'audio/ogg;codecs=opus': 'ogg',
      'audio/mp4': 'mp4',
      'audio/mpeg': 'mp3',
      'audio/wav': 'wav',
    };
    const ext = mimeToExt[req.file.mimetype] || 'webm';

    form.append('file', req.file.buffer, {
      filename: `audio.${ext}`,
      contentType: req.file.mimetype || 'audio/webm',
    });
    form.append('model', 'gpt-4o-transcribe');
    form.append('language', 'en');
    form.append('temperature', '0');  // Reduce hallucination — deterministic transcription
    // Prompt helps with business terminology accuracy — include common conversational phrases
    // so the model recognizes "include both bookings and revenue" instead of hallucinating
    form.append('prompt', 'Voice conversation about business data. Common phrases: "include both bookings and revenue", "also include cash collections", "for the quarter", "month to date", "year over year", "break it down by". Terms: bookings, revenue, collections, cash collections, AR aging, AP aging, GL, DSO, pipeline, forecast, sales rep, quarter, invoices, receivables, payables, overdue.');

    const response = await fetch('https://api.openai.com/v1/audio/transcriptions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OPENAI_KEY}`,
        ...form.getHeaders(),
      },
      body: form.getBuffer(),
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error('[STT] OpenAI error:', response.status, errText);
      return res.status(502).json({ error: `Transcription failed: ${response.status}`, detail: errText });
    }

    const result = await response.json();
    let transcript = (result.text || '').trim();

    // Guard: gpt-4o-transcribe sometimes echoes the prompt hint back when it hears
    // silence or ambient noise. Detect and discard these phantom transcriptions.
    const PROMPT_ECHO = 'voice conversation about business data';
    if (transcript.toLowerCase().replace(/[.,]/g, '').includes(PROMPT_ECHO)) {
      console.log(`[STT] Discarding prompt echo: "${transcript.substring(0, 60)}"`);
      transcript = '';
    }

    // Post-transcription corrections for known gpt-4o-transcribe hallucinations
    // "Include both bookings and" is commonly garbled as "Kingsland", "Kings land", etc.
    transcript = transcript
      .replace(/\bKingsland\b/gi, 'Include both bookings and')
      .replace(/\bKings land\b/gi, 'Include both bookings and')
      .replace(/\bKingston\b/gi, 'Include bookings and')   // another common hallucination
      .replace(/\bKings\s+and\b/gi, 'Include bookings and');  // partial hallucination

    console.log(`[STT] Transcribed ${audioSize} bytes → "${transcript.substring(0, 80)}${transcript.length > 80 ? '...' : ''}"`);

    res.json({ text: transcript, duration: result.duration || null });
  } catch (err) {
    console.error('[STT] Error:', err.message);
    res.status(500).json({ error: 'Transcription failed: ' + err.message });
  }
});

router.post('/tts', async (req, res) => {
  try {
    const OPENAI_KEY = (process.env.OPENAI_API_KEY || '').trim();
    if (!OPENAI_KEY) {
      return res.status(501).json({ error: 'TTS not configured (no OPENAI_API_KEY)' });
    }
    // Log key prefix for debugging (safe — only shows first 8 chars)
    console.log(`[TTS] Using key: ${OPENAI_KEY.substring(0, 8)}... (${OPENAI_KEY.length} chars)`);

    const { text, voice = 'nova' } = req.body;
    if (!text || text.length === 0) {
      return res.status(400).json({ error: 'text is required' });
    }

    // Allowed voices: alloy, echo, fable, onyx, nova, shimmer
    const allowedVoices = ['alloy', 'echo', 'fable', 'onyx', 'nova', 'shimmer'];
    const safeVoice = allowedVoices.includes(voice) ? voice : 'nova';

    const response = await fetch('https://api.openai.com/v1/audio/speech', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OPENAI_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'tts-1',   // tts-1 = optimized for real-time (~2x faster than tts-1-hd)
        input: text,
        voice: safeVoice,
        response_format: 'mp3',
        speed: 1.0,
      }),
    });

    if (!response.ok) {
      const errText = await response.text();
      console.error('[TTS] OpenAI error:', response.status, errText);
      return res.status(502).json({ error: `TTS generation failed: ${response.status}` });
    }

    // Stream audio back to client
    res.set({
      'Content-Type': 'audio/mpeg',
      'Cache-Control': 'no-cache',
    });

    // Pipe the response body directly
    const arrayBuffer = await response.arrayBuffer();
    res.send(Buffer.from(arrayBuffer));

    console.log(`[TTS] Generated ${text.length} chars with voice "${safeVoice}"`);
  } catch (err) {
    console.error('[TTS] Error:', err.message);
    res.status(500).json({ error: 'TTS generation failed' });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// DEMO DATA: Boost Q1 2026 revenue to be realistic relative to bookings
// One-time idempotent endpoint — inserts invoice records into appdata schema
// ─────────────────────────────────────────────────────────────────────────────
router.post('/:appId/demo/boost-revenue', async (req, res) => {
  const appId = req.params.appId;
  try {
    const s = `appdata_${appId}`;  // schema name
    const { force } = req.body || {};

    // Check if already boosted (allow force re-run)
    const check = await query(
      `SELECT COUNT(*) as cnt FROM ${s}."RA_CUSTOMER_TRX_ALL" WHERE "COMMENTS" = 'DEMO_BOOST_Q1_2026'`
    );
    if (check.rows[0].cnt > 0 && !force) {
      return res.json({ status: 'already_boosted', message: `Revenue already boosted (${check.rows[0].cnt} invoices exist). Pass {"force": true} to re-run.` });
    }
    // Clean up any previous boost data
    if (check.rows[0].cnt > 0) {
      await query(`DELETE FROM ${s}."RA_CUSTOMER_TRX_LINES_ALL" WHERE "CREATED_BY" = 'DEMO_BOOST'`);
      await query(`DELETE FROM ${s}."RA_CUSTOMER_TRX_ALL" WHERE "COMMENTS" = 'DEMO_BOOST_Q1_2026'`);
      console.log('[Demo] Cleaned up previous boost data');
    }

    // Get max IDs from existing data
    const maxTrx = await query(`SELECT COALESCE(MAX("CUSTOMER_TRX_ID"), 0) as m FROM ${s}."RA_CUSTOMER_TRX_ALL"`);
    const maxLine = await query(`SELECT COALESCE(MAX("CUSTOMER_TRX_LINE_ID"), 0) as m FROM ${s}."RA_CUSTOMER_TRX_LINES_ALL"`);
    let nextTrxId = maxTrx.rows[0].m + 1;
    let nextLineId = maxLine.rows[0].m + 1;

    // Get existing customer IDs and batch sources
    const custResult = await query(`SELECT DISTINCT "BILL_TO_CUSTOMER_ID" FROM ${s}."RA_CUSTOMER_TRX_ALL" WHERE "BILL_TO_CUSTOMER_ID" IS NOT NULL LIMIT 20`);
    const customers = custResult.rows.map(r => r.BILL_TO_CUSTOMER_ID);
    const batchSources = [1001, 1002, 1003, 1004, 1005];

    // Invoice plan: ~25 invoices across Jan-Mar 2026, each with 2-4 lines
    // Target: add ~$55M to bring Q1 revenue from $6M to ~$61M
    const invoicePlan = [
      // January: 9 invoices, ~$22M
      { date: '2026-01-05', lines: [{amt: 850000}, {amt: 720000}, {amt: 430000}] },
      { date: '2026-01-08', lines: [{amt: 1250000}, {amt: 980000}] },
      { date: '2026-01-12', lines: [{amt: 640000}, {amt: 510000}, {amt: 380000}] },
      { date: '2026-01-15', lines: [{amt: 1100000}, {amt: 750000}] },
      { date: '2026-01-19', lines: [{amt: 920000}, {amt: 680000}, {amt: 450000}] },
      { date: '2026-01-22', lines: [{amt: 1350000}, {amt: 890000}] },
      { date: '2026-01-25', lines: [{amt: 780000}, {amt: 560000}] },
      { date: '2026-01-28', lines: [{amt: 1050000}, {amt: 720000}, {amt: 490000}] },
      { date: '2026-01-31', lines: [{amt: 960000}, {amt: 640000}] },
      // February: 9 invoices, ~$18M
      { date: '2026-02-03', lines: [{amt: 750000}, {amt: 520000}, {amt: 380000}] },
      { date: '2026-02-06', lines: [{amt: 1100000}, {amt: 680000}] },
      { date: '2026-02-10', lines: [{amt: 890000}, {amt: 610000}] },
      { date: '2026-02-13', lines: [{amt: 720000}, {amt: 480000}, {amt: 350000}] },
      { date: '2026-02-17', lines: [{amt: 950000}, {amt: 710000}] },
      { date: '2026-02-20', lines: [{amt: 830000}, {amt: 560000}] },
      { date: '2026-02-24', lines: [{amt: 1050000}, {amt: 690000}] },
      { date: '2026-02-26', lines: [{amt: 780000}, {amt: 450000}] },
      { date: '2026-02-28', lines: [{amt: 920000}, {amt: 580000}] },
      // March: 7 invoices, ~$15M
      { date: '2026-03-03', lines: [{amt: 850000}, {amt: 640000}] },
      { date: '2026-03-07', lines: [{amt: 1150000}, {amt: 780000}] },
      { date: '2026-03-11', lines: [{amt: 720000}, {amt: 510000}, {amt: 390000}] },
      { date: '2026-03-14', lines: [{amt: 980000}, {amt: 650000}] },
      { date: '2026-03-18', lines: [{amt: 860000}, {amt: 590000}] },
      { date: '2026-03-21', lines: [{amt: 1100000}, {amt: 730000}] },
      { date: '2026-03-25', lines: [{amt: 940000}, {amt: 620000}] },
      // Additional large invoices to bring Q1 total to ~$61M
      { date: '2026-01-10', lines: [{amt: 2150000}, {amt: 1850000}] },
      { date: '2026-01-20', lines: [{amt: 1950000}, {amt: 1450000}] },
      { date: '2026-02-08', lines: [{amt: 1750000}, {amt: 1350000}] },
      { date: '2026-02-22', lines: [{amt: 1650000}, {amt: 1250000}] },
    ];

    let totalInvoices = 0;
    let totalLines = 0;
    let totalRevenue = 0;

    for (const inv of invoicePlan) {
      const custId = customers[totalInvoices % customers.length];
      const batchSrc = batchSources[totalInvoices % batchSources.length];
      const trxId = nextTrxId++;
      const trxNum = `INV-${trxId}`;

      // Insert invoice header — use raw SQL with interpolated values to avoid PG type deduction issues
      await query(
        `INSERT INTO ${s}."RA_CUSTOMER_TRX_ALL"
         ("CUSTOMER_TRX_ID", "TRX_NUMBER", "TRX_DATE", "CUST_TRX_TYPE_ID", "BILL_TO_CUSTOMER_ID",
          "BATCH_SOURCE_ID", "INVOICE_CURRENCY_CODE", "COMPLETE_FLAG", "STATUS_TRX", "ORG_ID",
          "CREATION_DATE", "CREATED_BY", "COMMENTS")
         VALUES ('${trxId}', '${trxNum}', '${inv.date}', '1001', '${custId}', '${batchSrc}', 'USD', 'Y', 'CL', '101', '${inv.date}', 'DEMO_BOOST', 'DEMO_BOOST_Q1_2026')`
      );

      // Insert invoice lines
      let lineNum = 0;
      for (const line of inv.lines) {
        lineNum++;
        const lineId = nextLineId++;
        const qty = Math.floor(line.amt / 500) || 1; // Derive qty from amount
        const unitPrice = Math.round(line.amt / qty * 100) / 100;
        await query(
          `INSERT INTO ${s}."RA_CUSTOMER_TRX_LINES_ALL"
           ("CUSTOMER_TRX_LINE_ID", "CUSTOMER_TRX_ID", "LINE_NUMBER", "LINE_TYPE", "DESCRIPTION",
            "QUANTITY_INVOICED", "QUANTITY_ORDERED", "UNIT_SELLING_PRICE", "EXTENDED_AMOUNT", "REVENUE_AMOUNT",
            "UOM_CODE", "CREATION_DATE", "CREATED_BY")
           VALUES ('${lineId}', '${trxId}', '${lineNum}', 'LINE', 'Product line ${lineNum}', '${qty}', '${qty}', '${unitPrice}', '${line.amt}', '${line.amt}', 'EA', '${inv.date}', 'DEMO_BOOST')`
        );
        totalRevenue += line.amt;
        totalLines++;
      }
      totalInvoices++;
    }

    // Verify new Q1 total
    const newTotal = await query(
      `SELECT SUM(l."EXTENDED_AMOUNT") as total
       FROM ${s}."RA_CUSTOMER_TRX_LINES_ALL" l
       JOIN ${s}."RA_CUSTOMER_TRX_ALL" h ON l."CUSTOMER_TRX_ID" = h."CUSTOMER_TRX_ID"
       WHERE h."TRX_DATE" >= '2026-01-01' AND h."TRX_DATE" < '2026-04-01'`
    );

    res.json({
      status: 'boosted',
      invoices_added: totalInvoices,
      lines_added: totalLines,
      revenue_added: totalRevenue,
      q1_2026_total: parseFloat(newTotal.rows[0].total),
    });
    console.log(`[Demo] Revenue boosted: +${totalInvoices} invoices, +${totalLines} lines, +$${(totalRevenue/1e6).toFixed(1)}M. Q1 total: $${(parseFloat(newTotal.rows[0].total)/1e6).toFixed(1)}M`);
  } catch (err) {
    console.error('[Demo] Boost revenue error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Export router + reusable functions for pipeline integration
module.exports = router;
module.exports.router = router;
module.exports.buildBOKGContext = buildBOKGContext;
module.exports.loadValueDictionaries = loadValueDictionaries;
module.exports.generateAndExecuteWithRetry = generateAndExecuteWithRetry;
module.exports.generateWithSelfConsistency = generateWithSelfConsistency;
module.exports.classifyIntentRich = classifyIntent;
module.exports.schemaLink = schemaLink;
module.exports.getFewShotExamples = getFewShotExamples;
module.exports.getContextDocumentSection = getContextDocumentSection;
module.exports.buildColumnBusinessNames = buildColumnBusinessNames;
