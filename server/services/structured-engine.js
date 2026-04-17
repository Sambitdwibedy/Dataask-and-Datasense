/**
 * Structured Engine — NL2SQL against the BOKG Knowledge Graph
 *
 * Reads from app_tables, app_columns, app_relationships (created by BOKG Builder)
 * to generate and execute SQL from natural language questions.
 *
 * Column names match the actual BOKG Builder schema:
 *   app_tables: entity_name (not business_name), entity_metadata, enrichment_status
 *   app_columns: business_name, is_pk, is_fk (not is_primary_key/is_foreign_key)
 *   app_relationships: from_table_id, from_column, to_table_id, to_column (not source_/target_)
 */
const { pool, query } = require('../db');

// Cost estimates: Sonnet input=?/MTok, output=?/MTok
const COST_PER_INPUT_TOKEN = 3 / 1000000;
const COST_PER_OUTPUT_TOKEN = 15 / 1000000;
const MAX_RETRIES = 2;
const DEFAULT_MODEL = 'claude-sonnet-4-20250514';

// ─── Schema Helpers ───

/**
 * Load the full BOKG schema context for an application.
 * Returns { tables, columns, relationships, contextDocs }.
 */
async function loadSchemaContext(appId) {
  // Tables with enrichment — entity_name is the business-friendly name in app_tables
  const [tablesRows] = await query(
    `SELECT id, table_name, entity_name, description, row_count, entity_metadata
     FROM app_tables WHERE app_id = ? AND enrichment_status IN ('approved', 'ai_enriched')
     ORDER BY table_name`,
    [appId]
  );

  // Columns with enrichment — is_pk/is_fk are the actual column names
  const [columnsRows] = await query(
    `SELECT ac.id, ac.table_id, ac.column_name, ac.business_name, ac.description,
            ac.data_type, ac.is_pk, ac.is_fk,
            ac.value_mapping, at.table_name
     FROM app_columns ac
     JOIN app_tables at ON ac.table_id = at.id
     WHERE at.app_id = ? AND at.enrichment_status IN ('approved', 'ai_enriched')
     ORDER BY at.table_name, ac.column_name`,
    [appId]
  );

  // Relationships — uses from_table_id/to_table_id (integer FKs), not source_table/target_table (names)
  const [relsRows] = await query(
    `SELECT ar.id,
            ft.table_name as from_table, ar.from_column,
            tt.table_name as to_table, ar.to_column,
            ar.rel_type, ar.confidence_score
     FROM app_relationships ar
     JOIN app_tables ft ON ar.from_table_id = ft.id
     JOIN app_tables tt ON ar.to_table_id = tt.id
     WHERE ar.app_id = ? AND (ar.enrichment_status = 'approved' OR ar.enrichment_status = 'ai_enriched')
     ORDER BY ar.confidence_score DESC`,
    [appId]
  );

  // Context documents (BOKG Builder's context-assisted build docs) — may not exist
  let contextDocs = [];
  try {
    const [contextRows] = await query(
      `SELECT filename, extracted_text, description
       FROM context_documents WHERE app_id = ?`,
      [appId]
    );
    contextDocs = contextRows;
  } catch (e) {
    // Table may not exist yet — that's fine
  }

  return {
    tables: tablesRows,
    columns: columnsRows,
    relationships: relsRows,
    contextDocs,
  };
}

/**
 * Schema linking: identify the most relevant tables for a question.
 */
// Stopwords to exclude from keyword matching — common English words that add noise
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
  'number', 'total', 'average', 'count', 'percentage', 'among', 'per',
  'there', 'their', 'them', 'they', 'what', 'where', 'when', 'here',
]);

function schemaLink(question, tables, columns, relationships, semanticMap = {}) {
  const qLower = question.toLowerCase();
  // Filter out stopwords and short words for keyword matching
  const qWords = qLower.split(/\s+/).filter(w => w.length > 2 && !SCHEMA_LINK_STOPWORDS.has(w));

  // Build graph centrality map: count incoming FK references per table.
  // Parent/driving tables (orders, gl_account) naturally have more children pointing to them,
  // making this a better tiebreaker than row count (which favors detail tables like order_lines).
  const incomingFKCount = {};
  for (const rel of (relationships || [])) {
    const toId = rel.to_table_id || rel.to_id;
    if (toId) incomingFKCount[toId] = (incomingFKCount[toId] || 0) + 1;
  }

  const scored = tables.map(table => {
    let score = 0;
    const tName = (table.table_name || '').toLowerCase();
    const eName = (table.entity_name || '').toLowerCase();  // entity_name is the business name for tables
    const desc = (table.description || '').toLowerCase();
    const tableWords = tName.split('_').filter(w => w.length > 2);

    // Direct table name mention
    if (qLower.includes(tName.replace(/_/g, ' ')) || qLower.includes(tName)) score += 10;
    // Entity/business name mention
    if (eName && qLower.includes(eName)) score += 15;

    // Keyword overlap with description and entity name
    for (const word of qWords) {
      if (desc.includes(word)) score += 2;
      if (eName.includes(word)) score += 3;
      if (tName.includes(word)) score += 3;

      // Fuzzy match: handle plurals/stems — "orders" matches "order", "categories" matches "category"
      const wordStem = word.length > 4 ? word.substring(0, word.length - 1) : word;
      for (const tw of tableWords) {
        const twStem = tw.length > 4 ? tw.substring(0, tw.length - 1) : tw;
        if (tw.startsWith(wordStem) || word.startsWith(twStem)) {
          score += 2;
          break;
        }
      }
    }

    // Check column matches
    const tableCols = columns.filter(c => c.table_id === table.id);
    for (const col of tableCols) {
      const cName = (col.column_name || '').toLowerCase();
      const cbName = (col.business_name || '').toLowerCase();
      if (qLower.includes(cName.replace(/_/g, ' '))) score += 5;
      if (cbName && qLower.includes(cbName)) score += 7;
    }

    // Domain/module match from entity_metadata
    const meta = typeof table.entity_metadata === 'string'
      ? JSON.parse(table.entity_metadata || '{}')
      : (table.entity_metadata || {});
    const domain = (meta.domain || meta.module || '').toLowerCase();
    if (domain && qLower.includes(domain)) score += 8;

    // Sample questions match — these are curated business questions that express
    // what each table can answer (e.g. "total bookings" → OE_ORDER_HEADERS_ALL).
    // This is critical for matching business terms not in table/column names.
    const sampleQuestions = meta.sample_questions || [];
    for (const sq of sampleQuestions) {
      const sqLower = sq.toLowerCase();
      // Strong match: query words appear in a sample question
      let sqMatchCount = 0;
      for (const word of qWords) {
        if (sqLower.includes(word)) sqMatchCount++;
      }
      if (sqMatchCount >= 2) {
        score += 5 + sqMatchCount; // 7+ points for strong semantic match
        break; // one strong match is enough
      } else if (sqMatchCount === 1) {
        score += 2; // weak single-word overlap
      }
    }

    // Graph centrality tiebreaker: parent/driving tables have more incoming FK references.
    // This scales to any schema size without requiring row counts (no COUNT(*) needed).
    const centrality = incomingFKCount[table.id] || 0;
    if (score > 0 && centrality > 0) {
      score += Math.min(centrality, 3); // max +3 points for highly-referenced tables
    }

    // Semantic similarity boost: vector embeddings catch vocabulary gaps that keywords miss.
    const similarity = semanticMap[table.id] || 0;
    if (similarity > 0) {
      const semanticBoost = Math.round(similarity * 16); // 0.25→4, 0.35→6, 0.50→8
      score += semanticBoost;
    }

    return { ...table, relevanceScore: score, _centrality: centrality, _similarity: similarity };
  });

  // Return tables with score > 0, sorted by relevance then similarity then centrality
  return scored
    .filter(t => t.relevanceScore > 0)
    .sort((a, b) => b.relevanceScore - a.relevanceScore || b._similarity - a._similarity || b._centrality - a._centrality);
}

// ─── Value Dictionary Post-Processing ───

async function loadValueDictionaries(appId) {
  const [rows] = await query(
    `SELECT ac.column_name, ac.value_mapping, at.table_name
     FROM app_columns ac
     JOIN app_tables at ON ac.table_id = at.id
     WHERE at.app_id = ? AND ac.value_mapping IS NOT NULL`,
    [appId]
  );

  const dictionaries = {};
  for (const row of rows) {
    try {
      const vm = typeof row.value_mapping === 'string' ? JSON.parse(row.value_mapping) : row.value_mapping;
      if (vm && typeof vm === 'object' && Object.keys(vm).length > 0) {
        const key = `${row.table_name}.${row.column_name}`;
        dictionaries[key] = {};
        for (const val of Object.keys(vm)) {
          dictionaries[key][String(val).toLowerCase()] = String(val);
        }
        dictionaries[row.column_name] = dictionaries[row.column_name] || {};
        Object.assign(dictionaries[row.column_name], dictionaries[key]);
      }
    } catch (e) { /* skip invalid */ }
  }
  return dictionaries;
}

function fixSQLCasing(sql, dictionaries) {
  let fixed = sql;
  // Find string literals in WHERE clauses
  const stringLiterals = sql.match(/'([^']+)'/g) || [];
  for (const literal of stringLiterals) {
    const value = literal.slice(1, -1);
    const lowerValue = value.toLowerCase();
    // Check all dictionaries for a case match
    for (const [, dict] of Object.entries(dictionaries)) {
      if (dict[lowerValue] && dict[lowerValue] !== value) {
        fixed = fixed.replace(literal, `'${dict[lowerValue]}'`);
        break;
      }
    }
  }
  return fixed;
}

/**
 * Fix date function calls on TEXT columns by adding  cast.
 * The DDL may say DATE but the actual PostgreSQL column might be TEXT.
 * This adds  casts to EXTRACT, DATE_TRUNC, and date arithmetic patterns.
 */
function fixDateCasts(sql) {
  let fixed = sql;
  // EXTRACT(YEAR FROM t1."date") → EXTRACT(YEAR FROM t1."date")
  // Match column ref inside EXTRACT that is NOT already cast
  fixed = fixed.replace(
    /EXTRACT\s*\(\s*(\w+)\s+FROM\s+([\w.]*"[^"]+")(?!::)\s*\)/gi,
    'EXTRACT(? FROM ?)'
  );
  // DATE_TRUNC('year', t1."date") → DATE_TRUNC('year', t1."date")
  fixed = fixed.replace(
    /DATE_TRUNC\s*\(\s*('[^']+'),\s*([\w.]*"[^"]+")(?!::)\s*\)/gi,
    'DATE_TRUNC(?, ?)'
  );
  return fixed;
}

// ─── SQL Extraction ───

function extractSQL(rawText) {
  let text = (rawText || '').trim();

  const sqlSectionMatch = text.match(/###\s*SQL\s*\n([\s\S]*?)(?=\n###|\n\*\*|$)/i);
  if (sqlSectionMatch) text = sqlSectionMatch[1].trim();

  const codeBlockMatch = text.match(/```(?:sql|postgresql|pg)?\s*\n?([\s\S]*?)```/i);
  if (codeBlockMatch) text = codeBlockMatch[1].trim();

  text = text.replace(/^(?:here\s+is\s+)?(?:the\s+)?(?:sql|query)\s*:\s*/i, '').trim();

  const selectMatch = text.match(/((?:WITH|SELECT)\s[\s\S]*?)(;?\s*$)/i);
  if (selectMatch) {
    text = selectMatch[1].trim().replace(/;\s*$/, '').trim();
  }

  text = text.replace(/```/g, '').trim();
  return text;
}

// ─── Few-Shot Examples ───

async function loadFewShotExamples(appId, question) {
  let result;
  try {
    result = await query(
      `SELECT nl_query, generated_sql FROM test_queries
       WHERE app_id = ? AND feedback = 'thumbs_up' AND generated_sql IS NOT NULL
       ORDER BY created_at DESC LIMIT 50`,
      [appId]
    );
  } catch (e) {
    // test_queries table may not exist yet
    return [];
  }

  if (rows.length === 0) return [];

  // Simple keyword relevance scoring
  const qWords = question.toLowerCase().split(/\s+/).filter(w => w.length > 3);
  const scored = rows.map(row => {
    const ql = row.nl_query.toLowerCase();
    let score = 0;
    for (const word of qWords) {
      if (ql.includes(word)) score++;
    }
    return { ...row, score };
  });

  return scored
    .filter(r => r.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 5);
}

// ─── NL2SQL Generation ───

/**
 * Full NL2SQL pipeline: schema link → build prompt → generate SQL → execute.
 * Returns { sql, results, explanation, confidence, tokenUsage }.
 */
async function queryStructuredData(question, { appId, userId, conversationHistory = [], debugPrompt = false } = {}) {
  const Anthropic = require('@anthropic-ai/sdk');
  const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

  // 1. Load schema context
  const schema = await loadSchemaContext(appId);
  if (schema.tables.length === 0) {
    return {
      answer: 'No approved knowledge graph is available for this application yet. The BOKG Builder admin needs to complete the build pipeline and publish the knowledge graph.',
      sql: null,
      results: null,
      confidence: 'low',
      tokenUsage: null,
    };
  }

  // 2. Schema linking (keyword + semantic + graph centrality)
  // Fetch semantic matches from pgvector in parallel with keyword scoring
  let semanticMap = {};
  try {
    const { semanticSchemaLink } = require('./embedding-service');
    const semanticMatches = await semanticSchemaLink(appId, question, 15, 0.25);
    for (const m of semanticMatches) semanticMap[m.table_id] = m.similarity;
  } catch (e) { /* embeddings not available — keyword-only fallback */ }

  const linkedTables = schemaLink(question, schema.tables, schema.columns, schema.relationships, semanticMap);
  const relevantTables = linkedTables.length > 0 ? linkedTables.slice(0, 15) : schema.tables.slice(0, 10);
  const relevantTableIds = new Set(relevantTables.map(t => t.id));

  // 3. Build schema DDL for prompt (with alias hints)
  const schemaDDL = relevantTables.map((table, idx) => {
    const alias = `t${idx + 1}`;
    const cols = schema.columns
      .filter(c => c.table_id === table.id)
      .map(c => {
        let colDef = `  "${c.column_name}" ${c.data_type}`;
        if (c.is_pk) colDef += ' PRIMARY KEY';
        const bName = c.business_name ? ` -- ${c.business_name}` : '';
        const desc = c.description ? ` | ${c.description}` : '';
        return colDef + bName + desc;
      })
      .join(',\n');

    // entity_name is the business-friendly name for tables; include row_count so LLM knows which tables have data
    const rc = parseInt(table.row_count) || 0;
    const rowInfo = rc > 0 ? ` (${rc.toLocaleString()} rows)` : '';
    const descHint = table.description ? ` | ${table.description.substring(0, 120)}` : '';
    const tableName = table.entity_name
      ? `"${table.table_name}" ${alias} -- ${table.entity_name}${rowInfo}${descHint}`
      : `"${table.table_name}" ${alias}${rowInfo}${descHint}`;

    return `CREATE TABLE ${tableName} (\n${cols}\n);`;
  }).join('\n\n');

  // 4. Relationships for prompt — uses from_table/to_table (resolved via JOINs in loadSchemaContext)
  const relevantRels = schema.relationships.filter(r => {
    const sourceTable = schema.tables.find(t => t.table_name === r.from_table);
    const targetTable = schema.tables.find(t => t.table_name === r.to_table);
    return (sourceTable && relevantTableIds.has(sourceTable.id)) ||
           (targetTable && relevantTableIds.has(targetTable.id));
  });

  const relText = relevantRels.map(r =>
    `${r.from_table}.${r.from_column} → ${r.to_table}.${r.to_column} (${r.rel_type})`
  ).join('\n');

  // 5. Few-shot examples
  const examples = await loadFewShotExamples(appId, question);
  const examplesText = examples.length > 0
    ? 'Verified examples:\n' + examples.map(e => `Q: ${e.nl_query}\nSQL: ${e.generated_sql}`).join('\n\n')
    : '';

  // 6. Context documents
  const contextText = schema.contextDocs.length > 0
    ? 'Reference context:\n' + schema.contextDocs.map(d => `[${d.filename}] ${d.extracted_text?.substring(0, 2000) || ''}`).join('\n---\n')
    : '';

  // 7. Build prompt
  const systemPrompt = `You are Data Ask, an expert SQL generator for enterprise applications.
Generate PostgreSQL queries from natural language questions using the provided schema.

CRITICAL RULES:
1. Use ONLY the exact table names and column names provided in the schema below. NEVER invent, guess, or simplify table names. If the schema says "invoice_item" you must use "invoice_item", not "invoice" or "invoices".
2. Always double-quote all table and column names (PostgreSQL standard).
3. Always assign short aliases to every table (e.g. t1, t2) and ALWAYS qualify every column reference with its table alias. Example: SELECT t1."column_name" FROM "my_table" t1.
4. Use JOIN conditions based on the provided relationships. Always qualify both sides with table aliases.
5. LIMIT results to 100 rows unless the user specifies otherwise.
6. For aggregations, include meaningful GROUP BY and ORDER BY clauses — all column references must use table aliases.
7. Use the business names and descriptions to understand what each table/column represents.
8. In SELECT, WHERE, GROUP BY, ORDER BY, and HAVING — every column MUST be prefixed with its table alias. No exceptions.
9. Return ONLY the SQL query — no explanation, no markdown fences.
10. ALWAYS generate a real SQL query using the provided tables. Only return 'No matching tables found' if NONE of the provided tables are even remotely relevant to the question. If a table's name or description matches a concept in the question, USE it.
11. IMPORTANT: When joining a header/parent table to a detail/line/child table (e.g. invoices → invoice_lines, orders → order_lines), and the user is asking about header-level data, use SELECT DISTINCT on the header columns OR use a subquery/aggregation to avoid duplicate rows caused by the one-to-many join fan-out.
12. JOIN rules: ONLY join tables using the provided Relationships section. If no relationship connects two tables, do NOT join them. Never use ON 1=1 or cross joins. If the user's question requires data from tables that cannot be joined via known relationships, query the tables separately or use only the tables that CAN be joined.
13. When the user asks about a concept like "account type" or "loan type", look for the closest matching column in the schema (e.g., "frequency", "status", "type"). Use column descriptions and business names to make the best match.
14. Date handling: ALWAYS cast date columns using  or  before date functions like EXTRACT, DATE_TRUNC, or date arithmetic. Example: EXTRACT(YEAR FROM t1."date"). This is required because some date columns may be stored as TEXT in the underlying database even if typed as DATE in the schema. Safe to do on actual DATE columns too (no-op cast).`;

  const userMessage = `Schema:
${schemaDDL}

Relationships:
${relText || 'None identified'}

${examplesText}

${contextText}

Question: ${question}

Generate the SQL query:`;

  // Debug mode: return the prompt without calling LLM
  if (debugPrompt) {
    return {
      sql: null, results: null, confidence: 'debug',
      _debug: {
        linkedTables: linkedTables.slice(0, 10).map(t => ({ name: t.table_name, score: t.relevanceScore, rows: t.row_count })),
        relevantTableCount: relevantTables.length,
        schemaDDL: schemaDDL.substring(0, 8000),
        relationships: relText.substring(0, 2000),
        systemPrompt: systemPrompt.substring(0, 1500),
      },
    };
  }

  // 8. Generate SQL with retry
  let lastError = null;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      const messages = [];
      // Include last few conversation turns for context
      for (const msg of conversationHistory.slice(-4)) {
        messages.push({ role: msg.role, content: msg.content });
      }

      let retryHint = '';
      if (attempt > 0) {
        retryHint = `\n\nPrevious SQL failed with error: ${lastError}\nPlease fix the query.`;
        if (lastError.includes('extract') && lastError.includes('text')) {
          retryHint += '\nHINT: The date column is stored as TEXT. Use  cast: EXTRACT(YEAR FROM column)';
        }
      }
      const promptWithRetry = attempt > 0
        ? `${userMessage}${retryHint}`
        : userMessage;

      messages.push({ role: 'user', content: promptWithRetry });

      const response = await client.messages.create({
        model: DEFAULT_MODEL,
        max_tokens: 4096,
        system: systemPrompt,
        messages,
      });

      let sql = extractSQL(response.content[0].text);

      // Value dictionary case fix
      const dictionaries = await loadValueDictionaries(appId);
      sql = fixSQLCasing(sql, dictionaries);

      // Fix date casts — ensure EXTRACT/DATE_TRUNC on text date columns work
      sql = fixDateCasts(sql);

      // Execute against the application's source data database (appdata_{appId})
      const dbName = `appdata_${appId}`;
      const dbClient = await pool.getConnection();
      let execRows = [];
      let execFields = [];
      try {
        await dbClient.execute(`USE \`${dbName}\``);
        await dbClient.execute('SET SESSION max_execution_time = 60000'); // 60s timeout
        const [rows, fields] = await dbClient.execute(sql);
        execRows = Array.isArray(rows) ? rows : [];
        execFields = fields ? fields.map(f => f.name) : [];
      } finally {
        dbClient.release();
      }

      // Determine confidence based on schema link strength
      const topScore = linkedTables.length > 0 ? linkedTables[0].relevanceScore : 0;
      let confidence = 'low';
      if (topScore >= 15 && examples.length > 0) confidence = 'high';
      else if (topScore >= 8) confidence = 'medium';

      return {
        answer: null,  // Will be filled by the ask route with a natural language summary
        sql,
        results: {
          rows: execRows.slice(0, 100),
          rowCount: execRows.length,
          fields: execFields,
        },
        confidence,
        tablesUsed: relevantTables.map(t => t.table_name),
        tokenUsage: {
          input: response.usage.input_tokens,
          output: response.usage.output_tokens,
          model: DEFAULT_MODEL,
          cost: (response.usage.input_tokens * COST_PER_INPUT_TOKEN +
                 response.usage.output_tokens * COST_PER_OUTPUT_TOKEN).toFixed(6),
        },
      };
    } catch (err) {
      lastError = err.message;
      if (attempt === MAX_RETRIES) {
        return {
          answer: `I was able to understand your question but the generated SQL encountered an error after ${MAX_RETRIES + 1} attempts: ${err.message}`,
          sql: null,
          results: null,
          confidence: 'low',
          error: err.message,
          tokenUsage: null,
        };
      }
    }
  }
}

module.exports = {
  queryStructuredData,
  loadSchemaContext,
  schemaLink,
};
