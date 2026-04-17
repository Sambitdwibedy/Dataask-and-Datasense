/**
 * LLM Enrichment Service - BOKG Builder
 *
 * Enriches database schema metadata using Anthropic Claude API to generate
 * human-readable business names, descriptions, confidence scores, value dictionaries,
 * and domain-specific guidance (join paths, formula hints, disambiguation rules).
 *
 * Designed to match the quality that achieved 81.2% on the BIRD Financial benchmark.
 * Key insight: enrichment quality depends heavily on PROFILING DATA (actual row values,
 * value dictionaries, sample data) — not just column names.
 */

const API_BASE = 'https://api.anthropic.com/v1/messages';
const MODEL = 'claude-sonnet-4-20250514';
const API_KEY = process.env.ANTHROPIC_API_KEY;
const API_TIMEOUT = 180000; // 180 seconds per request (large tables with profiling data need time)
const MAX_RETRIES = 3;

function getBackoffDelay(attempt) {
  return Math.min(1000 * Math.pow(2, attempt), 30000);
}

function isRetryableStatus(status) {
  return status === 429 || status === 500 || status === 502 || status === 503;
}

async function fetchWithRetry(url, options, attempt = 0) {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), API_TIMEOUT);

    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (isRetryableStatus(response.status) && attempt < MAX_RETRIES) {
      const delayMs = getBackoffDelay(attempt);
      console.warn(`LLM API returned ${response.status}, retrying in ${delayMs}ms (attempt ${attempt + 1}/${MAX_RETRIES})`);
      await new Promise(resolve => setTimeout(resolve, delayMs));
      return fetchWithRetry(url, options, attempt + 1);
    }

    return response;
  } catch (err) {
    if (err.name === 'AbortError') {
      if (attempt < MAX_RETRIES) {
        const delayMs = getBackoffDelay(attempt);
        console.warn(`LLM API timeout, retrying in ${delayMs}ms (attempt ${attempt + 1}/${MAX_RETRIES})`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
        return fetchWithRetry(url, options, attempt + 1);
      }
      throw new Error(`LLM API request timeout after ${MAX_RETRIES} retries`);
    }
    throw err;
  }
}

async function callClaudeAPI(systemPrompt, userPrompt) {
  if (!API_KEY) {
    throw new Error('ANTHROPIC_API_KEY environment variable not set');
  }

  const requestBody = {
    model: MODEL,
    max_tokens: 16384,
    temperature: 0, // Deterministic output — key finding from rollback evaluation
    system: systemPrompt,
    messages: [{ role: 'user', content: userPrompt }],
  };

  const response = await fetchWithRetry(API_BASE, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify(requestBody),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`LLM API error ${response.status}: ${errorText}`);
  }

  const data = await response.json();

  if (!data.content || data.content.length === 0) {
    throw new Error('Empty response from LLM API');
  }

  // Extract token usage from API response
  const usage = data.usage || {};
  return {
    text: data.content[0].text,
    token_usage: {
      input_tokens: usage.input_tokens || 0,
      output_tokens: usage.output_tokens || 0,
      total_tokens: (usage.input_tokens || 0) + (usage.output_tokens || 0),
      model: MODEL,
    }
  };
}

function parseJSONResponse(text) {
  // Try markdown code block first
  const codeBlockMatch = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (codeBlockMatch) {
    return JSON.parse(codeBlockMatch[1].trim());
  }
  // Try raw JSON
  return JSON.parse(text);
}

/**
 * Build the enrichment system prompt — includes profiling-aware instructions
 */
function buildEnrichmentSystemPrompt(domainTaxonomy) {
  const domainInstruction = domainTaxonomy && domainTaxonomy.length > 0
    ? `\nDOMAIN/MODULE CLASSIFICATION (CRITICAL):
You MUST assign the "module" field to one of the following pre-defined domains.
Do NOT invent new domain names or create variations. Pick the BEST match from this list:
${domainTaxonomy.map(d => `- "${d.name}"${d.description ? ` — ${d.description}` : ''}`).join('\n')}

If a table truly does not fit any domain above, you may use "Other" but this should be rare.
NEVER create slight variations like "PO - Procurement", "Procurement (PO)", "PO - Purchasing" — these are ALL the same domain. Use the EXACT string from the list above.\n`
    : `\nDOMAIN/MODULE CLASSIFICATION:
Assign a concise, consistent domain/module name. Use broad categories (e.g., "Purchasing", "Accounts Payable", "General Ledger") rather than overly specific labels. Tables with similar prefixes (e.g., PO_*, AP_*, GL_*) should share the SAME domain name. Consistency is critical — do not create variations like "PO - Procurement" vs "Procurement (PO)" vs "PO - Purchasing". Pick ONE label and use it consistently.\n`;

  return `You are an expert database schema analyst for enterprise information architecture.

Your task is to enrich database schema metadata by generating:
1. Human-readable business names for each column
2. Rich descriptions that include domain-specific guidance for SQL generation
3. Column roles that classify how each column should be used in queries
4. Value dictionaries for coded/categorical columns
5. Confidence scores reflecting your certainty
6. Entity-level descriptions for the table itself

CONTEXT:
- Enterprise databases often have cryptic abbreviated names (e.g., A2, A3, CUST_ID, FK_ORDER)
- You will receive ACTUAL DATA SAMPLES and VALUE DISTRIBUTIONS — use these to understand what columns really contain
- Your enrichment will be used by an AI SQL generation system, so descriptions should include:
  * What values mean (especially coded values in non-English languages)
  * Which table to use for specific query types (disambiguation)
  * Join path guidance (how tables connect)
  * Formula guidance (how to compute derived metrics)
  * Edge cases and gotchas
${domainInstruction}
GUIDELINES FOR COLUMNS:

1. Business Names:
   - Title case (e.g., "Customer ID", "Transaction Amount")
   - Be specific — "Account Opening Date" not just "Date"
   - For FK columns, indicate the relationship

2. Descriptions (CRITICAL for SQL accuracy):
   - Explain what the column represents in business terms
   - If you see coded values (e.g., Czech language codes), decode them
   - Include value mappings: "VYDAJ=withdrawal, PRIJEM=credit"
   - Include usage guidance: "Use this column when the question asks about X"
   - Include disambiguation: "Use the 'order' table (NOT 'trans') for standing orders"
   - Include formula guidance where relevant
   - Mention if the table name is a SQL reserved word (e.g., "order" must be quoted)
   - IMPORTANT — Business Term Aliases: Include common synonyms and alternative names that users might
     use when referring to this column. For example, LIST_PRICE_PER_UNIT should mention "Also known as:
     standard cost, item cost, unit cost" because users often say "standard cost" when they mean list price.
     Similarly, SEGMENT1 in Oracle EBS should mention "Also known as: item number, part number".
     This is critical because the SQL generator will search descriptions when no exact column name matches
     the user's question. Without aliases, the model will invent non-existent columns.

3. Value Dictionaries:
   - For columns with ≤50 distinct values, include ALL values with counts
   - Decode coded values into English meanings
   - This is critical for non-English databases

4. Column Roles (CRITICAL for SQL generation quality):
   Classify each column's role so the SQL generator knows how to use it:
   - "surrogate_key": Auto-generated numeric ID used only for joins, NOT for display. Common patterns: *_ID columns that are PKs or FKs, internal sequence numbers. Users never ask for these directly.
   - "natural_key": The human-readable business identifier users actually refer to. Examples: item NUMBER, order NUMBER, employee NAME, part CODE, SEGMENT1 in Oracle EBS (item number). This is what to SELECT when users say "show me the items" or "by customer".
   - "measure": Numeric values users want to aggregate — amounts, quantities, costs, prices, rates, percentages.
   - "dimension": Categorical columns used for grouping/filtering — status, type, category codes, org names.
   - "date": Date/timestamp columns.
   - "description_col": Free-text description or name columns that provide human-readable context (e.g., DESCRIPTION, COMMENTS, NOTE).
   - "flag": Boolean or Y/N indicator columns.
   - "technical": System/audit columns users rarely query — WHO_CREATED, LAST_UPDATE_DATE, REQUEST_ID, PROGRAM_ID, etc.
   - "fk_only": Foreign key columns whose ONLY purpose is joining — they reference a surrogate_key in another table and should never appear in SELECT. Examples: ORG_ID when ORGANIZATION_NAME exists in a joined table.

   IMPORTANT: The distinction between surrogate_key and natural_key is the #1 quality driver for SQL generation.
   When a table has both INVENTORY_ITEM_ID (surrogate_key) and SEGMENT1 "Item Number" (natural_key), the SQL generator
   must know to SELECT SEGMENT1 for display and use INVENTORY_ITEM_ID only in JOIN conditions.

5. Confidence Score (0-100):
   - 90-100: Clear name + confirmed by data samples
   - 70-89: Good inference from name + data
   - 50-69: Moderate — data helps but meaning still partially unclear
   - 30-49: Low — cryptic name, data provides some clues
   - 0-29: Very low — opaque name, no clear pattern in data

6. Computed Measures (CRITICAL for query accuracy):
   Enterprise schemas rarely store derived business metrics as columns. Users ask for "total sales",
   "order amounts", "line values", "margin", etc. — but these must be COMPUTED from raw columns.
   For each table, identify the common business metrics that users would ask about and provide the
   SQL formula to compute them.

   Examples:
   - Order table with UNIT_SELLING_PRICE and ORDERED_QUANTITY → computed measure:
     {"name": "Line Amount", "formula": "UNIT_SELLING_PRICE * ORDERED_QUANTITY", "description": "Dollar value of each order line"}
   - Invoice lines with QUANTITY_INVOICED and UNIT_PRICE → computed measure:
     {"name": "Invoice Line Amount", "formula": "QUANTITY_INVOICED * UNIT_PRICE", "description": "Dollar amount of each invoice line"}
   - If a dollar amount requires joining a HEADER table to a LINES table, specify the join:
     {"name": "Sales Order Value", "formula": "SUM(lines.UNIT_SELLING_PRICE * lines.ORDERED_QUANTITY)", "join_table": "OE_ORDER_LINES_ALL", "join_on": "HEADER_ID = HEADER_ID", "description": "Total dollar value of a sales order (sum of all line amounts)"}

   IMPORTANT: If users would commonly ask "how much" or "dollar amount" or "total value" for this table
   and there is NO single column that answers it directly, you MUST provide a computed measure.
   This is the #1 cause of wrong SQL — the model falls back to COUNT(*) when it can't find an amount column.

7. Synonyms (CRITICAL for natural language matching):
   For each column, generate 2-4 natural language synonyms that a business user might use
   when asking questions about this data. These synonyms power the natural language search
   that maps user questions to the correct columns.

   Guidelines:
   - Use natural business language (e.g., "revenue" for INVOICE_AMOUNT, "DOB" for BIRTH_DATE)
   - Include common abbreviations and alternate phrasings
   - Do NOT include the column name itself or its business name as synonyms
   - Focus on terms a non-technical user would actually say
   - For medical/scientific columns, include both clinical and layperson terms
   - For coded columns, include the business concept the code represents

   Examples:
   - UNIT_SELLING_PRICE → ["selling price", "unit price", "price per unit", "item price"]
   - BIRTH_DATE → ["date of birth", "DOB", "birthday", "born on"]
   - T_CHO (cholesterol) → ["total cholesterol", "cholesterol level", "cholesterol"]

RESPONSE FORMAT:
Return valid JSON only (no markdown). Structure:
{
  "table_description": "Rich description of the table including join guidance, disambiguation, and domain context",
  "entity_type": "MASTER|TRANSACTION|REFERENCE|ASSOCIATION",
  "module": "Exact domain name from the taxonomy provided",
  "sample_questions": ["Example NL questions this table helps answer"],
  "computed_measures": [
    {
      "name": "Human-readable metric name (e.g. Line Amount, Order Value)",
      "formula": "SQL expression using column names from this table or a joined table",
      "join_table": "Other table name if the formula requires a JOIN (optional)",
      "join_on": "join_column = join_column (optional)",
      "description": "When to use this: what user questions map to this metric"
    }
  ],
  "columns": [
    {
      "column_name": "original_name",
      "business_name": "Human Readable Name",
      "description": "Rich description with SQL generation guidance",
      "column_role": "surrogate_key|natural_key|measure|dimension|date|description_col|flag|technical|fk_only",
      "confidence_score": 85,
      "value_dictionary": {"value1": count1, "value2": count2},
      "synonyms": ["synonym1", "synonym2", "synonym3"]
    }
  ]
}`;
}

/**
 * Format column data with profiling information for the prompt
 */
function formatColumnWithProfile(col, profile) {
  const parts = [`- ${col.column_name} (${col.data_type})`];

  if (col.is_pk) parts.push('[PRIMARY KEY]');
  if (col.is_fk) parts.push(`[FOREIGN KEY -> ${col.fk_reference || 'unknown'}]`);

  if (profile) {
    if (profile.distinct_count !== undefined) {
      parts.push(`[${profile.distinct_count} distinct values]`);
    }
    if (profile.null_rate > 0) {
      parts.push(`[${(profile.null_rate * 100).toFixed(1)}% null]`);
    }
    if (profile.is_numeric) {
      parts.push(`[range: ${profile.min_value} to ${profile.max_value}, avg: ${profile.avg_value}]`);
    }
    if (profile.value_dictionary && Object.keys(profile.value_dictionary).length > 0) {
      const vd = Object.entries(profile.value_dictionary)
        .slice(0, 15)
        .map(([v, c]) => `${v}(${c})`)
        .join(', ');
      parts.push(`\n    Value distribution: ${vd}`);
    } else if (profile.sample_values && profile.sample_values.length > 0) {
      const samples = profile.sample_values.slice(0, 10).join(', ');
      parts.push(`\n    Sample values: ${samples}`);
    }
  }

  return parts.join(' ');
}

/**
 * Enrich all columns for a single table, using profiling data
 *
 * @param {string} tableName - Table name
 * @param {array} columns - Column objects: { id, column_name, data_type, is_pk, is_fk, fk_reference }
 * @param {object} appContext - { app_name, app_type, related_tables: string[] }
 * @param {object} profileData - From profileTable(): { row_count, sample_rows, column_profiles }
 * @returns {Promise<object>} Enrichment result
 */
async function enrichTable(tableName, columns, appContext = {}, profileData = null) {
  if (!columns || columns.length === 0) {
    return { table_name: tableName, enrichment_status: 'skipped', columns: [], table_description: '' };
  }

  // CHUNKING: Split large tables into manageable chunks to avoid LLM timeout
  const CHUNK_THRESHOLD = 30;  // Tables above this get chunked
  const CHUNK_SIZE = 25;       // Columns per chunk

  if (columns.length > CHUNK_THRESHOLD) {
    console.log(`[Chunking] Table "${tableName}" has ${columns.length} columns — splitting into chunks of ${CHUNK_SIZE}`);

    const chunks = [];
    for (let i = 0; i < columns.length; i += CHUNK_SIZE) {
      chunks.push(columns.slice(i, i + CHUNK_SIZE));
    }
    console.log(`[Chunking] Created ${chunks.length} chunks for "${tableName}"`);

    // Enrich first chunk with full context to get table-level metadata
    const firstResult = await enrichTable(tableName, chunks[0], appContext, profileData);

    if (firstResult.enrichment_status === 'failed') {
      console.error(`[Chunking] First chunk failed for "${tableName}" — aborting remaining chunks`);
      // Return failure for all columns
      return {
        ...firstResult,
        columns: columns.map(col => ({
          id: col.id, column_name: col.column_name,
          business_name: '', description: '', confidence_score: 0, value_dictionary: null,
        })),
      };
    }

    // Enrich remaining chunks with table context from first result
    const allColumns = [...firstResult.columns];
    let totalApiCalls = firstResult.api_calls || 1;
    let totalTokens = { ...(firstResult.token_usage || { input_tokens: 0, output_tokens: 0, total_tokens: 0 }) };

    for (let i = 1; i < chunks.length; i++) {
      console.log(`[Chunking] Enriching chunk ${i + 1}/${chunks.length} for "${tableName}" (${chunks[i].length} cols)`);
      // Pass table description from first chunk as additional context
      const chunkContext = {
        ...appContext,
        context_documents: `Table "${tableName}" description: ${firstResult.table_description || 'N/A'}\nEntity type: ${firstResult.entity_type || 'UNKNOWN'}\nDomain: ${firstResult.module || 'General'}`,
      };
      const chunkResult = await enrichTable(tableName, chunks[i], chunkContext, profileData);

      if (chunkResult.columns) {
        allColumns.push(...chunkResult.columns);
      }
      totalApiCalls += chunkResult.api_calls || 1;
      if (chunkResult.token_usage) {
        totalTokens.input_tokens += chunkResult.token_usage.input_tokens || 0;
        totalTokens.output_tokens += chunkResult.token_usage.output_tokens || 0;
        totalTokens.total_tokens += chunkResult.token_usage.total_tokens || 0;
      }
    }

    console.log(`[Chunking] Completed "${tableName}": ${allColumns.length}/${columns.length} columns enriched in ${totalApiCalls} API calls`);

    return {
      table_name: tableName,
      enrichment_status: 'success',
      table_description: firstResult.table_description || '',
      entity_type: firstResult.entity_type || 'MASTER',
      module: firstResult.module || 'General',
      sample_questions: firstResult.sample_questions || [],
      computed_measures: firstResult.computed_measures || [],
      columns: allColumns,
      api_calls: totalApiCalls,
      token_usage: { ...totalTokens, model: MODEL },
    };
  }

  try {
    const systemPrompt = buildEnrichmentSystemPrompt(appContext.domain_taxonomy);

    // Build context section
    const contextLines = [
      `Table Name: ${tableName}`,
      appContext.app_name ? `Application: ${appContext.app_name}` : null,
      appContext.app_type ? `Application Type: ${appContext.app_type}` : null,
    ].filter(Boolean);

    if (profileData && profileData.row_count !== undefined) {
      contextLines.push(`Row Count: ${profileData.row_count.toLocaleString()}`);
    }

    const relatedTables = appContext.related_tables && appContext.related_tables.length > 0
      ? `\nOther Tables in Database:\n${appContext.related_tables.map(t => `- ${t}`).join('\n')}`
      : '';

    // Include context documents if provided (Context-Assisted Build)
    // Context is pre-scoped by the pipeline — only table-relevant metadata + user docs
    // The 30K safety limit here is a backstop; pipeline already budgets per-table context
    let contextSection = '';
    let hasContext = false;
    if (appContext.context_documents && appContext.context_documents.length > 0) {
      hasContext = true;
      const contextText = typeof appContext.context_documents === 'string'
        ? appContext.context_documents
        : appContext.context_documents;
      const truncated = contextText.length > 30000
        ? contextText.substring(0, 30000) + '\n[...truncated for length]'
        : contextText;
      console.log(`[Context] enrichTable "${tableName}": injecting ${truncated.length} chars of reference docs into prompt`);
      contextSection = `\n*** REFERENCE DOCUMENTATION — CRITICAL: Use this as your PRIMARY source of truth for column meanings ***
These documents describe the database schema. When a column name matches a documented field, USE the documented meaning
rather than guessing. Increase confidence scores for columns that are confirmed by documentation.

${truncated}
*** END REFERENCE DOCUMENTATION ***\n`;
    } else {
      console.log(`[Context] enrichTable "${tableName}": NO context documents provided — inference only`);
    }

    // Build column listing with profiling data
    const columnProfiles = profileData?.column_profiles || {};
    const columnListing = columns
      .map(col => formatColumnWithProfile(col, columnProfiles[col.column_name]))
      .join('\n');

    // Include sample rows if available
    let sampleRowsSection = '';
    if (profileData && profileData.sample_rows && profileData.sample_rows.length > 0) {
      const rows = profileData.sample_rows;
      const rowStrs = rows.map(r => JSON.stringify(r));
      sampleRowsSection = `\nSAMPLE ROWS (${rows.length} rows):\n${rowStrs.join('\n')}\n`;
    }

    const userPrompt = `${contextLines.join('\n')}${relatedTables}
${contextSection}
COLUMNS TO ENRICH:
${columnListing}
${sampleRowsSection}
Analyze the table structure, column names, data types, value distributions, and sample rows.
${hasContext ? `IMPORTANT: The REFERENCE DOCUMENTATION above contains authoritative descriptions of these columns.
You MUST cross-reference EVERY column against the documentation. When a column name appears in the documentation,
use the documented meaning as your primary source and set confidence to 85+ (documentation-confirmed).
Do NOT guess or infer meanings for columns that are clearly described in the documentation.\n` : ''}Generate enriched metadata for each column including business names, rich descriptions with SQL guidance,
value dictionaries for coded columns, and confidence scores.

Also generate a table-level description, entity type, module classification, and 2-3 sample NL questions.

Respond with JSON only.`;

    console.log(`Enriching table "${tableName}" (${columns.length} columns, profile: ${profileData ? 'yes' : 'no'})...`);

    const apiResult = await callClaudeAPI(systemPrompt, userPrompt);
    const responseText = apiResult.text;
    const tokenUsage = apiResult.token_usage;

    let result;
    try {
      result = parseJSONResponse(responseText);
    } catch (parseErr) {
      console.error(`Failed to parse JSON for table ${tableName}:`, parseErr.message);
      throw new Error(`LLM response was not valid JSON: ${parseErr.message}`);
    }

    if (!Array.isArray(result.columns)) {
      throw new Error('LLM response does not contain a "columns" array');
    }

    // Map results back by column_name (NOT by position — LLM may reorder columns)
    // Build lookup: column_name -> original column object (with DB id)
    const colLookup = {};
    for (const col of columns) {
      colLookup[col.column_name.toLowerCase()] = col;
    }

    const enrichedColumns = [];
    const matchedIds = new Set();

    // First pass: match LLM results to input columns by column_name
    for (const enriched of result.columns) {
      const key = (enriched.column_name || '').toLowerCase();
      const original = colLookup[key];
      if (original) {
        enrichedColumns.push({
          id: original.id,
          column_name: original.column_name,
          business_name: enriched.business_name || '',
          description: enriched.description || '',
          column_role: enriched.column_role || null,
          confidence_score: Math.max(0, Math.min(100, enriched.confidence_score || 0)),
          value_dictionary: enriched.value_dictionary || null,
          synonyms: Array.isArray(enriched.synonyms) ? enriched.synonyms : [],
        });
        matchedIds.add(original.id);
      } else {
        console.warn(`Enrichment returned unknown column "${enriched.column_name}" for table "${tableName}" — skipping`);
      }
    }

    // Second pass: collect any input columns not matched by LLM
    const missedColumns = [];
    for (const col of columns) {
      if (!matchedIds.has(col.id)) {
        missedColumns.push(col);
      }
    }

    // Retry pass: if LLM missed columns (likely due to output truncation), re-enrich them
    let retryApiCalls = 0;
    let retryTokenUsage = { input_tokens: 0, output_tokens: 0, total_tokens: 0 };
    if (missedColumns.length > 0 && missedColumns.length < columns.length) {
      console.log(`[Retry] ${missedColumns.length}/${columns.length} columns missed for "${tableName}" — sending focused retry`);
      try {
        const retryColumnListing = missedColumns
          .map(col => formatColumnWithProfile(col, (profileData?.column_profiles || {})[col.column_name]))
          .join('\n');

        const retryPrompt = `Table: ${tableName}
Application: ${appContext.app_name || 'Unknown'} (${appContext.app_type || 'database'})
Table Description: ${result.table_description || 'N/A'}
Domain: ${result.module || 'General'}

The following columns were NOT enriched in a previous pass. Enrich each one now.

COLUMNS TO ENRICH:
${retryColumnListing}

${profileData?.sample_rows?.length > 0 ? `SAMPLE ROWS:\n${profileData.sample_rows.slice(0, 3).map(r => JSON.stringify(r)).join('\n')}\n` : ''}
Generate enriched metadata for EVERY column listed above. Do not skip any.

Respond with JSON only: { "columns": [ { "column_name": "...", "business_name": "...", "description": "...", "column_role": "...", "confidence_score": N, "value_dictionary": null, "synonyms": [] } ] }`;

        const retryResult = await callClaudeAPI(
          'You are an expert database schema analyst. Enrich the given columns with business names, descriptions, roles, and confidence scores. Return JSON only.',
          retryPrompt
        );

        retryApiCalls = 1;
        retryTokenUsage = retryResult.token_usage || retryTokenUsage;

        const retryParsed = parseJSONResponse(retryResult.text);
        if (Array.isArray(retryParsed.columns)) {
          const retryMatched = new Set();
          for (const enriched of retryParsed.columns) {
            const key = (enriched.column_name || '').toLowerCase();
            const original = colLookup[key];
            if (original && !matchedIds.has(original.id)) {
              enrichedColumns.push({
                id: original.id,
                column_name: original.column_name,
                business_name: enriched.business_name || '',
                description: enriched.description || '',
                column_role: enriched.column_role || null,
                confidence_score: Math.max(0, Math.min(100, enriched.confidence_score || 0)),
                value_dictionary: enriched.value_dictionary || null,
                synonyms: Array.isArray(enriched.synonyms) ? enriched.synonyms : [],
              });
              retryMatched.add(original.id);
              matchedIds.add(original.id);
            }
          }
          console.log(`[Retry] Recovered ${retryMatched.size}/${missedColumns.length} columns for "${tableName}"`);
        }
      } catch (retryErr) {
        console.warn(`[Retry] Failed for "${tableName}": ${retryErr.message} — falling back to zero-confidence`);
      }
    }

    // Final fallback: any still-unmatched columns get zero-confidence placeholder
    for (const col of columns) {
      if (!matchedIds.has(col.id)) {
        console.warn(`Column "${col.column_name}" in table "${tableName}" unmatched after retry — zero confidence`);
        enrichedColumns.push({
          id: col.id,
          column_name: col.column_name,
          business_name: '',
          description: '',
          column_role: null,
          confidence_score: 0,
          value_dictionary: null,
        });
      }
    }

    return {
      table_name: tableName,
      enrichment_status: 'success',
      table_description: result.table_description || '',
      entity_type: result.entity_type || 'MASTER',
      module: result.module || 'General',
      sample_questions: result.sample_questions || [],
      computed_measures: result.computed_measures || [],
      columns: enrichedColumns,
      api_calls: 1 + retryApiCalls,
      token_usage: {
        input_tokens: tokenUsage.input_tokens + retryTokenUsage.input_tokens,
        output_tokens: tokenUsage.output_tokens + retryTokenUsage.output_tokens,
        total_tokens: tokenUsage.total_tokens + retryTokenUsage.total_tokens,
        model: MODEL,
      },
    };
  } catch (err) {
    console.error(`Failed to enrich table ${tableName}:`, err.message);

    return {
      table_name: tableName,
      enrichment_status: 'failed',
      table_description: '',
      columns: columns.map(col => ({
        id: col.id,
        column_name: col.column_name,
        business_name: '',
        description: '',
        confidence_score: 0,
        value_dictionary: null,
      })),
      error: err.message,
    };
  }
}

/**
 * Enrich columns across multiple tables
 */
async function enrichColumns(columns, tableContext = {}) {
  if (!columns || columns.length === 0) {
    return { enrichment_status: 'skipped', total_columns: 0, enriched_columns: [], stats: {} };
  }

  // Group by table
  const columnsByTable = {};
  columns.forEach(col => {
    const tableName = col.table_name || 'unknown';
    if (!columnsByTable[tableName]) columnsByTable[tableName] = [];
    columnsByTable[tableName].push(col);
  });

  const enrichedByTable = {};
  let totalSuccess = 0, totalFailed = 0;
  const allConfidenceScores = [];

  for (const [tableName, tableColumns] of Object.entries(columnsByTable)) {
    const appContext = {
      ...tableContext,
      related_tables: (tableContext.other_tables || []).filter(t => t !== tableName),
    };

    const result = await enrichTable(tableName, tableColumns, appContext);
    enrichedByTable[tableName] = result;

    if (result.enrichment_status === 'success') {
      totalSuccess++;
      result.columns.forEach(col => {
        if (col.confidence_score > 0) allConfidenceScores.push(col.confidence_score);
      });
    } else {
      totalFailed++;
    }
  }

  const allEnrichedColumns = [];
  Object.values(enrichedByTable).forEach(r => allEnrichedColumns.push(...(r.columns || [])));

  const avgConfidence = allConfidenceScores.length > 0
    ? Math.round(allConfidenceScores.reduce((a, b) => a + b, 0) / allConfidenceScores.length)
    : 0;

  return {
    enrichment_status: totalFailed === 0 ? 'success' : 'partial',
    total_columns: columns.length,
    enriched_columns: allEnrichedColumns,
    by_table: enrichedByTable,
    stats: {
      success: totalSuccess,
      failed: totalFailed,
      avg_confidence: avgConfidence,
      min_confidence: allConfidenceScores.length > 0 ? Math.min(...allConfidenceScores) : 0,
      max_confidence: allConfidenceScores.length > 0 ? Math.max(...allConfidenceScores) : 0,
    },
  };
}

module.exports = {
  enrichTable,
  enrichColumns,
  callClaudeAPI,
  parseJSONResponse,
  buildEnrichmentSystemPrompt,
};
