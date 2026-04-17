/**
 * Metadata Table Discovery Service
 *
 * Automatically discovers tables within the source database that contain
 * metadata about other tables — column descriptions, field labels, entity
 * definitions, configuration data, etc.
 *
 * Enterprise applications commonly embed their own data dictionaries:
 *   Oracle EBS:    FND_TABLES, FND_COLUMNS, FND_DESCRIPTIVE_FLEXS
 *   SAP:           DD03L, DD04T, DD02T, TADIR
 *   OFBiz:         ENTITY_*, SERVICE_*
 *   PeopleSoft:    PSRECDEFN, PSDBFIELD, PSRECFIELD
 *   Salesforce:    EntityDefinition, FieldDefinition
 *   Custom apps:   *_metadata, *_config, *_dictionary, *_definition
 *
 * Discovery approach:
 *   1. Name-based heuristics — scan table names for known patterns
 *   2. Structure-based heuristics — look for columns that describe other tables
 *   3. Content-based confirmation — verify that values reference actual schema objects
 *   4. Extract — pull descriptions/labels into context document format
 */

const { query } = require('../db');
const { executeOnSourceData } = require('./data-loader');

// ============================================================
// KNOWN METADATA TABLE PATTERNS
// ============================================================

// Table name patterns that commonly contain metadata
const METADATA_TABLE_PATTERNS = [
  // Oracle EBS
  /^fnd_tables$/i, /^fnd_columns$/i, /^fnd_descriptive_flexs$/i,
  /^fnd_lookup_values$/i, /^fnd_application$/i, /^fnd_flex_value_sets$/i,
  // SAP
  /^dd02t$/i, /^dd03l$/i, /^dd04t$/i, /^dd07t$/i, /^tadir$/i,
  // OFBiz / generic Java ERP
  /^entity_/i, /^service_/i,
  // PeopleSoft
  /^psrecdefn$/i, /^psdbfield$/i, /^psrecfield$/i,
  // Generic patterns
  /metadata$/i, /_metadata$/i, /^metadata_/i,
  /data_dictionary/i, /^dd_/i,
  /_definition$/i, /^definition_/i,
  /_config$/i, /^config_/i, /^sys_config/i,
  /_description$/i, /^description_/i,
  /column_desc/i, /table_desc/i, /field_desc/i,
  /schema_info/i, /table_info/i, /field_info/i,
  /^catalog_/i, /^sys_catalog/i,
  /lookup_type/i, /lookup_value/i, /reference_code/i,
  /value_set/i, /code_table/i, /code_value/i,
];

// Column name patterns that suggest a table describes other tables
const DESCRIPTOR_COLUMN_PATTERNS = [
  // Columns that reference table/entity names
  { name: /^table_name$/i, role: 'table_ref', weight: 3 },
  { name: /^entity_name$/i, role: 'table_ref', weight: 3 },
  { name: /^object_name$/i, role: 'table_ref', weight: 2 },
  { name: /^tabname$/i, role: 'table_ref', weight: 3 },
  { name: /^table_id$/i, role: 'table_ref', weight: 1 },

  // Columns that reference field/column names
  { name: /^column_name$/i, role: 'column_ref', weight: 3 },
  { name: /^field_name$/i, role: 'column_ref', weight: 3 },
  { name: /^fieldname$/i, role: 'column_ref', weight: 3 },
  { name: /^attribute_name$/i, role: 'column_ref', weight: 2 },
  { name: /^column_id$/i, role: 'column_ref', weight: 1 },

  // Columns that contain descriptions/labels
  { name: /^description$/i, role: 'description', weight: 2 },
  { name: /^column_description$/i, role: 'description', weight: 3 },
  { name: /^field_description$/i, role: 'description', weight: 3 },
  { name: /^label$/i, role: 'description', weight: 2 },
  { name: /^display_name$/i, role: 'description', weight: 2 },
  { name: /^business_name$/i, role: 'description', weight: 3 },
  { name: /^short_desc$/i, role: 'description', weight: 2 },
  { name: /^long_desc$/i, role: 'description', weight: 2 },
  { name: /^comment$/i, role: 'description', weight: 1 },
  { name: /^remarks$/i, role: 'description', weight: 1 },
  { name: /^data_type$/i, role: 'type_info', weight: 1 },
];

// ============================================================
// DISCOVERY FUNCTIONS
// ============================================================

/**
 * Discover metadata tables within the application's source data
 *
 * @param {number} appId - Application ID
 * @returns {object} Discovery results with candidates and extracted metadata
 */
async function discoverMetadataTables(appId) {
  console.log(`[MetadataDiscovery] Starting discovery for app ${appId}...`);

  // Get all tables in the application
  const tablesResult = await query(
    `SELECT id, table_name, row_count FROM app_tables WHERE app_id = $1 ORDER BY table_name`,
    [appId]
  );
  const tables = tablesResult.rows;
  const tableNames = tables.map(t => t.table_name.toLowerCase());

  console.log(`[MetadataDiscovery] Scanning ${tables.length} tables...`);

  const candidates = [];

  for (const table of tables) {
    const score = await scoreMetadataCandidate(appId, table, tableNames);
    if (score.total > 0) {
      candidates.push({
        table_id: table.id,
        table_name: table.table_name,
        row_count: table.row_count,
        score: score.total,
        reasons: score.reasons,
        descriptor_columns: score.descriptor_columns,
        confirmed: score.confirmed,
        confirmed_ref_col: score.confirmed_ref_col || null,
        fk_resolution: score.fk_resolution || null,
      });
    }
  }

  // Sort by score descending
  candidates.sort((a, b) => b.score - a.score);

  console.log(`[MetadataDiscovery] Found ${candidates.length} metadata table candidates`);

  // Extract metadata from confirmed candidates
  let extractedContext = '';
  let extractedEntries = 0;
  // Merged structured index: { "table_name": [ { column, description, type, source } ] }
  const mergedTableIndex = {};

  for (const candidate of candidates.filter(c => c.confirmed || c.score >= 5)) {
    try {
      const extracted = await extractMetadataFromTable(appId, candidate, tableNames);
      if (extracted.text && extracted.entries > 0) {
        extractedContext += `\n=== Auto-discovered from: ${candidate.table_name} ===\n`;
        extractedContext += extracted.text + '\n';
        extractedEntries += extracted.entries;
        candidate.extracted_entries = extracted.entries;

        // Merge per-table index
        for (const [tbl, cols] of Object.entries(extracted.tableIndex || {})) {
          if (!mergedTableIndex[tbl]) mergedTableIndex[tbl] = [];
          mergedTableIndex[tbl].push(...cols);
        }
      }
    } catch (err) {
      console.error(`[MetadataDiscovery] Failed to extract from ${candidate.table_name}:`, err.message);
    }
  }

  const indexedTableCount = Object.keys(mergedTableIndex).length;
  console.log(`[MetadataDiscovery] Extracted ${extractedEntries} metadata entries from ${candidates.filter(c => c.extracted_entries > 0).length} tables, indexed ${indexedTableCount} target tables`);

  return {
    tables_scanned: tables.length,
    candidates,
    confirmed_count: candidates.filter(c => c.confirmed).length,
    extracted_entries: extractedEntries,
    extracted_context: extractedContext.trim(),
    table_index: mergedTableIndex,
  };
}

/**
 * Score a table as a metadata candidate using name, structure, and content heuristics
 */
async function scoreMetadataCandidate(appId, table, allTableNames) {
  const result = { total: 0, reasons: [], descriptor_columns: {}, confirmed: false };
  const tableName = table.table_name;

  // 1. Name-based scoring
  for (const pattern of METADATA_TABLE_PATTERNS) {
    if (pattern.test(tableName)) {
      result.total += 2;
      result.reasons.push(`Name matches metadata pattern: ${pattern}`);
      break;
    }
  }

  // 2. Structure-based scoring — check column names
  const colsResult = await query(
    `SELECT column_name, data_type FROM app_columns WHERE table_id = $1`,
    [table.id]
  );
  const columns = colsResult.rows;

  let hasTableRef = false;
  let hasColumnRef = false;
  let hasDescription = false;

  // Track weights per table_ref column so we can sort them later
  const tableRefWeights = {};

  for (const col of columns) {
    for (const pattern of DESCRIPTOR_COLUMN_PATTERNS) {
      if (pattern.name.test(col.column_name)) {
        result.total += pattern.weight;
        result.descriptor_columns[pattern.role] = result.descriptor_columns[pattern.role] || [];
        result.descriptor_columns[pattern.role].push(col.column_name);

        if (pattern.role === 'table_ref') {
          hasTableRef = true;
          tableRefWeights[col.column_name] = pattern.weight;
        }
        if (pattern.role === 'column_ref') hasColumnRef = true;
        if (pattern.role === 'description') hasDescription = true;
      }
    }
  }

  // Sort table_ref columns by weight descending (prefer TABLE_NAME over TABLE_ID)
  if (result.descriptor_columns.table_ref) {
    result.descriptor_columns.table_ref.sort((a, b) =>
      (tableRefWeights[b] || 0) - (tableRefWeights[a] || 0)
    );
  }

  if (hasTableRef && hasDescription) {
    result.total += 3;
    result.reasons.push('Has both table reference and description columns');
  }
  if (hasColumnRef && hasDescription) {
    result.total += 3;
    result.reasons.push('Has both column reference and description columns');
  }

  // 3. Content-based confirmation — try each table_ref column until one confirms
  if (result.total >= 3 && result.descriptor_columns.table_ref) {
    for (const refCol of result.descriptor_columns.table_ref) {
      try {
        const confirmed = await confirmMetadataContent(
          appId, tableName, refCol, allTableNames
        );
        if (confirmed) {
          result.confirmed = true;
          result.confirmed_ref_col = refCol;
          result.total += 5;
          result.reasons.push(`Content confirmed via ${refCol}: values match actual table names`);
          break;
        }
      } catch (err) {
        // Content check failed — try next column
      }
    }

    // If no direct match, try FK resolution for numeric ID columns
    // e.g., FND_COLUMNS.TABLE_ID → JOIN FND_TABLES to resolve names
    if (!result.confirmed) {
      try {
        const fkResult = await confirmViaForeignKeyResolution(appId, tableName, result.descriptor_columns.table_ref, allTableNames);
        if (fkResult.confirmed) {
          result.confirmed = true;
          result.confirmed_ref_col = fkResult.resolvedRefCol;
          result.fk_resolution = fkResult;
          result.total += 5;
          result.reasons.push(`Content confirmed via FK resolution: ${tableName}.${fkResult.idCol} → ${fkResult.lookupTable}.${fkResult.lookupNameCol}`);
        }
      } catch (err) {
        // FK resolution failed — not fatal
      }
    }
  }

  // Also check for lookup/reference tables: tables with code + description columns
  // and relatively few rows (< 500 typically)
  if (!hasTableRef && hasDescription && table.row_count > 0 && table.row_count < 500) {
    const hasCodeCol = columns.some(c =>
      /^(code|type|key|value|symbol|status|category|kind)$/i.test(c.column_name) ||
      /_(code|type|key|symbol|status|category)$/i.test(c.column_name)
    );
    if (hasCodeCol) {
      result.total += 1;
      result.reasons.push('Looks like a reference/lookup table (code + description, few rows)');
    }
  }

  return result;
}

/**
 * Confirm that a table_ref column actually contains names of real tables
 */
async function confirmMetadataContent(appId, tableName, tableRefColumn, allTableNames) {
  try {
    // Instead of sampling from the metadata table (which fails when it's much larger
    // than our schema), check how many of OUR tables appear in the metadata table.
    // This works correctly regardless of metadata table size.
    const tableList = allTableNames.map(t => `'${t.replace(/'/g, "''")}'`).join(',');
    const matchResult = await executeOnSourceData(appId,
      `SELECT COUNT(DISTINCT LOWER("${tableRefColumn}")) as match_count
       FROM "${tableName}"
       WHERE LOWER("${tableRefColumn}") IN (${tableList})`
    );
    const matchCount = parseInt(matchResult.rows[0]?.match_count || 0);

    // If at least 10% of our schema tables appear in this metadata table, it's confirmed
    return matchCount >= Math.max(3, Math.floor(allTableNames.length * 0.1));
  } catch {
    return false;
  }
}

/**
 * Try to confirm a metadata table by resolving numeric FK columns through companion tables.
 *
 * Example: FND_COLUMNS has TABLE_ID (numeric) → JOIN FND_TABLES ON TABLE_ID to get TABLE_NAME
 *
 * Strategy:
 *   1. For each numeric table_ref column (like TABLE_ID), look for other metadata candidate
 *      tables that have both that same column name AND a TABLE_NAME column
 *   2. JOIN through that companion table and check if resolved names match real tables
 */
async function confirmViaForeignKeyResolution(appId, tableName, tableRefCols, allTableNames) {
  const schemaName = `appdata_${appId}`;

  // Identify numeric ID columns (column names ending in _ID)
  const idCols = tableRefCols.filter(c => /_id$/i.test(c));
  if (idCols.length === 0) return { confirmed: false };

  for (const idCol of idCols) {
    // Look for companion tables that have both this ID column and a TABLE_NAME/ENTITY_NAME column
    // Common patterns: FND_COLUMNS.TABLE_ID → FND_TABLES.TABLE_ID + FND_TABLES.TABLE_NAME
    const companionPatterns = [
      { nameCol: 'TABLE_NAME', idCol },
      { nameCol: 'ENTITY_NAME', idCol },
      { nameCol: 'OBJECT_NAME', idCol },
      { nameCol: 'TABNAME', idCol },
    ];

    // Search for companion tables in the same schema
    for (const pattern of companionPatterns) {
      try {
        // Find tables that have both the ID column and a name column
        const companionResult = await query(
          `SELECT DISTINCT t.table_name
           FROM app_tables t
           JOIN app_columns c1 ON c1.table_id = t.id AND UPPER(c1.column_name) = UPPER($2)
           JOIN app_columns c2 ON c2.table_id = t.id AND UPPER(c2.column_name) = UPPER($3)
           WHERE t.app_id = $1 AND UPPER(t.table_name) != UPPER($4)`,
          [appId, idCol, pattern.nameCol, tableName]
        );

        for (const companion of companionResult.rows) {
          // Check how many of OUR tables appear via the FK resolution JOIN.
          // Instead of sampling (which fails with large metadata tables),
          // count how many of our schema tables exist in the resolved names.
          const tableList = allTableNames.map(t => `'${t.replace(/'/g, "''")}'`).join(',');
          const joinSql = `
            SELECT COUNT(DISTINCT LOWER(c."${pattern.nameCol}")) AS match_count
            FROM "${tableName}" t
            JOIN "${companion.table_name}" c ON c."${idCol}" = t."${idCol}"
            WHERE LOWER(c."${pattern.nameCol}") IN (${tableList})
          `;
          const joinResult = await executeOnSourceData(appId, joinSql);
          const matches = parseInt(joinResult.rows[0]?.match_count || 0);

          if (matches >= Math.max(3, Math.floor(allTableNames.length * 0.1))) {
            return {
              confirmed: true,
              idCol,
              lookupTable: companion.table_name,
              lookupNameCol: pattern.nameCol,
              resolvedRefCol: `${companion.table_name}.${pattern.nameCol}`,
              matchRate: matches / allTableNames.length,
            };
          }
        }
      } catch (err) {
        // This companion pattern didn't work, try next
        continue;
      }
    }
  }

  return { confirmed: false };
}

/**
 * Extract metadata entries from a confirmed metadata table.
 * Handles both direct table_ref columns (TABLE_NAME) and FK-resolved ones (TABLE_ID → JOIN).
 */
async function extractMetadataFromTable(appId, candidate, allTableNames) {
  const { table_name, descriptor_columns, fk_resolution } = candidate;
  const lines = [];
  let entries = 0;
  // Structured index: { "target_table_name": [ { column, description, type } ] }
  const tableIndex = {};

  try {
    // Build a SELECT that pulls the most useful columns
    const selectCols = [];
    let tableRefCol = descriptor_columns.table_ref?.[0];
    const columnRefCol = descriptor_columns.column_ref?.[0];
    const descCols = descriptor_columns.description || [];
    const typeCols = descriptor_columns.type_info || [];

    // Determine if we need FK resolution for the table reference
    const useFkResolution = fk_resolution && fk_resolution.confirmed;
    let joinClause = '';
    let resolvedTableNameAlias = null;

    if (useFkResolution) {
      // Use JOIN to resolve numeric IDs to table names
      // e.g., JOIN FND_TABLES ft ON ft.TABLE_ID = FND_COLUMNS.TABLE_ID → ft.TABLE_NAME
      const alias = 'fk_lookup';
      joinClause = `JOIN "${fk_resolution.lookupTable}" ${alias} ON ${alias}."${fk_resolution.idCol}" = t."${fk_resolution.idCol}"`;
      resolvedTableNameAlias = `${alias}."${fk_resolution.lookupNameCol}"`;
      selectCols.push(`${resolvedTableNameAlias} AS resolved_table_name`);
    } else if (tableRefCol) {
      selectCols.push(`t."${tableRefCol}"`);
    }

    if (columnRefCol) selectCols.push(`t."${columnRefCol}"`);
    for (const d of descCols) selectCols.push(`t."${d}"`);
    for (const t of typeCols) selectCols.push(`t."${t}"`);

    if (selectCols.length === 0) {
      // Fallback: select all columns
      const result = await executeOnSourceData(appId,
        `SELECT * FROM "${table_name}" LIMIT 200`
      );
      if (result.rows.length > 0) {
        lines.push(`Table: ${table_name} (${result.rows.length} rows sampled)`);
        lines.push(`Columns: ${result.columns.join(', ')}`);
        for (const row of result.rows.slice(0, 100)) {
          const vals = Object.entries(row)
            .filter(([k, v]) => v !== null && v !== '')
            .map(([k, v]) => `${k}=${v}`)
            .join(' | ');
          if (vals) { lines.push(vals); entries++; }
        }
      }
    } else {
      // Filter to rows that reference actual tables in our schema
      let whereClause = '';
      if (useFkResolution) {
        // Filter via the resolved table name from the JOIN
        const tableList = allTableNames.map(t => `'${t}'`).join(',');
        whereClause = `WHERE LOWER(fk_lookup."${fk_resolution.lookupNameCol}") IN (${tableList})`;
      } else if (tableRefCol) {
        const tableList = allTableNames.map(t => `'${t}'`).join(',');
        whereClause = `WHERE LOWER(t."${tableRefCol}") IN (${tableList})`;
      }

      const sql = `SELECT ${selectCols.join(', ')} FROM "${table_name}" t ${joinClause} ${whereClause} LIMIT 2000`;
      const result = await executeOnSourceData(appId, sql);

      if (result.rows.length > 0) {
        const displayCols = useFkResolution
          ? ['resolved_table_name', ...(columnRefCol ? [columnRefCol] : []), ...descCols, ...typeCols]
          : selectCols.map(s => s.replace(/^t\./, '').replace(/"/g, ''));
        lines.push(`Metadata from: ${table_name}${useFkResolution ? ` (via ${fk_resolution.lookupTable})` : ''}`);
        lines.push(`Fields: ${displayCols.join(', ')}`);
        lines.push('---');

        for (const row of result.rows) {
          const parts = [];
          // Resolve the target table name — either directly or via FK JOIN
          const targetTable = useFkResolution
            ? (row.resolved_table_name || '').toString().toLowerCase()
            : (tableRefCol ? (row[tableRefCol] || '').toString().toLowerCase() : null);
          const targetColumn = columnRefCol ? (row[columnRefCol] || '').toString() : null;
          const desc = descCols.map(d => row[d]).filter(Boolean).join('; ');
          const typeInfo = typeCols.map(tp => row[tp]).filter(Boolean).join('; ');

          if (targetTable) parts.push(`Table: ${useFkResolution ? row.resolved_table_name : row[tableRefCol]}`);
          if (columnRefCol && row[columnRefCol]) parts.push(`Column: ${row[columnRefCol]}`);
          for (const d of descCols) {
            if (row[d]) parts.push(`Description: ${row[d]}`);
          }
          for (const tp of typeCols) {
            if (row[tp]) parts.push(`Type: ${row[tp]}`);
          }
          if (parts.length > 0) {
            lines.push(parts.join(' | '));
            entries++;

            // Build structured index keyed by target table
            if (targetTable && allTableNames.includes(targetTable)) {
              if (!tableIndex[targetTable]) tableIndex[targetTable] = [];
              tableIndex[targetTable].push({
                column: targetColumn,
                description: desc || null,
                type: typeInfo || null,
                source: table_name,
              });
            }
          }
        }
      }
    }
  } catch (err) {
    lines.push(`[Extraction error: ${err.message}]`);
  }

  return { text: lines.join('\n'), entries, tableIndex };
}

/**
 * Run discovery and store results as auto-generated context documents
 *
 * @param {number} appId - Application ID
 * @returns {object} Summary of what was discovered and stored
 */
async function discoverAndStoreMetadata(appId) {
  const discovery = await discoverMetadataTables(appId);

  if (discovery.extracted_context && discovery.extracted_entries > 0) {
    // Check if we already have an auto-discovered document for this app
    const existing = await query(
      `SELECT id FROM context_documents WHERE app_id = $1 AND filename = 'Auto-Discovered Schema Metadata'`,
      [appId]
    );

    // Store the structured table index as JSON alongside the full text
    // This enables table-scoped context injection at enrichment time
    const indexJson = JSON.stringify(discovery.table_index || {});

    if (existing.rows.length > 0) {
      // Update existing
      await query(
        `UPDATE context_documents
         SET extracted_text = $1, description = $2, file_size = $3, metadata = $4, uploaded_at = NOW()
         WHERE id = $5`,
        [
          discovery.extracted_context,
          `Auto-discovered from ${discovery.confirmed_count} metadata tables (${discovery.extracted_entries} entries, ${Object.keys(discovery.table_index || {}).length} tables indexed)`,
          discovery.extracted_context.length,
          indexJson,
          existing.rows[0].id,
        ]
      );
      console.log(`[MetadataDiscovery] Updated existing auto-discovered context document`);
    } else {
      // Create new
      await query(
        `INSERT INTO context_documents (app_id, filename, file_type, file_size, extracted_text, description, metadata, uploaded_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())`,
        [
          appId,
          'Auto-Discovered Schema Metadata',
          'auto_discovered',
          discovery.extracted_context.length,
          discovery.extracted_context,
          `Auto-discovered from ${discovery.confirmed_count} metadata tables (${discovery.extracted_entries} entries, ${Object.keys(discovery.table_index || {}).length} tables indexed)`,
          indexJson,
        ]
      );
      console.log(`[MetadataDiscovery] Created new auto-discovered context document`);
    }
  }

  return discovery;
}

module.exports = {
  discoverMetadataTables,
  discoverAndStoreMetadata,
  METADATA_TABLE_PATTERNS,
  DESCRIPTOR_COLUMN_PATTERNS,
};
