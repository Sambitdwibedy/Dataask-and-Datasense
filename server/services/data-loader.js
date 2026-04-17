/**
 * Data Loader Service - BOKG Builder
 *
 * Loads source database data into PostgreSQL for live querying and profiling.
 * Supports SQLite files (BIRD benchmark) with auto-detection of types.
 *
 * Creates a separate schema "appdata_{appId}" for each application's source data.
 */

const { pool, query } = require('../db');
const path = require('path');

// Map SQLite types to PostgreSQL types
function sqliteTypeToPg(sqliteType) {
  const t = (sqliteType || '').toUpperCase();
  if (t.includes('INT')) return 'BIGINT';
  if (t.includes('REAL') || t.includes('FLOAT') || t.includes('DOUBLE')) return 'DOUBLE PRECISION';
  if (t.includes('DATE') || t.includes('TIME')) return 'TEXT'; // keep as text for flexibility
  if (t.includes('BLOB')) return 'BYTEA';
  return 'TEXT';
}

/**
 * Load a SQLite database into PostgreSQL as a separate schema
 * @param {number} appId - Application ID
 * @param {string} sqlitePath - Path to .sqlite file
 * @param {object} options - { dropExisting: false }
 * @returns {Promise<object>} Load stats
 */
async function loadSqliteToPostgres(appId, sqlitePath, options = {}) {
  // Dynamically import better-sqlite3 or use sqlite3
  let sqlite3;
  try {
    sqlite3 = require('better-sqlite3');
  } catch {
    // Fallback: use child_process to run python for SQLite extraction
    return loadSqliteViaPython(appId, sqlitePath, options);
  }
}

/**
 * Load SQLite data via Python subprocess (works without better-sqlite3)
 *
 * ARCHITECTURE NOTE (v2.8.2):
 * The original approach dumped ALL tables into a single JSON blob via stdout.
 * With full FND reference data (651K rows in FND_COLUMNS alone), the total
 * payload reached ~1GB — exceeding Node's 500MB maxBuffer and causing silent
 * extraction failures. FND_RESOLVED_FKS never made it into PostgreSQL, which
 * is why Strategy 4 relationships weren't appearing despite correct SQL.
 *
 * Fix: Extract tables one-at-a-time via individual files in /tmp. Each table
 * gets its own JSON file, loaded sequentially. No single buffer ever holds
 * more than one table's data. This also gives better progress logging.
 */
async function loadSqliteViaPython(appId, sqlitePath, options = {}) {
  const { execSync } = require('child_process');
  const fs = require('fs');
  const schemaName = `appdata_${appId}`;

  console.log(`Loading SQLite data into schema "${schemaName}"...`);

  // Drop and recreate schema
  if (options.dropExisting) {
    await query(`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`);
  }
  await query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`);

  // Step 1: Get table list and metadata from SQLite (small payload — just names/counts/schemas)
  const listScript = `
import sqlite3, json, sys
conn = sqlite3.connect(sys.argv[1])
cur = conn.cursor()
tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
result = []
for table in tables:
    col_info = cur.execute(f'PRAGMA table_info("{table}")').fetchall()
    cols = [(c[1], c[2]) for c in col_info]
    pk_cols = [c[1] for c in col_info if c[5] > 0]
    count = cur.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    fks = []
    for fk in cur.execute(f'PRAGMA foreign_key_list("{table}")').fetchall():
        fks.append({"from": fk[3], "to_table": fk[2], "to_col": fk[4]})
    # Fix column types: SQLite allows floats in INTEGER columns
    corrected_cols = []
    for cname, ctype in cols:
        if ctype and "INT" in ctype.upper():
            has_real = cur.execute(f'SELECT 1 FROM "{table}" WHERE typeof("{cname}") = "real" LIMIT 1').fetchone()
            if has_real:
                ctype = "REAL"
        corrected_cols.append((cname, ctype))
    cols = corrected_cols
    result.append({"table": table, "columns": cols, "count": count, "pk_columns": pk_cols, "foreign_keys": fks})
conn.close()
json.dump(result, sys.stdout)
`;

  const listScriptPath = `/tmp/list_sqlite_${appId}.py`;
  fs.writeFileSync(listScriptPath, listScript);

  console.log('Reading table list from SQLite...');
  const listRaw = execSync(`python3 ${listScriptPath} "${sqlitePath}"`, {
    maxBuffer: 10 * 1024 * 1024, // 10MB is plenty for table metadata
    timeout: 30000,
  });
  const tableList = JSON.parse(listRaw.toString());
  console.log(`  Found ${tableList.length} tables in SQLite`);

  // Step 2: Extract each table's rows individually via file-based transfer
  // This avoids the 500MB stdout buffer limit that killed the old single-blob approach.
  const extractOneScript = `
import sqlite3, json, sys
conn = sqlite3.connect(sys.argv[1])
cur = conn.cursor()
table = sys.argv[2]
out_path = sys.argv[3]
rows = cur.execute(f'SELECT * FROM "{table}"').fetchall()
with open(out_path, 'w') as f:
    json.dump([list(r) for r in rows], f)
conn.close()
print(len(rows))
`;

  const extractOnePath = `/tmp/extract_one_${appId}.py`;
  fs.writeFileSync(extractOnePath, extractOneScript);

  const stats = { tables: 0, rows: 0, fk_count: 0, pk_count: 0 };

  // Collect FK and PK info from SQLite for later use in relationship detection
  const sqliteForeignKeys = []; // { from_table, from_col, to_table, to_col }
  const sqlitePrimaryKeys = {}; // table -> [col_names]

  for (const tableData of tableList) {
    const tableName = tableData.table;
    const columns = tableData.columns; // [[name, type], ...]

    // Store PK and FK info
    if (tableData.pk_columns && tableData.pk_columns.length > 0) {
      sqlitePrimaryKeys[tableName] = tableData.pk_columns;
      stats.pk_count += tableData.pk_columns.length;
    }
    if (tableData.foreign_keys && tableData.foreign_keys.length > 0) {
      for (const fk of tableData.foreign_keys) {
        sqliteForeignKeys.push({ from_table: tableName, from_col: fk.from, to_table: fk.to_table, to_col: fk.to_col });
        stats.fk_count++;
      }
    }

    // Create table in PG schema
    const colDefs = columns
      .map(([name, type]) => `"${name}" ${sqliteTypeToPg(type)}`)
      .join(', ');

    const fullTableName = `${schemaName}."${tableName}"`;

    await query(`DROP TABLE IF EXISTS ${fullTableName}`);
    await query(`CREATE TABLE ${fullTableName} (${colDefs})`);

    // Extract rows for this table via file-based transfer
    const rowFile = `/tmp/rows_${appId}_${tableName}.json`;
    try {
      const rowCountStr = execSync(
        `python3 ${extractOnePath} "${sqlitePath}" "${tableName}" "${rowFile}"`,
        { maxBuffer: 1024 * 1024, timeout: 300000 } // 5 min timeout for large tables
      ).toString().trim();

      const rowCount = parseInt(rowCountStr) || 0;

      if (rowCount > 0) {
        // Read rows from the temp file
        const rowsRaw = fs.readFileSync(rowFile, 'utf8');
        const rows = JSON.parse(rowsRaw);

        // Batch insert rows
        const batchSize = 500;
        for (let i = 0; i < rows.length; i += batchSize) {
          const batch = rows.slice(i, i + batchSize);
          const placeholders = batch
            .map(
              (row, rowIdx) =>
                '(' +
                row
                  .map((_, colIdx) => `$${rowIdx * columns.length + colIdx + 1}`)
                  .join(', ') +
                ')'
            )
            .join(', ');

          const values = batch.flat();
          const colNames = columns.map(([name]) => `"${name}"`).join(', ');

          await query(
            `INSERT INTO ${fullTableName} (${colNames}) VALUES ${placeholders}`,
            values
          );
        }

        stats.rows += rows.length;
        console.log(`  ✓ ${tableName}: ${rows.length.toLocaleString()} rows loaded`);
      } else {
        console.log(`  ✓ ${tableName}: 0 rows (empty table)`);
      }
    } catch (extractErr) {
      console.error(`  ✗ ${tableName}: extraction failed — ${extractErr.message}`);
      // Table schema still created (empty) — pipeline can continue
    } finally {
      // Clean up temp file
      try { fs.unlinkSync(rowFile); } catch {}
    }

    stats.tables++;
  }

  // Apply PK/FK info from SQLite to app_columns
  if (stats.pk_count > 0 || stats.fk_count > 0) {
    console.log(`Applying SQLite schema info: ${stats.pk_count} PKs, ${stats.fk_count} FKs...`);
    const appTablesResult = await query('SELECT id, table_name FROM app_tables WHERE app_id = $1', [appId]);
    const appTableMap = {};
    for (const t of appTablesResult.rows) {
      appTableMap[t.table_name.toLowerCase()] = t.id;
    }

    // Mark PKs
    for (const [tableName, pkCols] of Object.entries(sqlitePrimaryKeys)) {
      const tableId = appTableMap[tableName.toLowerCase()];
      if (!tableId) continue;
      for (const pkCol of pkCols) {
        await query(
          `UPDATE app_columns SET is_pk = true WHERE table_id = $1 AND column_name = $2`,
          [tableId, pkCol]
        );
      }
    }

    // Mark FKs with references
    for (const fk of sqliteForeignKeys) {
      const tableId = appTableMap[fk.from_table.toLowerCase()];
      if (!tableId) continue;
      const fkRef = `${fk.to_table}.${fk.to_col}`;
      await query(
        `UPDATE app_columns SET is_fk = true, fk_reference = $1 WHERE table_id = $2 AND column_name = $3`,
        [fkRef, tableId, fk.from_col]
      );
      console.log(`  FK: ${fk.from_table}.${fk.from_col} → ${fk.to_table}.${fk.to_col}`);
    }
  }

  console.log(
    `✓ Schema "${schemaName}" loaded: ${stats.tables} tables, ${stats.rows.toLocaleString()} rows, ${stats.fk_count} FKs`
  );
  return stats;
}

/**
 * Profile a table: sample rows, compute value dictionaries, detect patterns
 * @param {number} appId - Application ID
 * @param {string} tableName - Table name in source data schema
 * @param {string[]} columns - Column names
 * @param {number} [sampleRowCount=10] - Number of sample rows to include in profile
 * @returns {Promise<object>} Profile data with samples and value dictionaries
 */
async function profileTable(appId, tableName, columns, sampleRowCount = 10) {
  const schemaName = `appdata_${appId}`;
  const fullTableName = `${schemaName}."${tableName}"`;

  // Get row count
  const countResult = await query(`SELECT COUNT(*) as cnt FROM ${fullTableName}`);
  const rowCount = parseInt(countResult.rows[0].cnt);

  // Sample rows deterministically (consistent ordering ensures enrichment reproducibility)
  // Use ctid as a stable row identifier — avoids ORDER BY RANDOM() which changes prompt on every run
  const limit = Math.max(5, Math.min(100, sampleRowCount || 10));
  const sampleResult = await query(
    `SELECT * FROM ${fullTableName} ORDER BY ctid LIMIT ${limit}`
  );
  const sampleRows = sampleResult.rows;

  // Per-column profiling
  const columnProfiles = {};
  for (const colName of columns) {
    const profile = {};

    // Distinct count and null rate
    const statsResult = await query(
      `SELECT
        COUNT(DISTINCT "${colName}") as distinct_count,
        COUNT(*) as total,
        SUM(CASE WHEN "${colName}" IS NULL THEN 1 ELSE 0 END) as null_count
       FROM ${fullTableName}`
    );
    const s = statsResult.rows[0];
    profile.distinct_count = parseInt(s.distinct_count);
    profile.null_count = parseInt(s.null_count);
    profile.null_rate = rowCount > 0 ? (parseInt(s.null_count) / rowCount) : 0;

    // Value dictionary (top 25 distinct values with counts) for low-cardinality columns
    if (profile.distinct_count <= 50) {
      const vdResult = await query(
        `SELECT "${colName}" as val, COUNT(*) as cnt
         FROM ${fullTableName}
         WHERE "${colName}" IS NOT NULL
         GROUP BY "${colName}"
         ORDER BY cnt DESC
         LIMIT 25`
      );
      profile.value_dictionary = {};
      vdResult.rows.forEach(r => {
        profile.value_dictionary[String(r.val)] = parseInt(r.cnt);
      });
    }

    // Sample distinct values (up to 15)
    const samplesResult = await query(
      `SELECT DISTINCT "${colName}" as val
       FROM ${fullTableName}
       WHERE "${colName}" IS NOT NULL
       ORDER BY "${colName}"
       LIMIT 15`
    );
    profile.sample_values = samplesResult.rows.map(r => r.val);

    // Numeric stats
    const numCheck = await query(
      `SELECT MIN("${colName}"::numeric) as min_val, MAX("${colName}"::numeric) as max_val,
              AVG("${colName}"::numeric) as avg_val
       FROM ${fullTableName}
       WHERE "${colName}" IS NOT NULL AND "${colName}" ~ '^-?[0-9]+(\\.[0-9]+)?$'`
    ).catch(() => null);

    if (numCheck && numCheck.rows[0] && numCheck.rows[0].min_val !== null) {
      profile.is_numeric = true;
      profile.min_value = parseFloat(numCheck.rows[0].min_val);
      profile.max_value = parseFloat(numCheck.rows[0].max_val);
      profile.avg_value = parseFloat(parseFloat(numCheck.rows[0].avg_val).toFixed(2));
    }

    columnProfiles[colName] = profile;
  }

  return {
    table_name: tableName,
    row_count: rowCount,
    sample_rows: sampleRows,
    column_profiles: columnProfiles,
  };
}

/**
 * Check if source data schema exists for an app
 */
async function hasSourceData(appId) {
  const schemaName = `appdata_${appId}`;
  const result = await query(
    `SELECT schema_name FROM information_schema.schemata WHERE schema_name = $1`,
    [schemaName]
  );
  return result.rows.length > 0;
}

/**
 * Execute a read-only query against the app's source data
 * @param {number} appId - Application ID
 * @param {string} sql - SQL query to execute
 * @param {object} [options] - Optional settings
 * @param {number} [options.maxRows=1000] - Max rows to return (default 1000 for UI safety;
 *   pipeline internals like Strategy 4 FK resolution need higher limits since FND_RESOLVED_FKS
 *   has 25K+ rows and our business tables may not appear in the first 1000 alphabetically)
 * @param {number} [options.timeout=60000] - Statement timeout in ms
 */
async function executeOnSourceData(appId, sql, options = {}) {
  const { maxRows = 1000, timeout = 60000 } = options;
  const schemaName = `appdata_${appId}`;

  // Set search_path so unqualified table names resolve to the app's schema
  const client = await pool.connect();
  try {
    await client.query(`SET search_path TO ${schemaName}, public`);
    await client.query(`SET statement_timeout = ${timeout}`); // configurable timeout

    const startTime = Date.now();
    const result = await client.query(sql);
    const duration_ms = Date.now() - startTime;

    return {
      rows: maxRows > 0 ? result.rows.slice(0, maxRows) : result.rows,
      row_count: result.rowCount,
      columns: result.fields ? result.fields.map(f => f.name) : [],
      duration_ms,
    };
  } finally {
    client.release();
  }
}

/**
 * Browse table data with pagination
 */
async function browseTable(appId, tableName, { limit = 50, offset = 0 } = {}) {
  const schemaName = `appdata_${appId}`;
  const fullTableName = `${schemaName}."${tableName}"`;

  const countResult = await query(`SELECT COUNT(*) as cnt FROM ${fullTableName}`);
  const totalRows = parseInt(countResult.rows[0].cnt);

  const dataResult = await query(
    `SELECT * FROM ${fullTableName} LIMIT $1 OFFSET $2`,
    [limit, offset]
  );

  return {
    table_name: tableName,
    total_rows: totalRows,
    offset,
    limit,
    rows: dataResult.rows,
    columns: dataResult.fields ? dataResult.fields.map(f => f.name) : [],
  };
}

/**
 * Sync app_columns with the actual source data schema columns.
 * Reads column names and types from the PostgreSQL information_schema and
 * creates/updates app_columns entries to match the real source data.
 * This ensures the BOKG context references the correct column names.
 */
async function syncColumnsFromSource(appId) {
  const schemaName = `appdata_${appId}`;

  // Get all tables in the source schema
  const srcTables = await query(
    `SELECT table_name FROM information_schema.tables
     WHERE table_schema = $1 AND table_type = 'BASE TABLE'
     ORDER BY table_name`,
    [schemaName]
  );

  // Get app_tables for this app
  const appTables = await query(
    'SELECT id, table_name FROM app_tables WHERE app_id = $1',
    [appId]
  );
  const appTableMap = {};
  for (const t of appTables.rows) {
    appTableMap[t.table_name.toLowerCase()] = t;
  }

  let synced = 0;
  let created = 0;

  for (const srcTable of srcTables.rows) {
    const tableName = srcTable.table_name;
    const appTable = appTableMap[tableName.toLowerCase()];

    if (!appTable) {
      // Auto-create app_tables entry for source tables not yet registered
      // (needed for enterprise apps like OEBS/SAP where seed only creates the application record)
      const insertResult = await query(
        `INSERT INTO app_tables (app_id, table_name, entity_name, description, enrichment_status)
         VALUES ($1, $2, $3, '', 'draft')
         RETURNING id, table_name`,
        [appId, tableName, tableName]
      );
      const newTable = insertResult.rows[0];
      appTableMap[tableName.toLowerCase()] = newTable;
      console.log(`  syncColumns: auto-created app_tables entry for "${tableName}" (id: ${newTable.id})`);
      // Fall through to column sync below using the newly created entry
    }
    // Re-fetch in case we just created it
    const syncTable = appTableMap[tableName.toLowerCase()];

    // Get actual columns from source schema
    const srcCols = await query(
      `SELECT column_name, data_type, ordinal_position
       FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = $2
       ORDER BY ordinal_position`,
      [schemaName, tableName]
    );

    // Get existing app_columns for this table
    const existingCols = await query(
      'SELECT id, column_name FROM app_columns WHERE table_id = $1',
      [syncTable.id]
    );
    const existingMap = {};
    for (const c of existingCols.rows) {
      existingMap[c.column_name.toLowerCase()] = c;
    }

    // Track which app_column IDs have been matched/handled
    const handledIds = new Set();

    for (const srcCol of srcCols.rows) {
      const colName = srcCol.column_name;
      const dataType = srcCol.data_type.toUpperCase();

      // Check if this exact column name already exists
      const existing = existingMap[colName.toLowerCase()];

      if (existing) {
        handledIds.add(existing.id);
        // Column exists — update name if case differs (e.g., seed had camelCase, source has snake_case)
        if (existing.column_name !== colName) {
          await query(
            'UPDATE app_columns SET column_name = $1, data_type = $2 WHERE id = $3',
            [colName, dataType, existing.id]
          );
          console.log(`  syncColumns: renamed "${existing.column_name}" → "${colName}" in ${tableName}`);
          synced++;
        }
      } else {
        // Check if there's a camelCase variant that maps to this snake_case name
        // e.g., seed has "accountId" but source has "account_id"
        const camelVariant = colName.replace(/_([a-z])/g, (_, c) => c.toUpperCase());
        const existingCamel = existingMap[camelVariant.toLowerCase()];

        if (existingCamel) {
          handledIds.add(existingCamel.id);
          // Rename the camelCase entry to match actual source column name
          await query(
            'UPDATE app_columns SET column_name = $1, data_type = $2 WHERE id = $3',
            [colName, dataType, existingCamel.id]
          );
          console.log(`  syncColumns: renamed "${existingCamel.column_name}" → "${colName}" in ${tableName}`);
          synced++;
        } else {
          // Column not in app_columns at all — create it
          // Detect if it's likely a PK (first column or named *_id matching table name)
          const isPK = colName.toLowerCase() === `${tableName.toLowerCase()}_id`;
          await query(
            `INSERT INTO app_columns (table_id, column_name, data_type, is_pk, enrichment_status)
             VALUES ($1, $2, $3, $4, 'draft')`,
            [syncTable.id, colName, dataType, isPK]
          );
          console.log(`  syncColumns: added missing column "${colName}" to ${tableName}`);
          created++;
        }
      }
    }

    // Remove app_columns that weren't matched to any source column
    // (These are stale entries from seed data that don't exist in the actual source)
    for (const [name, existing] of Object.entries(existingMap)) {
      if (!handledIds.has(existing.id)) {
        await query('DELETE FROM app_columns WHERE id = $1', [existing.id]);
        console.log(`  syncColumns: removed stale column "${existing.column_name}" from ${tableName}`);
      }
    }
  }

  console.log(`  syncColumns complete: ${synced} renamed, ${created} created`);
  return { synced, created };
}

/**
 * Drop source data schema for an app (used during reset)
 */
async function dropSourceData(appId) {
  const schemaName = `appdata_${appId}`;
  await query(`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`);
}

module.exports = {
  loadSqliteToPostgres,
  profileTable,
  hasSourceData,
  executeOnSourceData,
  browseTable,
  syncColumnsFromSource,
  dropSourceData,
};
