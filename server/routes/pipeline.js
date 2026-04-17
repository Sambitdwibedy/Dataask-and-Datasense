const express = require('express');
const { query } = require('../db');
const { enrichTable } = require('../services/llm-service');
const { loadSqliteToPostgres, profileTable, hasSourceData, dropSourceData, syncColumnsFromSource } = require('../services/data-loader');
const { buildBOKGContext, loadValueDictionaries, generateAndExecuteWithRetry } = require('./query-engine');
const path = require('path');
const fs = require('fs');

const router = express.Router();

// In-memory store for active pipeline progress (keyed by run ID)
const activeRuns = {};

// Custom error for pipeline cancellation (distinct from real failures)
class PipelineCancelled extends Error {
  constructor(message = 'Pipeline cancelled by user') {
    super(message);
    this.name = 'PipelineCancelled';
  }
}

// Custom error for gate pause (not a real error — pipeline is waiting)
class PipelineGatePause extends Error {
  constructor(stageName) {
    super(`Pipeline paused at gate: ${stageName}`);
    this.name = 'PipelineGatePause';
    this.stage = stageName;
  }
}

/**
 * Check if the current pipeline run has been cancelled.
 * Call this between stages and between individual table enrichments.
 * Throws PipelineCancelled if the user requested cancellation.
 */
function checkCancelled(runId) {
  const progress = activeRuns[runId];
  if (progress && progress.cancelled) {
    throw new PipelineCancelled();
  }
}

/**
 * Persist a snapshot of pipeline progress to the database.
 * Called periodically during long-running stages so progress survives container restarts.
 */
async function snapshotProgress(runId) {
  const progress = activeRuns[runId];
  if (!progress) return;
  try {
    await query(
      `UPDATE pipeline_runs SET stages = $1, progress_snapshot = $2 WHERE id = $3`,
      [JSON.stringify(progress.stages), JSON.stringify({
        tables_total: progress.tables_total,
        tables_done: progress.tables_done,
        columns_total: progress.columns_total,
        columns_done: progress.columns_done,
        current_table: progress.current_table,
        current_stage: progress.current_stage,
        errors: (progress.errors || []).slice(-20), // keep last 20 errors
        token_usage: progress.token_usage,
        workers: progress.workers,
        concurrency: progress.concurrency,
        metadata_discovery: progress.metadata_discovery,
        qpd_results: progress.qpd_results,
        enrich_table_times: (progress.enrich_table_times || []).slice(-50),
        enrich_start_time: progress.enrich_start_time,
      }), runId]
    );
  } catch (err) {
    console.warn(`[Snapshot] Failed to persist progress for run ${runId}:`, err.message);
  }
}

// Stages that require user approval before proceeding (gated stages)
const GATED_STAGES = ['enrich', 'validate'];

/**
 * Wait for user approval at a gate. Sets the stage to 'awaiting_approval'
 * and returns a Promise that resolves when the user approves or skips.
 * @returns {string} 'approve' or 'skip'
 */
function waitForGateApproval(runId, stageName, gateSummary) {
  const progress = activeRuns[runId];
  if (!progress) throw new Error('Run not found');

  return new Promise((resolve, reject) => {
    progress.pendingGate = {
      stage: stageName,
      summary: gateSummary,
      resolve,
      reject,
      created_at: new Date().toISOString(),
    };
  });
}

// Known SQLite paths for BIRD benchmark databases
const BIRD_SQLITE_PATHS = {
  'financial': 'BIRD Benchmark/dev_databases/financial/financial.sqlite',
  'california_schools': 'BIRD Benchmark/dev_databases/california_schools/california_schools.sqlite',
  'card_games': 'BIRD Benchmark/dev_databases/card_games/card_games.sqlite',
  'codebase_community': 'BIRD Benchmark/dev_databases/codebase_community/codebase_community.sqlite',
  'debit_card_specializing': 'BIRD Benchmark/dev_databases/debit_card_specializing/debit_card_specializing.sqlite',
  'european_football_2': 'BIRD Benchmark/dev_databases/european_football_2/european_football_2.sqlite',
  'formula_1': 'BIRD Benchmark/dev_databases/formula_1/formula_1.sqlite',
  'student_club': 'BIRD Benchmark/dev_databases/student_club/student_club.sqlite',
  'superhero': 'BIRD Benchmark/dev_databases/superhero/superhero.sqlite',
  'thrombosis_prediction': 'BIRD Benchmark/dev_databases/thrombosis_prediction/thrombosis_prediction.sqlite',
  'toxicology': 'BIRD Benchmark/dev_databases/toxicology/toxicology.sqlite',
  'ofbiz': 'OFBiz Schema/ofbiz_test.db',
  'oebs': 'OEBS Object Model/oebs_test.db',
  'sap': 'SAP ECC Semantic Model/sap_test.db',
};

// Direct app type → SQLite key mapping for enterprise apps whose names don't
// fuzzy-match the short keys (e.g. "Oracle EBS" doesn't contain substring "oebs")
const TYPE_TO_SQLITE_KEY = {
  'oracle ebs': 'oebs',
  'oracle_ebs': 'oebs',
  'sap ecc': 'sap',
  'sap_ecc': 'sap',
  'ofbiz': 'ofbiz',
};

/**
 * Auto-decompress a .gz file to produce the uncompressed version.
 * Returns the path to the decompressed file, or null on failure.
 */
function decompressGz(gzPath) {
  try {
    const zlib = require('zlib');
    // Try writing next to the .gz first, fall back to /tmp if filesystem is read-only
    const outPath = gzPath.replace(/\.gz$/, '');
    if (fs.existsSync(outPath)) return outPath;  // Already decompressed
    const tmpPath = path.join('/tmp', path.basename(gzPath).replace(/\.gz$/, ''));
    if (fs.existsSync(tmpPath)) return tmpPath;  // Already decompressed to /tmp
    console.log(`  Decompressing ${path.basename(gzPath)}...`);
    const compressed = fs.readFileSync(gzPath);
    const decompressed = zlib.gunzipSync(compressed);
    // Try in-place first, then /tmp
    try {
      fs.writeFileSync(outPath, decompressed);
      console.log(`  Decompressed: ${outPath} (${(decompressed.length / 1024 / 1024).toFixed(1)} MB)`);
      return outPath;
    } catch (writeErr) {
      console.warn(`  Cannot write to ${outPath} (${writeErr.code}), falling back to /tmp`);
      fs.writeFileSync(tmpPath, decompressed);
      console.log(`  Decompressed to /tmp: ${tmpPath} (${(decompressed.length / 1024 / 1024).toFixed(1)} MB)`);
      return tmpPath;
    }
  } catch (err) {
    console.warn(`  Failed to decompress ${gzPath}: ${err.message}`);
    return null;
  }
}

/**
 * Check for a SQLite file at basePath, trying both uncompressed and .gz variants.
 * If only .gz exists, auto-decompresses it. Returns the path or null.
 */
function findSqliteFile(basePath) {
  if (fs.existsSync(basePath)) return basePath;
  const gzPath = basePath + '.gz';
  if (fs.existsSync(gzPath)) return decompressGz(gzPath);
  return null;
}

function findSqlitePath(app) {
  // Check config for explicit path
  const config = app.config || {};
  if (config.sqlite_path && fs.existsSync(config.sqlite_path)) {
    return config.sqlite_path;
  }

  const appName = (app.name || '').toLowerCase();
  const appType = (app.type || '').toLowerCase();

  // __dirname is /app/server/routes on Railway, or .../app/server/routes locally
  // deployRoot should be the project root (where data/ and BIRD Benchmark/ live)
  const routesDir = __dirname;                          // /app/server/routes
  const serverDir = path.resolve(routesDir, '..');      // /app/server
  const appDir = path.resolve(serverDir, '..');         // /app  (project root on Railway)
  const workspaceDir = path.resolve(appDir, '..');      // parent of app/ (local workspace)

  // Strategy 1: Direct type-to-key mapping (most reliable for enterprise apps)
  const directKey = TYPE_TO_SQLITE_KEY[appType];
  if (directKey && BIRD_SQLITE_PATHS[directKey]) {
    const relPath = BIRD_SQLITE_PATHS[directKey];
    const dataPath = findSqliteFile(path.join(appDir, 'data', `${directKey}.sqlite`));
    if (dataPath) {
      console.log(`Found SQLite via type mapping "${appType}" → "${directKey}": ${dataPath}`);
      return dataPath;
    }
    const birdPath = findSqliteFile(path.join(appDir, relPath));
    if (birdPath) {
      console.log(`Found SQLite via type mapping at BIRD path: ${birdPath}`);
      return birdPath;
    }
    const wsPath = findSqliteFile(path.join(workspaceDir, relPath));
    if (wsPath) {
      console.log(`Found SQLite via type mapping at workspace path: ${wsPath}`);
      return wsPath;
    }
  }

  // Strategy 2: Fuzzy name/type substring matching (works for BIRD benchmark DBs)
  for (const [key, relPath] of Object.entries(BIRD_SQLITE_PATHS)) {
    // Normalize: strip spaces, underscores, and hyphens so "California Schools" matches "california_schools"
    // and "E-Business" doesn't break matching
    const normalizedKey = key.replace(/[_\-\s]/g, '');
    const normalizedName = appName.replace(/[_\-\s]/g, '');
    const normalizedType = appType.replace(/[_\-\s]/g, '');
    if (normalizedName.includes(normalizedKey) || normalizedType.includes(normalizedKey)) {
      // Check 1: data/ folder in project root (Railway Docker deployment)
      const dataPath = findSqliteFile(path.join(appDir, 'data', `${key}.sqlite`));
      if (dataPath) {
        console.log(`Found SQLite at deployed path: ${dataPath}`);
        return dataPath;
      }

      // Check 2: BIRD benchmark paths relative to project root
      const birdPath = findSqliteFile(path.join(appDir, relPath));
      if (birdPath) {
        console.log(`Found SQLite at BIRD path: ${birdPath}`);
        return birdPath;
      }

      // Check 3: BIRD benchmark paths relative to workspace (local dev)
      const wsPath = findSqliteFile(path.join(workspaceDir, relPath));
      if (wsPath) {
        console.log(`Found SQLite at workspace path: ${wsPath}`);
        return wsPath;
      }
    }
  }

  console.warn(`No SQLite file found for app "${app.name}"`);
  console.warn(`  Searched: ${path.join(appDir, 'data')}, ${appDir}, ${workspaceDir}`);
  return null;
}

// POST /api/pipeline/:appId/run - trigger real pipeline enrichment
router.post('/:appId/run', async (req, res) => {
  try {
    const { appId } = req.params;

    const appCheck = await query(
      'SELECT id, name, type, status, config FROM applications WHERE id = $1',
      [appId]
    );
    if (appCheck.rows.length === 0) {
      return res.status(404).json({ error: 'Application not found' });
    }

    const app = appCheck.rows[0];

    // Check if already running
    const runningCheck = await query(
      "SELECT id FROM pipeline_runs WHERE app_id = $1 AND status = 'running'",
      [appId]
    );
    if (runningCheck.rows.length > 0) {
      return res.status(409).json({ error: 'Pipeline already running for this application' });
    }

    // Create pipeline run
    const stages = {
      ingest: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
      profile: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
      infer: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
      context: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
      enrich: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
      review: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
      validate: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
      publish: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
    };

    const result = await query(
      `INSERT INTO pipeline_runs (app_id, triggered_by, status, started_at, stages)
       VALUES ($1, $2, $3, NOW(), $4)
       RETURNING *`,
      [appId, req.user.id, 'running', JSON.stringify(stages)]
    );

    const run = result.rows[0];

    await query("UPDATE applications SET status = 'enriching', updated_at = NOW() WHERE id = $1", [appId]);

    activeRuns[run.id] = {
      stages,
      tables_total: 0,
      tables_done: 0,
      columns_total: 0,
      columns_done: 0,
      current_table: null,
      current_stage: 'ingest',
      errors: [],
      enrich_start_time: null,
      enrich_table_times: [],  // track per-table durations for ETA
      token_usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 },
      workers: [],  // per-worker status: [{ id, status, current_table, tables_done }]
      concurrency: 1,  // will be set when enrichment starts
      cancelled: false,  // set to true when user cancels
      pendingGate: null,  // set when pipeline pauses for approval
    };

    res.status(201).json({ run });

    // Background enrichment
    runEnrichmentPipeline(run.id, appId, app).catch(err => {
      console.error(`Pipeline run ${run.id} failed:`, err);
    });
  } catch (err) {
    console.error('Pipeline run error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ============================================================
// BACKGROUND PIPELINE — the real enrichment logic
// ============================================================
async function runEnrichmentPipeline(runId, appId, app) {
  const progress = activeRuns[runId];

  try {
    // --- STAGE 1: INGEST (load source data into PG) ---
    await updateStage(runId, 'ingest', 'running');
    progress.current_stage = 'ingest';

    const sqlitePath = findSqlitePath(app);
    let sourceDataAvailable = await hasSourceData(appId);

    if (sqlitePath && !sourceDataAvailable) {
      console.log(`Loading source data from: ${sqlitePath}`);
      await loadSqliteToPostgres(appId, sqlitePath, { dropExisting: true });
      sourceDataAvailable = true;
    } else if (sourceDataAvailable) {
      console.log('Source data already loaded, skipping ingest');
    } else {
      console.log('No source data file found, will enrich from metadata only');
    }

    await updateStage(runId, 'ingest', 'completed');
    checkCancelled(runId);
    await snapshotProgress(runId);

    // --- STAGE 1.5: SYNC COLUMNS (align app_columns with actual source schema) ---
    if (sourceDataAvailable) {
      console.log('Syncing app_columns with source data schema...');
      try {
        await syncColumnsFromSource(appId);
      } catch (syncErr) {
        console.warn('Column sync warning:', syncErr.message);
      }
    }

    // --- STAGE 2: PROFILE (sample rows, compute value dictionaries) ---
    await updateStage(runId, 'profile', 'running');
    progress.current_stage = 'profile';

    const tablesResult = await query(
      'SELECT id, table_name, entity_name FROM app_tables WHERE app_id = $1 ORDER BY table_name',
      [appId]
    );
    const tables = tablesResult.rows;

    // Guard: if no tables exist after ingest, something went wrong — fail loudly
    if (tables.length === 0) {
      const errMsg = `No tables found for app "${app.name}" (ID: ${appId}) after ingest. ` +
        `SQLite path resolved: ${findSqlitePath(app) || 'NONE'}. ` +
        `Source data available: ${sourceDataAvailable}. ` +
        `Check that the SQLite file exists and contains tables.`;
      console.error(`Pipeline ABORT: ${errMsg}`);
      throw new Error(errMsg);
    }

    const allTableNames = tables.map(t => t.entity_name || t.table_name);

    // Count draft columns
    const colCountResult = await query(
      `SELECT COUNT(*) as cnt FROM app_columns ac
       JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = $1 AND ac.enrichment_status = 'draft'`,
      [appId]
    );
    progress.tables_total = tables.length;
    progress.columns_total = parseInt(colCountResult.rows[0].cnt);

    // Profile each table (sample rows, value dictionaries)
    const tableProfiles = {};
    const pipelineConfig = (app.config || {}).pipeline || {};
    const sampleRowCount = pipelineConfig.sample_row_count || 10;
    if (sourceDataAvailable) {
      for (const table of tables) {
        try {
          const colNames = (await query(
            'SELECT column_name FROM app_columns WHERE table_id = $1',
            [table.id]
          )).rows.map(r => r.column_name);

          const profile = await profileTable(appId, table.table_name, colNames, sampleRowCount);
          tableProfiles[table.id] = profile;

          // Update row count in app_tables
          await query('UPDATE app_tables SET row_count = $1 WHERE id = $2', [profile.row_count, table.id]);

          console.log(`  Profiled ${table.table_name}: ${profile.row_count.toLocaleString()} rows`);
        } catch (err) {
          console.warn(`  Failed to profile ${table.table_name}:`, err.message);
        }
      }
    }

    await updateStage(runId, 'profile', 'completed');
    checkCancelled(runId);
    await snapshotProgress(runId);

    // --- STAGE 3: DISCOVER (gather columns, detect relationships, seed patterns) ---
    await updateStage(runId, 'infer', 'running');
    progress.current_stage = 'infer';

    const tableColumnsMap = {};
    for (const table of tables) {
      const colsResult = await query(
        `SELECT id, column_name, data_type, is_pk, is_fk, fk_reference
         FROM app_columns
         WHERE table_id = $1 AND enrichment_status = 'draft'
         ORDER BY column_name`,
        [table.id]
      );
      if (colsResult.rows.length > 0) {
        tableColumnsMap[table.id] = { table, columns: colsResult.rows };
      }
    }

    // Detect and store FK relationships in app_relationships table
    // Uses two strategies: (1) explicit is_fk flag, (2) naming convention detection
    console.log('Discovering relationships...');
    await query('DELETE FROM app_relationships WHERE app_id = $1', [appId]);

    // Build lookup: table_name → { table, pkColumns }
    const tableLookup = {};
    for (const table of tables) {
      const pkCols = await query(
        `SELECT column_name FROM app_columns WHERE table_id = $1 AND is_pk = true`,
        [table.id]
      );
      tableLookup[table.table_name.toLowerCase()] = {
        table,
        pkColumns: pkCols.rows.map(r => r.column_name)
      };
    }

    for (const table of tables) {
      // Get ALL columns for this table (not just is_fk=true)
      const allCols = await query(
        `SELECT id, column_name, is_fk, fk_reference FROM app_columns WHERE table_id = $1`,
        [table.id]
      );

      for (const col of allCols.rows) {
        let targetEntry = null;
        let toColumn = null;

        // Strategy 1: Explicit FK flag
        if (col.is_fk) {
          const baseName = col.column_name.replace(/_id$/i, '').replace(/Id$/, '');
          const fkRef = col.fk_reference || '';
          targetEntry = tableLookup[baseName.toLowerCase()]
            || tableLookup[baseName.toLowerCase() + 's']
            || Object.values(tableLookup).find(e => fkRef.toLowerCase().includes(e.table.table_name.toLowerCase()));
        }

        // Strategy 2: Naming convention — column named {tableName}Id or {tableName}_id
        // where tableName matches another table, and this column is NOT the PK of its own table
        if (!targetEntry) {
          const colName = col.column_name;

          // Check camelCase pattern: accountId, districtId, clientId, dispId
          const camelMatch = colName.match(/^([a-z]+)Id$/);
          // Check snake_case pattern: account_id, district_id
          const snakeMatch = colName.match(/^([a-z_]+)_id$/i);

          const baseName = camelMatch ? camelMatch[1] : (snakeMatch ? snakeMatch[1] : null);

          if (baseName) {
            // Don't match if this column is the SOLE PK of its own table AND matches the table name
            // (e.g., account.accountId is the identity PK, not an FK)
            // But DO match composite PK members that reference other tables
            // (e.g., trans.accountId is part of a composite PK but is still an FK to account)
            const ownPKs = tableLookup[table.table_name.toLowerCase()]?.pkColumns || [];
            const isOwnPK = ownPKs.includes(colName);
            const isSolePK = isOwnPK && ownPKs.length === 1;
            const matchesOwnTable = baseName.toLowerCase() === table.table_name.toLowerCase();
            const skipAsIdentityPK = isSolePK || (isOwnPK && matchesOwnTable);

            if (!skipAsIdentityPK) {
              targetEntry = tableLookup[baseName.toLowerCase()]
                || tableLookup[baseName.toLowerCase() + 's'];
            }
          }
        }

        if (targetEntry && targetEntry.table.id !== table.id) {
          toColumn = targetEntry.pkColumns[0] || col.column_name;

          try {
            await query(
              `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING`,
              [appId, table.id, col.column_name, targetEntry.table.id, toColumn, 'fk', 'many_to_one']
            );
            console.log(`  Relationship: ${table.table_name}.${col.column_name} → ${targetEntry.table.table_name}.${toColumn}`);

            // Also mark this column as FK if it wasn't already
            if (!col.is_fk) {
              await query('UPDATE app_columns SET is_fk = true WHERE id = $1', [col.id]);
            }
          } catch (relErr) {
            console.warn(`  Failed to store relationship: ${relErr.message}`);
          }
        }
      }
    }

    // Strategy 3: Shared column name matching
    // If two tables share a column with the exact same name, and that column is a PK in one of them,
    // create a relationship. Handles PK-to-PK joins (CDSCode in California Schools, etc.)
    // where child tables use the parent's PK as their own PK.
    console.log('  Strategy 3: shared column name detection...');
    const allColsByName = {}; // colName -> [{ table, col, isPK, colCount }]
    const tableColCounts = {}; // tableId -> column count
    for (const table of tables) {
      const cols = await query(
        `SELECT id, column_name, is_pk, is_fk FROM app_columns WHERE table_id = $1`,
        [table.id]
      );
      tableColCounts[table.id] = cols.rows.length;
      for (const col of cols.rows) {
        const key = col.column_name.toLowerCase();
        if (!allColsByName[key]) allColsByName[key] = [];
        allColsByName[key].push({ table, col, isPK: col.is_pk, isFK: col.is_fk });
      }
    }

    for (const [colName, entries] of Object.entries(allColsByName)) {
      if (entries.length < 2) continue; // Column only in one table
      const pkEntries = entries.filter(e => e.isPK);
      if (pkEntries.length === 0) continue; // No PK holder — can't determine direction

      // Pick the parent: use widest table (most columns) as a rough heuristic.
      // This is imperfect — the real authority is declared FKs (Strategy 1).
      // Strategy 3 is a fallback for schemas without FK declarations.
      // The enrichment stage can later correct relationship direction if needed.
      const pkEntry = pkEntries.reduce((best, e) =>
        (tableColCounts[e.table.id] || 0) > (tableColCounts[best.table.id] || 0) ? e : best
      );

      for (const entry of entries) {
        if (entry.table.id === pkEntry.table.id) continue; // Don't self-reference
        // Allow PK-to-PK joins — common in government/education DBs where child tables
        // use the parent's natural key as their own PK (e.g., frpm.CDSCode → schools.CDSCode)
        const cardinality = entry.isPK ? 'one_to_one' : 'many_to_one';

        try {
          await query(
            `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING`,
            [appId, entry.table.id, entry.col.column_name, pkEntry.table.id, pkEntry.col.column_name, 'fk', cardinality]
          );
          console.log(`  Shared-name: ${entry.table.table_name}.${entry.col.column_name} → ${pkEntry.table.table_name}.${pkEntry.col.column_name} (${cardinality})`);

          if (!entry.isFK) {
            await query('UPDATE app_columns SET is_fk = true WHERE id = $1', [entry.col.id]);
          }
        } catch (relErr) { /* ignore duplicates */ }
      }
    }

    // Strategy 3b: Shared column matching for schemas WITHOUT declared PKs
    // Many real-world schemas (SAP exports, data warehouses, flat-file imports) have no PK declarations.
    // For columns shared across multiple tables, find the "anchor" table where the column's values
    // are most unique (highest DISTINCT/COUNT ratio), then create relationships from other tables
    // to the anchor, confirmed by value overlap sampling.
    // Skips ubiquitous columns (>50% of tables, e.g. SAP MANDT) and generic metadata columns.
    console.log('  Strategy 3b: shared column matching (no-PK schemas, value-overlap confirmed)...');
    try {
      const sourceAvail3b = await hasSourceData(appId);
      if (!sourceAvail3b) {
        console.log('  Strategy 3b skipped — no source data available');
      } else {
        const { executeOnSourceData: execSrc3b } = require('../services/data-loader');
        const totalTableCount = tables.length;
        const ubiquityThreshold = Math.max(5, Math.floor(totalTableCount * 0.5));

        // Generic/metadata columns to skip — not meaningful FK relationships
        const skipColumns = new Set([
          'mandt',   // SAP client key — partition column, not FK
          'id', 'name', 'description', 'status', 'type',
          'created_at', 'updated_at', 'created_by', 'modified_by',
          'last_update_date', 'creation_date', 'last_updated_by',
          'record_id', 'row_id', 'seq', 'sequence', 'sort_order',
          'is_active', 'is_deleted', 'flag', 'comments', 'notes',
          'remark', 'remarks',
          'erdat', 'ernam', 'aedat', 'aenam', // SAP: created/changed date/by
          'loekz', 'spras',                     // SAP: deletion flag, language key
          'budat', 'bldat', 'cpudt', 'cputm',  // SAP: posting/document/entry dates/times
          'usnam', 'tcode',                      // SAP: username, transaction code
        ]);

        // Get existing relationships to avoid duplicates
        const existRels3b = await query(
          `SELECT from_table_id, from_column, to_table_id, to_column FROM app_relationships WHERE app_id = $1`,
          [appId]
        );
        const existRelSet3b = new Set(
          existRels3b.rows.map(r => `${r.from_table_id}:${r.from_column}:${r.to_table_id}:${r.to_column}`)
        );

        // Pre-fetch row counts from app_tables (tables query may not include row_count)
        const rowCounts3b = {};
        const rcResult = await query('SELECT id, row_count FROM app_tables WHERE app_id = $1', [appId]);
        for (const r of rcResult.rows) {
          rowCounts3b[r.id] = parseInt(r.row_count || 0);
        }

        let s3bCount = 0;
        let s3bChecked = 0;

        for (const [colName, entries] of Object.entries(allColsByName)) {
          // Skip if Strategy 3 already handled (any entry is PK)
          if (entries.some(e => e.isPK)) continue;
          // Skip ubiquitous, generic, or too-short columns
          if (entries.length < 2 || entries.length > ubiquityThreshold) continue;
          if (skipColumns.has(colName)) continue;
          if (colName.length < 3) continue;

          // Only tables with actual data
          const entriesWithData = entries.filter(e => (rowCounts3b[e.table.id] || 0) > 0);
          if (entriesWithData.length < 2) continue;

          s3bChecked++;

          // Find the "anchor" (reference/master) table: highest DISTINCT(col)/COUNT(*) ratio.
          // Tie-break: prefer the table with MORE distinct values (most complete reference set).
          // In SAP, LFA1 (23 vendors) should beat LFM1 (3 vendors); KNA1 (32 customers) beats KNB1 (9).
          let bestRatio = 0;
          let anchorEntry = null;

          for (const entry of entriesWithData) {
            try {
              const distinctResult = await execSrc3b(appId,
                `SELECT COUNT(DISTINCT "${entry.col.column_name}") as dcnt, COUNT(*) as tcnt
                 FROM "${entry.table.table_name}"
                 WHERE "${entry.col.column_name}" IS NOT NULL`,
                { timeout: 5000 }
              );
              const dcnt = parseInt(distinctResult.rows[0]?.dcnt || 0);
              const tcnt = parseInt(distinctResult.rows[0]?.tcnt || 0);
              if (tcnt === 0 || dcnt === 0) continue;

              const ratio = dcnt / tcnt;
              // Prefer highest uniqueness ratio; break ties by MORE distinct values
              // (larger reference table = more complete master data)
              if (ratio > bestRatio || (ratio === bestRatio && anchorEntry && dcnt > anchorEntry.distinctCnt)) {
                bestRatio = ratio;
                anchorEntry = { ...entry, distinctCnt: dcnt, totalCnt: tcnt };
              }
            } catch (e) { /* skip on error */ }
          }

          // Anchor must have reasonably unique values (≥50% distinct)
          if (!anchorEntry || bestRatio < 0.5) continue;

          // Create relationships from other tables to the anchor, confirmed by value overlap
          for (const entry of entriesWithData) {
            if (entry.table.id === anchorEntry.table.id) continue;

            const relKey = `${entry.table.id}:${entry.col.column_name}:${anchorEntry.table.id}:${anchorEntry.col.column_name}`;
            const reverseKey = `${anchorEntry.table.id}:${anchorEntry.col.column_name}:${entry.table.id}:${entry.col.column_name}`;
            if (existRelSet3b.has(relKey) || existRelSet3b.has(reverseKey)) continue;

            try {
              // Sample distinct values from the child table
              const childSample = await execSrc3b(appId,
                `SELECT DISTINCT "${entry.col.column_name}" as val FROM "${entry.table.table_name}"
                 WHERE "${entry.col.column_name}" IS NOT NULL LIMIT 50`,
                { timeout: 5000 }
              );
              if (childSample.rows.length === 0) continue;

              const sampleValues = childSample.rows.map(r => r.val);
              const escaped = sampleValues.map(v =>
                v === null ? 'NULL' : `'${String(v).replace(/'/g, "''")}'`
              ).join(', ');

              // Check how many child values exist in the anchor table
              const anchorMatch = await execSrc3b(appId,
                `SELECT COUNT(DISTINCT "${anchorEntry.col.column_name}") as cnt
                 FROM "${anchorEntry.table.table_name}"
                 WHERE "${anchorEntry.col.column_name}" IN (${escaped})`,
                { timeout: 5000 }
              );

              const matchCount = parseInt(anchorMatch.rows[0]?.cnt || 0);
              const overlapRatio = matchCount / sampleValues.length;

              // Require ≥50% value overlap. For reference tables with few distinct values
              // (e.g., SAP T001 with 1 company code), allow matchCount of 1 if sample is small.
              const minMatches = Math.min(2, sampleValues.length);
              if (overlapRatio >= 0.5 && matchCount >= minMatches) {
                const confidence = Math.min(85, Math.round(overlapRatio * 100));
                await query(
                  `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality, confidence_score)
                   VALUES ($1, $2, $3, $4, $5, 'inferred', 'many_to_one', $6)
                   ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING`,
                  [appId, entry.table.id, entry.col.column_name, anchorEntry.table.id, anchorEntry.col.column_name, confidence]
                );
                existRelSet3b.add(relKey);
                s3bCount++;
                console.log(`    Shared-col: ${entry.table.table_name}.${entry.col.column_name} → ${anchorEntry.table.table_name}.${anchorEntry.col.column_name} (${Math.round(overlapRatio * 100)}% overlap, uniqueness=${bestRatio.toFixed(2)})`);
              }
            } catch (overlapErr) { /* skip on error */ }
          }
        }
        console.log(`  Strategy 3b: checked ${s3bChecked} shared columns, found ${s3bCount} new relationships`);
      }
    } catch (s3bErr) {
      console.warn(`  Strategy 3b warning: ${s3bErr.message}`);
    }

    // Strategy 4: Metadata-driven FK resolution (application-agnostic)
    // Discovers FK relationships from application metadata/data-dictionary tables.
    // Supports Oracle EBS (FND_FOREIGN_KEYS/FND_RESOLVED_FKS), SAP (DD05S, DD08L),
    // PeopleSoft (PSRECFIELDDB), and generic from_table/to_table patterns.
    // Requires source data schema to exist — skip entirely if not available.
    console.log('  Strategy 4: metadata-driven FK resolution...');
    try {
      const { executeOnSourceData, hasSourceData: hasSourceCheck } = require('../services/data-loader');
      const s4SourceAvail = await hasSourceCheck(appId);
      if (!s4SourceAvail) {
        console.log('  Strategy 4 skipped — no source data schema available');
        throw { message: 'skipped', skipped: true };
      }
      const allTableNamesLower = allTableNames.map(n => n.toLowerCase());

      // Application-agnostic FK metadata table patterns
      // Each entry defines a table that stores FK relationship definitions and its column mapping
      const FK_METADATA_PATTERNS = [
        // Oracle EBS — pre-computed denormalized FK table (fast path)
        { table: 'fnd_resolved_fks', type: 'oracle_resolved',
          fromTable: 'FROM_TABLE', fromCol: 'FROM_COLUMN', toTable: 'TO_TABLE', toCol: 'TO_COLUMN',
          filter: `COALESCE("ENABLED_FLAG", 'Y') != 'N'` },
        // Oracle EBS — raw FK tables (needs complex JOIN, handled as special case below)
        { table: 'fnd_foreign_keys', type: 'oracle_raw' },
        // SAP — Foreign key field assignments
        { table: 'dd05s', type: 'sap',
          fromTable: 'TABNAME', fromCol: 'FIELDNAME', toTable: 'CHECKTABLE', toCol: 'CHECKFIELD' },
        // SAP — Table FK relationships
        { table: 'dd08l', type: 'sap',
          fromTable: 'TABNAME', fromCol: 'FIELDNAME', toTable: 'CHECKTABLE', toCol: 'CHECKFIELD' },
        // PeopleSoft — Record field DB overrides with references
        { table: 'psrecfielddb', type: 'peoplesoft',
          fromTable: 'RECNAME', fromCol: 'FIELDNAME', toTable: 'REFTBLRECNAME', toCol: null },
      ];

      // Discover which FK metadata tables exist in the source data
      const sourceTablesS4 = await executeOnSourceData(appId,
        `SELECT LOWER(table_name) as tbl FROM information_schema.tables
         WHERE table_schema = 'appdata_${appId}'`
      );
      const sourceTableSet = new Set(sourceTablesS4.rows.map(r => r.tbl));

      let totalS4Count = 0;

      // --- Generic FK metadata extraction (application-agnostic) ---
      for (const pattern of FK_METADATA_PATTERNS) {
        if (!sourceTableSet.has(pattern.table)) continue;

        // Oracle raw FND tables need a special 6-table JOIN — handle below
        if (pattern.type === 'oracle_raw') continue;

        console.log(`    Found FK metadata table: ${pattern.table} (${pattern.type})`);

        try {
          // Try the actual table name (uppercase, as stored by most ERP exports)
          const actualName = pattern.table.toUpperCase();
          const filterClause = pattern.filter ? `AND ${pattern.filter}` : '';
          const toColSelect = pattern.toCol
            ? `, "${pattern.toCol}" as to_column`
            : '';

          const fkResult = await executeOnSourceData(appId,
            `SELECT "${pattern.fromTable}" as from_table, "${pattern.fromCol}" as from_column,
                    "${pattern.toTable}" as to_table ${toColSelect}
             FROM "${actualName}"
             WHERE "${pattern.fromTable}" IS NOT NULL
               AND "${pattern.toTable}" IS NOT NULL ${filterClause}`,
            { maxRows: 0, timeout: 60000 }
          );

          let patternRelCount = 0;
          let patternSkipped = 0;

          for (const row of fkResult.rows) {
            const fromTableName = row.from_table;
            const toTableName = row.to_table;
            const fromColumn = row.from_column;
            const toColumn = row.to_column || fromColumn;

            if (!allTableNamesLower.includes(fromTableName.toLowerCase()) ||
                !allTableNamesLower.includes(toTableName.toLowerCase())) {
              patternSkipped++;
              continue;
            }

            const fromEntry = tableLookup[fromTableName.toLowerCase()];
            const toEntry = tableLookup[toTableName.toLowerCase()];
            if (!fromEntry || !toEntry || fromEntry.table.id === toEntry.table.id) continue;

            try {
              await query(
                `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality, confidence_score)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING`,
                [appId, fromEntry.table.id, fromColumn, toEntry.table.id, toColumn, 'fk', 'many_to_one', 95]
              );
              patternRelCount++;
            } catch (relErr) { /* ignore duplicates */ }
          }

          console.log(`    ${pattern.table}: ${fkResult.rows.length} FK entries, ${patternRelCount} matched schema, ${patternSkipped} skipped (not in schema)`);
          totalS4Count += patternRelCount;
        } catch (extractErr) {
          console.warn(`    Failed to extract from ${pattern.table}: ${extractErr.message}`);
        }
      }

      // --- Oracle EBS special case: raw FND_FOREIGN_KEYS (6-table JOIN) ---
      // Only runs if FND_RESOLVED_FKS was NOT found (it would have been handled above)
      if (sourceTableSet.has('fnd_foreign_keys') && !sourceTableSet.has('fnd_resolved_fks')) {
        console.log('    Oracle EBS: FND_RESOLVED_FKS not found, using raw FND table JOINs (may be slow)...');
        try {
          const fndFKResult = await executeOnSourceData(appId, `
            SELECT
              ft_from."TABLE_NAME" as from_table,
              fc_from."COLUMN_NAME" as from_column,
              ft_to."TABLE_NAME" as to_table,
              COALESCE(fc_to."COLUMN_NAME", fc_from."COLUMN_NAME") as to_column,
              fk."FOREIGN_KEY_NAME" as fk_name
            FROM "FND_FOREIGN_KEYS" fk
            JOIN "FND_FOREIGN_KEY_COLUMNS" fkc ON fkc."FOREIGN_KEY_ID" = fk."FOREIGN_KEY_ID"
              AND fkc."TABLE_ID" = fk."TABLE_ID"
            JOIN "FND_TABLES" ft_from ON ft_from."TABLE_ID" = fk."TABLE_ID"
            JOIN "FND_TABLES" ft_to ON ft_to."TABLE_ID" = fk."PRIMARY_KEY_TABLE_ID"
            JOIN "FND_COLUMNS" fc_from ON fc_from."COLUMN_ID" = fkc."COLUMN_ID"
              AND fc_from."TABLE_ID" = fk."TABLE_ID"
            LEFT JOIN "FND_PRIMARY_KEY_COLUMNS" pkc ON pkc."PRIMARY_KEY_ID" = fk."PRIMARY_KEY_ID"
              AND pkc."PRIMARY_KEY_SEQUENCE" = fkc."FOREIGN_KEY_SEQUENCE"
              AND pkc."TABLE_ID" = fk."PRIMARY_KEY_TABLE_ID"
            LEFT JOIN "FND_COLUMNS" fc_to ON fc_to."COLUMN_ID" = pkc."COLUMN_ID"
              AND fc_to."TABLE_ID" = fk."PRIMARY_KEY_TABLE_ID"
            WHERE fk."ENABLED_FLAG" IS DISTINCT FROM 'N'
            ORDER BY ft_from."TABLE_NAME", fc_from."COLUMN_NAME"
          `, { maxRows: 0, timeout: 120000 });

          if (fndFKResult && fndFKResult.rows.length > 0) {
            let fndRelCount = 0;
            let skippedNotInSchema = 0;

            for (const row of fndFKResult.rows) {
              const fromTableName = row.from_table;
              const toTableName = row.to_table;
              const fromColumn = row.from_column;
              const toColumn = row.to_column || fromColumn;

              if (!allTableNamesLower.includes(fromTableName.toLowerCase()) ||
                  !allTableNamesLower.includes(toTableName.toLowerCase())) {
                skippedNotInSchema++;
                continue;
              }

              const fromEntry = tableLookup[fromTableName.toLowerCase()];
              const toEntry = tableLookup[toTableName.toLowerCase()];
              if (!fromEntry || !toEntry || fromEntry.table.id === toEntry.table.id) continue;

              try {
                await query(
                  `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality, confidence_score)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                   ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING`,
                  [appId, fromEntry.table.id, fromColumn, toEntry.table.id, toColumn, 'fk', 'many_to_one', 95]
                );
                fndRelCount++;
              } catch (relErr) { /* ignore duplicates */ }
            }

            console.log(`    Oracle EBS raw JOINs: ${fndFKResult.rows.length} declared FKs, ${fndRelCount} matched schema, ${skippedNotInSchema} skipped`);
            totalS4Count += fndRelCount;
          }
        } catch (fndJoinErr) {
          console.warn(`    Oracle EBS raw JOIN failed: ${fndJoinErr.message}`);
        }
      }

      if (totalS4Count > 0) {
        console.log(`  Strategy 4 total: ${totalS4Count} metadata-declared FK relationships`);
      } else {
        console.log('  Strategy 4: no FK metadata tables found in source data');
      }
    } catch (fndErr) {
      if (!fndErr.skipped) console.warn(`  Strategy 4 warning: ${fndErr.message}`);
    }

    // Strategy 5: Value-Overlap FK Detection
    // For columns with potential FK names (ending in _id, Id, or matching another table's PK name)
    // that weren't caught by Strategies 1-4, check if the actual VALUES overlap with a known PK column.
    // This catches cross-module FKs where naming conventions differ.
    console.log('  Strategy 5: value-overlap FK detection...');
    try {
      const { executeOnSourceData } = require('../services/data-loader');
      const sourceAvailable = await hasSourceData(appId);

      if (sourceAvailable) {
        // Get existing relationships to avoid duplicates
        const existingRels = await query(
          `SELECT from_table_id, from_column, to_table_id, to_column FROM app_relationships WHERE app_id = $1`,
          [appId]
        );
        const existingRelSet = new Set(
          existingRels.rows.map(r => `${r.from_table_id}:${r.from_column}:${r.to_table_id}:${r.to_column}`)
        );

        // Build candidate pairs: columns that end in _id/Id but weren't already matched,
        // AND columns sharing a name with a PK in another table (even if not ending in _id)
        const candidatePairs = []; // { fromTable, fromCol, toTable, toCol }

        // Gather all PK columns with their table info for lookup
        const pkColumnsMap = {}; // colName.lower -> [{ tableEntry, colName }]
        for (const [tblName, entry] of Object.entries(tableLookup)) {
          for (const pkCol of entry.pkColumns) {
            const key = pkCol.toLowerCase();
            if (!pkColumnsMap[key]) pkColumnsMap[key] = [];
            pkColumnsMap[key].push({ tableEntry: entry, colName: pkCol });
          }
        }

        // Find unmatched FK-looking columns
        for (const table of tables) {
          const cols = await query(
            `SELECT column_name, is_pk FROM app_columns WHERE table_id = $1`,
            [table.id]
          );

          for (const col of cols.rows) {
            const colName = col.column_name;
            const colLower = colName.toLowerCase();

            // Skip columns that are already in a relationship (as from_column for this table)
            const alreadyMatched = existingRels.rows.some(
              r => r.from_table_id === table.id && r.from_column === colName
            );
            if (alreadyMatched) continue;

            // Candidate check 1: column name matches a PK in another table (exact match)
            const pkMatches = pkColumnsMap[colLower] || [];
            for (const pkMatch of pkMatches) {
              if (pkMatch.tableEntry.table.id === table.id) continue; // skip self
              const relKey = `${table.id}:${colName}:${pkMatch.tableEntry.table.id}:${pkMatch.colName}`;
              if (!existingRelSet.has(relKey)) {
                candidatePairs.push({
                  fromTable: table, fromCol: colName,
                  toTable: pkMatch.tableEntry.table, toCol: pkMatch.colName
                });
              }
            }

            // Candidate check 2: column ends in _id/Id and we haven't found a target yet
            // Try to match base name variations against all table PKs
            if (candidatePairs.filter(p => p.fromTable.id === table.id && p.fromCol === colName).length === 0) {
              const idMatch = colName.match(/^(.+?)(?:_id|Id)$/);
              if (idMatch) {
                const baseName = idMatch[1].toLowerCase().replace(/_/g, '');
                // Check all tables for a PK that could be the target
                for (const [tblName, entry] of Object.entries(tableLookup)) {
                  if (entry.table.id === table.id) continue;
                  const tblNameNorm = tblName.replace(/_/g, '');
                  // Check if base name is related to this table name
                  if (tblNameNorm.includes(baseName) || baseName.includes(tblNameNorm)) {
                    for (const pkCol of entry.pkColumns) {
                      const relKey = `${table.id}:${colName}:${entry.table.id}:${pkCol}`;
                      if (!existingRelSet.has(relKey)) {
                        candidatePairs.push({
                          fromTable: table, fromCol: colName,
                          toTable: entry.table, toCol: pkCol
                        });
                      }
                    }
                  }
                }
              }
            }
          }
        }

        // Now check actual value overlap for candidate pairs
        let s5Count = 0;
        const s5Checked = candidatePairs.length;
        console.log(`    Found ${candidatePairs.length} candidate pairs to check...`);

        for (const pair of candidatePairs) {
          try {
            // Sample distinct values from the child (from) column
            const childSample = await executeOnSourceData(appId,
              `SELECT DISTINCT "${pair.fromCol}" as val FROM "${pair.fromTable.table_name}"
               WHERE "${pair.fromCol}" IS NOT NULL LIMIT 50`,
              { timeout: 10000 }
            );
            if (childSample.rows.length === 0) continue;

            // Check how many of those values exist in the parent (to) column
            const sampleValues = childSample.rows.map(r => r.val);
            // Escape values for safe SQL (these come from our own DB, not user input)
            const escaped = sampleValues.map(v =>
              v === null ? 'NULL' : `'${String(v).replace(/'/g, "''")}'`
            ).join(', ');
            const parentMatch = await executeOnSourceData(appId,
              `SELECT COUNT(DISTINCT "${pair.toCol}") as cnt FROM "${pair.toTable.table_name}"
               WHERE "${pair.toCol}" IN (${escaped})`,
              { timeout: 10000 }
            );

            const matchCount = parseInt(parentMatch.rows[0]?.cnt || 0);
            const overlapRatio = matchCount / sampleValues.length;

            // Require at least 50% overlap. For small samples (1-2 values), allow matchCount >= sample size.
            const minMatchesS5 = Math.min(2, sampleValues.length);
            if (overlapRatio >= 0.5 && matchCount >= minMatchesS5) {
              const relKey = `${pair.fromTable.id}:${pair.fromCol}:${pair.toTable.id}:${pair.toCol}`;
              if (!existingRelSet.has(relKey)) {
                const confidence = Math.min(95, Math.round(overlapRatio * 100));
                await query(
                  `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality, confidence_score)
                   VALUES ($1, $2, $3, $4, $5, 'inferred', 'many_to_one', $6)
                   ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING`,
                  [appId, pair.fromTable.id, pair.fromCol, pair.toTable.id, pair.toCol, confidence]
                );
                existingRelSet.add(relKey);
                s5Count++;
                console.log(`    Value-overlap: ${pair.fromTable.table_name}.${pair.fromCol} → ${pair.toTable.table_name}.${pair.toCol} (${Math.round(overlapRatio * 100)}% overlap, ${matchCount}/${sampleValues.length})`);
              }
            }
          } catch (overlapErr) {
            // Skip pairs that fail — table might be empty or column type mismatch
          }
        }
        console.log(`  Strategy 5: checked ${s5Checked} candidates, found ${s5Count} new relationships`);
      } else {
        console.log('  Strategy 5 skipped — no source data available');
      }
    } catch (s5Err) {
      console.warn(`  Strategy 5 warning: ${s5Err.message}`);
    }

    const relCount = (await query('SELECT COUNT(*) as cnt FROM app_relationships WHERE app_id = $1', [appId])).rows[0].cnt;
    console.log(`  Discovered ${relCount} relationships total`);

    // Metadata table discovery — find self-describing tables (FND_*, DD0*, *_metadata, etc.)
    let metadataDiscovery = null;
    try {
      const { discoverAndStoreMetadata } = require('../services/metadata-discovery');
      metadataDiscovery = await discoverAndStoreMetadata(appId);
      if (metadataDiscovery.extracted_entries > 0) {
        console.log(`  Metadata discovery: found ${metadataDiscovery.candidates.length} candidates, extracted ${metadataDiscovery.extracted_entries} entries from ${metadataDiscovery.confirmed_count} confirmed tables`);
      } else if (metadataDiscovery.candidates.length > 0) {
        console.log(`  Metadata discovery: ${metadataDiscovery.candidates.length} candidates found but no extractable metadata`);
      } else {
        console.log(`  Metadata discovery: no metadata tables detected in this schema`);
      }
    } catch (err) {
      console.warn(`  Metadata discovery warning: ${err.message}`);
    }

    progress.metadata_discovery = metadataDiscovery ? {
      candidates: metadataDiscovery.candidates.length,
      confirmed: metadataDiscovery.confirmed_count,
      extracted_entries: metadataDiscovery.extracted_entries,
    } : null;

    await updateStage(runId, 'infer', 'completed');
    checkCancelled(runId);
    await snapshotProgress(runId);

    // --- STAGE 3.5: CONTEXT CHECK (verify if reference documents are loaded) ---
    await updateStage(runId, 'context', 'running');
    progress.current_stage = 'context';

    // Re-check context docs (includes any just auto-discovered from metadata tables)
    const contextCheckResult = await query(
      `SELECT COUNT(*) as count, COALESCE(SUM(LENGTH(extracted_text)), 0) as total_chars
       FROM context_documents WHERE app_id = $1 AND extracted_text IS NOT NULL AND extracted_text != ''`,
      [appId]
    );
    const contextDocCount = parseInt(contextCheckResult.rows[0].count);
    const contextChars = parseInt(contextCheckResult.rows[0].total_chars);

    if (contextDocCount > 0) {
      console.log(`Context-Assisted Build: ${contextDocCount} documents available (${contextChars} chars${metadataDiscovery?.extracted_entries > 0 ? ', includes auto-discovered metadata' : ''})`);
    } else {
      console.log('No context documents — enrichment will rely on profiling data and AI inference only');
    }

    await updateStage(runId, 'context', 'completed');
    checkCancelled(runId);
    await snapshotProgress(runId);

    // --- GATE: AI ENRICHMENT APPROVAL ---
    // Pause and wait for user to approve or skip the expensive enrichment stage
    let skipEnrich = false;
    {
      // Calculate gate summary for the user
      const draftColsResult = await query(
        `SELECT COUNT(*) as col_count FROM app_columns
         WHERE table_id IN (SELECT id FROM app_tables WHERE app_id = $1)
         AND enrichment_status = 'draft'`,
        [appId]
      );
      const draftColCount = parseInt(draftColsResult.rows[0].col_count);
      const tablesToEnrichCount = tables.length;
      // Read concurrency setting early so we can show it in the gate
      const appCfgEarly = typeof app.config === 'string' ? JSON.parse(app.config) : (app.config || {});
      const pipelineCfgEarly = appCfgEarly.pipeline || {};
      const concurrency = pipelineCfgEarly.parallel_concurrency || 8;
      const estimatedMinutes = Math.ceil((tablesToEnrichCount * 0.5) / concurrency); // ~30s per table / parallel workers

      const gateSummary = {
        tables: tablesToEnrichCount,
        columns: draftColCount,
        estimated_minutes: estimatedMinutes,
        parallel_workers: concurrency,
        context_docs: contextDocCount,
        metadata_entries: metadataDiscovery?.extracted_entries || 0,
      };

      await updateStage(runId, 'enrich', 'awaiting_approval');
      progress.current_stage = 'enrich';
      console.log(`[Pipeline] Gate: awaiting approval for AI enrichment (${tablesToEnrichCount} tables, ${concurrency} workers, ~${estimatedMinutes} min)`);

      const gateAction = await waitForGateApproval(runId, 'enrich', gateSummary);
      checkCancelled(runId);

      if (gateAction === 'skip') {
        console.log(`[Pipeline] AI Enrichment SKIPPED by user`);
        await updateStage(runId, 'enrich', 'skipped');
        skipEnrich = true;
      }
    }

    if (!skipEnrich) {
    // --- STAGE 4: AI SEMANTIC ENRICHMENT (LLM calls with profiling data) ---
    // Scale optimizations: skip enriched tables, batch small tables, parallel processing
    await updateStage(runId, 'enrich', 'running');
    progress.current_stage = 'enrich';
    progress.enrich_start_time = Date.now();

    // Read enrichment config from app settings (fall back to sensible defaults)
    const appCfg = typeof app.config === 'string' ? JSON.parse(app.config) : (app.config || {});
    const pipelineCfg = appCfg.pipeline || {};
    const PARALLEL_CONCURRENCY = pipelineCfg.parallel_concurrency || 8;
    const BATCH_COLUMN_THRESHOLD = pipelineCfg.batch_column_threshold || 8;
    const BATCH_MAX_COLUMNS = pipelineCfg.batch_max_columns || 30;

    // Load context documents for Context-Assisted Build
    // Separate user-uploaded docs from auto-discovered metadata for smart scoping
    const contextDocsResult = await query(
      `SELECT filename, file_type, extracted_text, metadata FROM context_documents
       WHERE app_id = $1 AND extracted_text IS NOT NULL AND extracted_text != ''
       ORDER BY uploaded_at`,
      [appId]
    );

    // User-uploaded docs (PDF, text, spreadsheets, images) — these are generally small
    // and relevant to the whole schema, so we include them globally
    const userDocs = contextDocsResult.rows.filter(d => d.file_type !== 'auto_discovered');
    const userContextText = userDocs.length > 0
      ? userDocs.map(d => `=== Source: ${d.filename} ===\n${d.extracted_text}`).join('\n\n')
      : '';

    // Auto-discovered metadata — can be massive in enterprise apps
    // We use the structured table_index (stored in metadata JSONB) for per-table lookup
    // Falls back to full text (truncated) if no structured index exists
    const autoDiscoveredDoc = contextDocsResult.rows.find(d => d.file_type === 'auto_discovered');
    let metadataTableIndex = null;
    if (autoDiscoveredDoc) {
      try {
        const meta = typeof autoDiscoveredDoc.metadata === 'string'
          ? JSON.parse(autoDiscoveredDoc.metadata)
          : autoDiscoveredDoc.metadata;
        if (meta && typeof meta === 'object' && Object.keys(meta).length > 0) {
          metadataTableIndex = meta;
          console.log(`Context-Assisted Build: structured metadata index loaded (${Object.keys(meta).length} tables indexed)`);
        }
      } catch (e) {
        // Fall back to unstructured
      }
    }

    // Budget constants for context assembly
    // These are per-table budgets — each enrichTable call gets up to this much context
    const USER_CONTEXT_BUDGET = 30000;    // chars for user-uploaded docs (generous to avoid losing info)
    const METADATA_CONTEXT_BUDGET = 10000; // chars for table-specific metadata
    const GLOBAL_METADATA_BUDGET = 5000;  // chars for fallback unstructured metadata

    /**
     * Assemble scoped context for a specific table being enriched.
     * For small schemas (<20 tables), just sends everything.
     * For large schemas, filters metadata to only entries relevant to this table.
     */
    function getTableContext(tableName) {
      const parts = [];
      const tableNameLower = tableName.toLowerCase();

      // 1. User-uploaded docs — always included (truncated to budget)
      if (userContextText) {
        const truncated = userContextText.length > USER_CONTEXT_BUDGET
          ? userContextText.substring(0, USER_CONTEXT_BUDGET) + '\n[...truncated for length]'
          : userContextText;
        parts.push(truncated);
      }

      // 2. Auto-discovered metadata — table-scoped if we have structured index
      if (metadataTableIndex && metadataTableIndex[tableNameLower]) {
        const tableEntries = metadataTableIndex[tableNameLower];
        const metadataLines = [`=== Auto-Discovered Metadata for "${tableName}" ===`];
        for (const entry of tableEntries) {
          const line = [
            entry.column ? `Column: ${entry.column}` : null,
            entry.description ? `Description: ${entry.description}` : null,
            entry.type ? `Type: ${entry.type}` : null,
          ].filter(Boolean).join(' | ');
          metadataLines.push(line);
        }
        const metadataText = metadataLines.join('\n');
        const truncated = metadataText.length > METADATA_CONTEXT_BUDGET
          ? metadataText.substring(0, METADATA_CONTEXT_BUDGET) + '\n[...truncated]'
          : metadataText;
        parts.push(truncated);
      } else if (autoDiscoveredDoc && !metadataTableIndex) {
        // No structured index — fall back to global metadata text (truncated)
        const truncated = autoDiscoveredDoc.extracted_text.length > GLOBAL_METADATA_BUDGET
          ? autoDiscoveredDoc.extracted_text.substring(0, GLOBAL_METADATA_BUDGET) + '\n[...truncated — large metadata catalog]'
          : autoDiscoveredDoc.extracted_text;
        parts.push(`=== Auto-Discovered Schema Metadata ===\n${truncated}`);
      }

      const combined = parts.join('\n\n');
      if (combined) {
        console.log(`[Context] Table "${tableName}": injecting ${combined.length} chars of context (${parts.length} sections)`);
      } else {
        console.log(`[Context] Table "${tableName}": NO context available`);
      }
      return combined || null;
    }

    const totalContextDocs = contextDocsResult.rows.length;
    if (totalContextDocs > 0) {
      console.log(`Context-Assisted Build: ${userDocs.length} user docs, ${autoDiscoveredDoc ? '1 auto-discovered' : '0 auto-discovered'}, table-scoped=${!!metadataTableIndex}`);
    }

    // Filter to only tables with draft columns (skip already-enriched)
    const tablesToEnrich = Object.values(tableColumnsMap);
    const skippedCount = tables.length - tablesToEnrich.length;
    if (skippedCount > 0) {
      console.log(`Skipping ${skippedCount} tables (already enriched), processing ${tablesToEnrich.length} tables`);
    }

    // =========================================================
    // DOMAIN TAXONOMY — derive consistent domain names from table prefixes
    // =========================================================
    // Well-known ERP module prefixes (Oracle EBS, SAP, etc.)
    const KNOWN_MODULE_PREFIXES = {
      // Oracle EBS modules
      'AP': 'Accounts Payable', 'AR': 'Accounts Receivable', 'GL': 'General Ledger',
      'PO': 'Purchasing', 'INV': 'Inventory', 'OM': 'Order Management',
      'OE': 'Order Entry', 'HR': 'Human Resources', 'PA': 'Project Accounting',
      'FA': 'Fixed Assets', 'CE': 'Cash Management', 'XLA': 'Subledger Accounting',
      'FND': 'Foundation', 'WF': 'Workflow', 'IBY': 'Payments',
      'QP': 'Advanced Pricing', 'CS': 'Service', 'CSI': 'Install Base',
      'ASO': 'Quoting', 'OKC': 'Contracts', 'OKS': 'Service Contracts',
      'WSH': 'Shipping', 'WIP': 'Work in Process', 'BOM': 'Bills of Material',
      'ENG': 'Engineering', 'MRP': 'Material Requirements Planning',
      'CSD': 'Depot Repair', 'JTF': 'CRM Foundation', 'HZ': 'Trading Community',
      'IBE': 'iStore', 'AMS': 'Marketing', 'ASN': 'Sales',
      'PER': 'Human Resources', 'PAY': 'Payroll', 'BEN': 'Benefits',
      'MTL': 'Inventory', 'RCV': 'Receiving', 'RA': 'Receivables',
      'ZX': 'Tax', 'ICX': 'Self-Service', 'FLM': 'Flow Manufacturing',
      'GMD': 'Process Manufacturing', 'GR': 'Process Manufacturing',
      'CN': 'Collections', 'IEX': 'Collections',
      'CSF': 'Field Service', 'CUG': 'Service', 'AME': 'Approvals Management',
      'WMS': 'Warehouse Management', 'MSC': 'Supply Chain Planning',
      'PON': 'Sourcing', 'POS': 'iSupplier', 'FTE': 'Transportation',
      // SAP common prefixes
      'BSEG': 'Financial Accounting', 'BKPF': 'Financial Accounting',
      'EKKO': 'Purchasing', 'EKPO': 'Purchasing', 'VBAK': 'Sales',
      'MARA': 'Material Management', 'KNA1': 'Customer Master',
      'LFA1': 'Vendor Master',
    };

    function deriveDomainTaxonomy(tableNames) {
      // Step 1: Extract prefixes and count tables per prefix
      const prefixCounts = {};
      const prefixToTables = {};
      for (const name of tableNames) {
        // Try to extract prefix: first segment before underscore
        const parts = name.split('_');
        if (parts.length >= 2) {
          const prefix = parts[0].toUpperCase();
          prefixCounts[prefix] = (prefixCounts[prefix] || 0) + 1;
          if (!prefixToTables[prefix]) prefixToTables[prefix] = [];
          prefixToTables[prefix].push(name);
        }
      }

      // Step 2: Map prefixes to known domain names, or generate from prefix
      const domainMap = {}; // domain_name -> { name, description, tableCount, prefixes }
      for (const [prefix, count] of Object.entries(prefixCounts)) {
        const knownName = KNOWN_MODULE_PREFIXES[prefix];
        const domainName = knownName || (count >= 3 ? `${prefix} Module` : null);
        if (domainName) {
          if (!domainMap[domainName]) {
            domainMap[domainName] = { name: domainName, description: '', tableCount: 0, prefixes: [] };
          }
          domainMap[domainName].tableCount += count;
          domainMap[domainName].prefixes.push(prefix);
          if (knownName) {
            domainMap[domainName].description = `${domainMap[domainName].prefixes.join(', ')} tables`;
          }
        }
      }

      // Step 3: Sort by table count (most tables first) and return
      const taxonomy = Object.values(domainMap)
        .sort((a, b) => b.tableCount - a.tableCount)
        .map(d => ({
          name: d.name,
          description: d.description || `${d.prefixes.join(', ')} tables (${d.tableCount} tables)`,
        }));

      // Always include "Other" as a catch-all
      if (taxonomy.length > 0) {
        taxonomy.push({ name: 'Other', description: 'Tables that do not clearly belong to any domain above' });
      }

      return taxonomy;
    }

    const domainTaxonomy = deriveDomainTaxonomy(allTableNames);
    if (domainTaxonomy.length > 1) {
      console.log(`Domain taxonomy derived: ${domainTaxonomy.length - 1} domains from table prefixes: ${domainTaxonomy.slice(0, 10).map(d => d.name).join(', ')}${domainTaxonomy.length > 11 ? '...' : ''}`);
    }

    // Separate into large tables (individual calls) and small tables (batch candidates)
    const largeTables = [];
    const smallTables = [];
    for (const entry of tablesToEnrich) {
      if (entry.columns.length <= BATCH_COLUMN_THRESHOLD) {
        smallTables.push(entry);
      } else {
        largeTables.push(entry);
      }
    }

    // Group small tables into batches
    const smallBatches = [];
    let currentBatch = [];
    let currentBatchCols = 0;
    for (const entry of smallTables) {
      if (currentBatchCols + entry.columns.length > BATCH_MAX_COLUMNS && currentBatch.length > 0) {
        smallBatches.push(currentBatch);
        currentBatch = [];
        currentBatchCols = 0;
      }
      currentBatch.push(entry);
      currentBatchCols += entry.columns.length;
    }
    if (currentBatch.length > 0) smallBatches.push(currentBatch);

    console.log(`Enrichment plan: ${largeTables.length} large tables (individual), ${smallTables.length} small tables in ${smallBatches.length} batches, concurrency=${PARALLEL_CONCURRENCY}`);

    // Initialize worker tracking
    progress.concurrency = PARALLEL_CONCURRENCY;
    progress.workers = [];
    for (let i = 0; i < Math.min(PARALLEL_CONCURRENCY, largeTables.length + smallBatches.length); i++) {
      progress.workers.push({ id: i, status: 'idle', current_table: null, tables_done: 0, started_at: null });
    }

    // Helper: process a single table enrichment and write results to DB
    async function processEnrichResult(result, table) {
      // Track token usage
      if (result.token_usage) {
        progress.token_usage.input_tokens += result.token_usage.input_tokens;
        progress.token_usage.output_tokens += result.token_usage.output_tokens;
        progress.token_usage.total_tokens += result.token_usage.total_tokens;

        try {
          const costEstimate = (result.token_usage.input_tokens * 3 / 1000000) + (result.token_usage.output_tokens * 15 / 1000000);
          await query(
            `INSERT INTO token_usage (app_id, pipeline_run_id, stage, table_name, input_tokens, output_tokens, total_tokens, model, cost_estimate)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
            [appId, runId, 'enrich', table.entity_name || table.table_name,
             result.token_usage.input_tokens, result.token_usage.output_tokens, result.token_usage.total_tokens,
             result.token_usage.model || 'claude-sonnet-4-20250514', costEstimate]
          );
        } catch (tokenErr) {
          console.warn('Failed to log token usage:', tokenErr.message);
        }
      }

      // Write column results to DB
      for (const enrichedCol of result.columns) {
        if (!enrichedCol.id) continue;

        const status = result.enrichment_status === 'failed' ? 'draft'
          : enrichedCol.confidence_score >= 70 ? 'ai_enriched' : 'needs_review';

        const valueMapping = enrichedCol.value_dictionary
          ? JSON.stringify(enrichedCol.value_dictionary)
          : null;

        await query(
          `UPDATE app_columns
           SET business_name = $1, description = $2, confidence_score = $3,
               enrichment_status = $4, enriched_by = 'ai', enriched_at = NOW(),
               value_mapping = $5, column_role = $6
           WHERE id = $7`,
          [
            enrichedCol.business_name,
            enrichedCol.description,
            enrichedCol.confidence_score,
            status,
            valueMapping,
            enrichedCol.column_role || null,
            enrichedCol.id,
          ]
        );

        // Write AI-generated synonyms from enrichment
        if (enrichedCol.synonyms && enrichedCol.synonyms.length > 0) {
          for (const term of enrichedCol.synonyms) {
            try {
              await query(
                `INSERT INTO app_synonyms (app_id, column_id, table_id, term, source, confidence_score, status, created_by)
                 VALUES ($1, $2, $3, $4, 'ai_generated', 75, 'active', NULL)
                 ON CONFLICT (app_id, column_id, term) DO UPDATE SET status = 'active', confidence_score = 75, updated_at = NOW()`,
                [appId, enrichedCol.id, table.id, term.toLowerCase().trim()]
              );
            } catch (synErr) { /* skip duplicates */ }
          }
        }

        progress.columns_done++;
      }

      // Store entity-level metadata
      const entityMetadata = {
        entity_type: result.entity_type || 'UNKNOWN',
        domain: result.module || result.domain || 'General',
        business_name: result.table_description ? (table.entity_name || table.table_name) : null,
        sample_questions: result.sample_questions || [],
        computed_measures: result.computed_measures || [],
      };
      await query(
        `UPDATE app_tables SET description = $1, entity_metadata = $2, enrichment_status = 'ai_enriched', enriched_by = 'ai', enriched_at = NOW() WHERE id = $3`,
        [result.table_description || '', JSON.stringify(entityMetadata), table.id]
      );

      progress.tables_done++;

      if (result.error) {
        progress.errors.push({ table: table.entity_name || table.table_name, error: result.error });
      }

      // Snapshot progress every 5 tables
      if (progress.tables_done % 5 === 0) {
        snapshotProgress(runId).catch(() => {});
      }
    }

    // Helper: enrich a single table entry (workerId for tracking)
    async function enrichSingleTable(entry, workerId) {
      checkCancelled(runId); // Check before starting each table
      const { table, columns } = entry;
      const tableName = table.entity_name || table.table_name;
      progress.current_table = tableName;
      if (workerId !== undefined && progress.workers[workerId]) {
        progress.workers[workerId].status = 'enriching';
        progress.workers[workerId].current_table = tableName;
        progress.workers[workerId].started_at = Date.now();
      }
      const tableStart = Date.now();

      console.log(`Enriching "${tableName}" (${columns.length} cols, profile: ${tableProfiles[table.id] ? 'yes' : 'no'})...`);

      const result = await enrichTable(tableName, columns, {
        app_name: app.name,
        app_type: app.type,
        related_tables: allTableNames.filter(t => t !== tableName).slice(0, 20),
        context_documents: getTableContext(tableName),
        domain_taxonomy: domainTaxonomy.length > 1 ? domainTaxonomy : null,
      }, tableProfiles[table.id] || null);

      const tableDuration = Date.now() - tableStart;
      progress.enrich_table_times.push(tableDuration);

      await processEnrichResult(result, table);

      // Update worker status
      if (workerId !== undefined && progress.workers[workerId]) {
        progress.workers[workerId].tables_done++;
        progress.workers[workerId].status = 'idle';
        progress.workers[workerId].current_table = null;
      }
    }

    // Helper: enrich a batch of small tables (one LLM call for multiple tables)
    async function enrichBatch(batch, workerId) {
      // For batches, we still call enrichTable per-table but run them in parallel
      // This is because each table needs its own prompt context for accuracy
      // The parallelism comes from running multiple tables concurrently
      // Pass workerId to the first, remaining run under same worker label
      if (workerId !== undefined && progress.workers[workerId]) {
        const names = batch.map(e => (e.table.entity_name || e.table.table_name));
        progress.workers[workerId].status = 'enriching';
        progress.workers[workerId].current_table = names.join(', ');
      }
      await Promise.all(batch.map(entry => enrichSingleTable(entry)));
      if (workerId !== undefined && progress.workers[workerId]) {
        progress.workers[workerId].status = 'idle';
        progress.workers[workerId].current_table = null;
      }
    }

    // Build the work queue: each task accepts a workerId
    const workQueue = [];
    for (const entry of largeTables) {
      workQueue.push((wId) => enrichSingleTable(entry, wId));
    }
    for (const batch of smallBatches) {
      workQueue.push((wId) => enrichBatch(batch, wId));
    }

    // Execute with concurrency limit — each worker passes its ID to tasks
    async function runWithConcurrency(tasks, concurrency) {
      let idx = 0;

      async function worker(workerId) {
        while (idx < tasks.length) {
          if (progress.cancelled) break; // Stop picking up new tasks
          const taskIdx = idx++;
          try {
            await tasks[taskIdx](workerId);
          } catch (err) {
            if (err instanceof PipelineCancelled) throw err; // Propagate cancel
            console.error(`Enrichment task ${taskIdx} failed:`, err.message);
            progress.errors.push({ task: taskIdx, error: err.message });
            if (progress.workers[workerId]) {
              progress.workers[workerId].status = 'error';
              progress.workers[workerId].current_table = null;
            }
          }
        }
        // Worker finished all its tasks
        if (progress.workers[workerId]) {
          progress.workers[workerId].status = 'done';
        }
      }

      const workers = [];
      for (let i = 0; i < Math.min(concurrency, tasks.length); i++) {
        workers.push(worker(i));
      }
      await Promise.all(workers);
    }

    await runWithConcurrency(workQueue, PARALLEL_CONCURRENCY);

    // --- POST-ENRICHMENT: Deterministic Domain Assignment ---
    // The LLM is unreliable for domain classification (99/132 tables ended up "unclassified"
    // despite explicit taxonomy in the prompt). Instead, we assign domains DETERMINISTICALLY
    // from table name prefixes, which is correct for 95%+ of ERP tables. The LLM-assigned
    // domain is kept as a fallback only for tables with no recognized prefix.
    try {
      const enrichedTables = await query(
        `SELECT id, table_name, entity_metadata FROM app_tables WHERE app_id = $1 AND entity_metadata IS NOT NULL`,
        [appId]
      );

      let prefixAssigned = 0;
      let llmKept = 0;
      let fallbackOther = 0;
      const finalDomainCounts = {};

      for (const row of enrichedTables.rows) {
        const meta = typeof row.entity_metadata === 'string' ? JSON.parse(row.entity_metadata) : row.entity_metadata;
        const tableName = row.table_name;
        const oldDomain = meta?.domain || 'General';

        // Step 1: Try to assign domain from table name prefix (deterministic, always correct)
        const prefix = tableName.split('_')[0]?.toUpperCase();
        const prefixDomain = prefix ? KNOWN_MODULE_PREFIXES[prefix] : null;

        let newDomain;
        if (prefixDomain) {
          // Prefix matches a known ERP module — use it unconditionally
          newDomain = prefixDomain;
          prefixAssigned++;
        } else if (oldDomain && oldDomain !== 'General' && oldDomain !== 'UNKNOWN' && oldDomain !== '' && oldDomain !== 'Other') {
          // No prefix match, but LLM assigned something meaningful — keep it
          newDomain = oldDomain;
          llmKept++;
        } else {
          // No prefix match, LLM didn't help — mark as Other
          newDomain = 'Other';
          fallbackOther++;
        }

        // Update if changed
        if (newDomain !== oldDomain) {
          meta.domain = newDomain;
          await query(
            `UPDATE app_tables SET entity_metadata = $1 WHERE id = $2`,
            [JSON.stringify(meta), row.id]
          );
        }

        finalDomainCounts[newDomain] = (finalDomainCounts[newDomain] || 0) + 1;
      }

      const finalDomainCount = Object.keys(finalDomainCounts).length;
      console.log(`Domain assignment: ${enrichedTables.rows.length} tables → ${finalDomainCount} domains`);
      console.log(`  ${prefixAssigned} from prefix, ${llmKept} kept from LLM, ${fallbackOther} fallback to Other`);
      console.log(`  Domains: ${Object.entries(finalDomainCounts).sort((a, b) => b[1] - a[1]).map(([d, c]) => `${d}(${c})`).join(', ')}`);
    } catch (normErr) {
      console.warn('Domain assignment warning (non-fatal):', normErr.message);
    }

    // --- POST-ENRICHMENT: Deterministic Column Role Assignment ---
    // The LLM classifies column_role, but we apply heuristic overrides for patterns
    // that are universally consistent across ERP schemas. This ensures correctness
    // even when the LLM misclassifies or omits the role.
    try {
      const allCols = await query(
        `SELECT ac.id, ac.column_name, ac.data_type, ac.is_pk, ac.is_fk, ac.column_role, ac.business_name,
                at.table_name
         FROM app_columns ac
         JOIN app_tables at ON ac.table_id = at.id
         WHERE at.app_id = $1 AND at.enrichment_status = 'ai_enriched'`,
        [appId]
      );

      let heuristicOverrides = 0;
      let heuristicFills = 0;
      const TECHNICAL_PATTERNS = /^(CREATED_BY|CREATION_DATE|LAST_UPDATE_DATE|LAST_UPDATED_BY|LAST_UPDATE_LOGIN|REQUEST_ID|PROGRAM_APPLICATION_ID|PROGRAM_ID|PROGRAM_UPDATE_DATE|CONCURRENT_PROGRAM_ID|WHO_CREATED|WHO_UPDATED|OBJECT_VERSION_NUMBER|ATTRIBUTE_CATEGORY|ATTRIBUTE\d+|GLOBAL_ATTRIBUTE\d+|TP_ATTRIBUTE\d+|TP_ATTRIBUTE_CATEGORY)$/i;
      const DATE_PATTERNS = /(_DATE|_TIMESTAMP|_AT|_ON|_WHEN|_TIME)$/i;
      const FLAG_PATTERNS = /(_FLAG|_ENABLED|_ACTIVE|_YN|_INDICATOR|ENABLED_FLAG|ACTIVE_FLAG)$/i;
      const DESCRIPTION_PATTERNS = /^(DESCRIPTION|COMMENTS|NOTE|NOTES|REMARKS|MEMO|LONG_DESCRIPTION)$/i;

      for (const col of allCols.rows) {
        const name = col.column_name.toUpperCase();
        const currentRole = col.column_role;
        let newRole = currentRole;

        // Heuristic 1: Technical/audit columns — override even if LLM said something else
        if (TECHNICAL_PATTERNS.test(name)) {
          newRole = 'technical';
        }
        // Heuristic 2: *_ID columns that are PK → surrogate_key (unless LLM said natural_key with good reason)
        else if (name.endsWith('_ID') && col.is_pk && currentRole !== 'natural_key') {
          newRole = 'surrogate_key';
        }
        // Heuristic 3: *_ID columns that are FK but not PK → fk_only
        else if (name.endsWith('_ID') && col.is_fk && !col.is_pk && !currentRole) {
          newRole = 'fk_only';
        }
        // Heuristic 4: Flag columns
        else if (FLAG_PATTERNS.test(name) && !currentRole) {
          newRole = 'flag';
        }
        // Heuristic 5: Date columns
        else if ((DATE_PATTERNS.test(name) || col.data_type?.toUpperCase()?.includes('DATE') || col.data_type?.toUpperCase()?.includes('TIMESTAMP')) && !currentRole) {
          newRole = 'date';
        }
        // Heuristic 6: Description columns
        else if (DESCRIPTION_PATTERNS.test(name) && !currentRole) {
          newRole = 'description_col';
        }
        // Heuristic 7: Common natural key patterns (NUMBER, CODE, NAME columns that aren't _ID)
        else if (/(_NUMBER|_NUM|_CODE|_NAME|^SEGMENT\d+)$/i.test(name) && !name.endsWith('_ID') && !currentRole) {
          newRole = 'natural_key';
        }
        // Heuristic 8: Common measure patterns
        else if (/(_AMOUNT|_AMT|_QTY|_QUANTITY|_RATE|_PRICE|_COST|_TOTAL|_PERCENT|_PCT|_WEIGHT|_VOLUME)$/i.test(name) && !currentRole) {
          newRole = 'measure';
        }

        if (newRole && newRole !== currentRole) {
          await query('UPDATE app_columns SET column_role = $1 WHERE id = $2', [newRole, col.id]);
          if (currentRole) {
            heuristicOverrides++;
          } else {
            heuristicFills++;
          }
        }
      }

      console.log(`Column role assignment: ${allCols.rows.length} columns processed`);
      console.log(`  ${heuristicFills} roles filled by heuristic, ${heuristicOverrides} LLM roles overridden`);
    } catch (roleErr) {
      console.warn('Column role assignment warning (non-fatal):', roleErr.message);
    }

    // --- POST-ENRICHMENT: Business Term Alias Injection ---
    // Users often refer to columns by business terms that differ from the actual column name.
    // The #1 retry cause is the LLM inventing a column (e.g., STANDARD_COST) because the user said
    // "standard cost" but the real column is LIST_PRICE_PER_UNIT. This heuristic appends "Also known as"
    // aliases to column descriptions so the NL2SQL model can find the right column.
    try {
      // Pattern: column name regex → aliases to append (if not already in description)
      const ALIAS_PATTERNS = [
        { col: /^LIST_PRICE(_PER_UNIT)?$/i, aliases: 'standard cost, item cost, unit cost, cost per unit', context: 'Use this column when users ask for "standard cost" or "item cost"' },
        { col: /^SEGMENT1$/i, aliases: 'item number, part number, item code', context: 'This is the user-facing item identifier in Oracle EBS' },
        { col: /^SEGMENT2$/i, aliases: 'item category, sub-inventory', context: 'Secondary segment — often item category or sub-classification' },
        { col: /^UNIT_SELLING_PRICE$/i, aliases: 'selling price, sale price, line price, price per unit', context: 'Use this for sales dollar amounts when multiplied by quantity' },
        { col: /^UNIT_PRICE$/i, aliases: 'purchase price, PO price, buying price, cost per unit', context: 'Use this for purchase dollar amounts when multiplied by quantity' },
        { col: /^ORDERED_QUANTITY$/i, aliases: 'order quantity, qty ordered, line quantity', context: 'Number of units on an order line' },
        { col: /^QUANTITY_ON_HAND$/i, aliases: 'on-hand quantity, stock on hand, inventory quantity, qty on hand, current stock', context: 'Current inventory balance' },
        { col: /^QUANTITY_INVOICED$/i, aliases: 'invoiced qty, billed quantity', context: 'Number of units invoiced/billed' },
        { col: /^TRANSACTION_QUANTITY$/i, aliases: 'qty transacted, movement quantity, transfer quantity', context: 'Quantity moved in an inventory transaction' },
        { col: /^PRIMARY_QUANTITY$/i, aliases: 'primary qty, base quantity, base UOM quantity', context: 'Quantity in the primary unit of measure' },
        { col: /^INVOICE_AMOUNT$/i, aliases: 'invoice total, bill amount, invoice value', context: 'Total dollar amount of the invoice' },
        { col: /^AMOUNT$/i, aliases: 'dollar amount, payment amount, transaction amount', context: 'Monetary value — check context for whether this is a payment, invoice, or journal amount' },
        { col: /^ENTERED_DR$/i, aliases: 'debit amount, debit, DR', context: 'Journal entry debit in entered currency' },
        { col: /^ENTERED_CR$/i, aliases: 'credit amount, credit, CR', context: 'Journal entry credit in entered currency' },
        { col: /^ACCOUNTED_DR$/i, aliases: 'functional debit, base debit', context: 'Journal entry debit in functional/base currency' },
        { col: /^ACCOUNTED_CR$/i, aliases: 'functional credit, base credit', context: 'Journal entry credit in functional/base currency' },
      ];

      const colsForAlias = await query(
        `SELECT ac.id, ac.column_name, ac.description
         FROM app_columns ac
         JOIN app_tables at ON ac.table_id = at.id
         WHERE at.app_id = $1 AND at.enrichment_status = 'ai_enriched'`,
        [appId]
      );

      let aliasesAdded = 0;
      for (const col of colsForAlias.rows) {
        const match = ALIAS_PATTERNS.find(p => p.col.test(col.column_name));
        if (match) {
          const desc = col.description || '';
          // Only add if aliases aren't already in the description
          if (!desc.toLowerCase().includes('also known as') && !desc.toLowerCase().includes(match.aliases.split(',')[0].trim().toLowerCase())) {
            const aliasAppend = ` Also known as: ${match.aliases}. ${match.context}.`;
            const newDesc = desc + aliasAppend;
            await query('UPDATE app_columns SET description = $1 WHERE id = $2', [newDesc, col.id]);
            aliasesAdded++;
          }
        }
      }

      if (aliasesAdded > 0) {
        console.log(`Business term aliases: ${aliasesAdded} column descriptions enriched with aliases`);
      }
    } catch (aliasErr) {
      console.warn('Business term alias injection warning (non-fatal):', aliasErr.message);
    }

    // --- POST-ENRICHMENT: Computed Measure Discovery ---
    // Detect common price × quantity patterns across tables and add computed measures
    // to entity_metadata. These are the #1 cause of wrong SQL (COUNT instead of SUM).
    try {
      const enrichedTablesForMeasures = await query(
        `SELECT at.id, at.table_name, at.entity_metadata,
                json_agg(json_build_object('column_name', ac.column_name, 'column_role', ac.column_role, 'business_name', ac.business_name, 'data_type', ac.data_type)) as columns
         FROM app_tables at
         JOIN app_columns ac ON ac.table_id = at.id
         WHERE at.app_id = $1 AND at.enrichment_status = 'ai_enriched'
         GROUP BY at.id, at.table_name, at.entity_metadata`,
        [appId]
      );

      let measuresAdded = 0;

      // Common price × quantity column pairs (case-insensitive matching)
      const PRICE_QTY_PATTERNS = [
        { price: /^UNIT_(SELLING_)?PRICE$/i, qty: /^ORDERED_QUANTITY$/i, name: 'Line Amount', desc: 'Dollar value per line (unit price × ordered quantity)' },
        { price: /^UNIT_(SELLING_)?PRICE$/i, qty: /^SHIPPED_QUANTITY$/i, name: 'Shipped Amount', desc: 'Dollar value of shipped quantity (unit price × shipped quantity)' },
        { price: /^UNIT_(SELLING_)?PRICE$/i, qty: /^INVOICED_QUANTITY$/i, name: 'Invoiced Amount', desc: 'Dollar value of invoiced quantity (unit price × invoiced quantity)' },
        { price: /^UNIT_PRICE$/i, qty: /^QUANTITY$/i, name: 'Line Amount', desc: 'Dollar value per line (unit price × quantity)' },
        { price: /^UNIT_PRICE$/i, qty: /^QUANTITY_INVOICED$/i, name: 'Invoiced Amount', desc: 'Dollar value invoiced (unit price × quantity invoiced)' },
        { price: /^LIST_PRICE_PER_UNIT$/i, qty: /^(TRANSACTION_QUANTITY|PRIMARY_QUANTITY)$/i, name: 'Transaction Value', desc: 'Dollar value of inventory transaction (list price × quantity)' },
        { price: /^(STANDARD_COST|ITEM_COST)$/i, qty: /^(TRANSACTION_QUANTITY|PRIMARY_QUANTITY)$/i, name: 'Cost Value', desc: 'Cost value of transaction (cost × quantity)' },
      ];

      for (const tableRow of enrichedTablesForMeasures.rows) {
        const meta = typeof tableRow.entity_metadata === 'string' ? JSON.parse(tableRow.entity_metadata) : (tableRow.entity_metadata || {});
        const existingMeasures = meta.computed_measures || [];
        const cols = tableRow.columns || [];
        const colNames = cols.map(c => c.column_name);
        const newMeasures = [...existingMeasures];
        let added = false;

        for (const pattern of PRICE_QTY_PATTERNS) {
          const priceCol = colNames.find(n => pattern.price.test(n));
          const qtyCol = colNames.find(n => pattern.qty.test(n));
          if (priceCol && qtyCol) {
            // Check if this measure already exists
            const formula = `"${priceCol}" * "${qtyCol}"`;
            const exists = newMeasures.some(m => m.formula && m.formula.includes(priceCol) && m.formula.includes(qtyCol));
            if (!exists) {
              newMeasures.push({
                name: pattern.name,
                formula: formula,
                description: `${pattern.desc}. Use SUM(${formula}) for totals. Use this when users ask for "dollar amount", "value", "sales amount", or "how much".`
              });
              added = true;
            }
          }
        }

        // Also check for header tables that need cross-table computed measures
        // (e.g., OE_ORDER_HEADERS_ALL needs to reference OE_ORDER_LINES_ALL for order value)
        if (tableRow.table_name.match(/_HEADERS_ALL$/i)) {
          const linesTableName = tableRow.table_name.replace(/_HEADERS_ALL$/i, '_LINES_ALL');
          const linesTable = enrichedTablesForMeasures.rows.find(t => t.table_name.toUpperCase() === linesTableName.toUpperCase());
          if (linesTable) {
            const linesCols = (linesTable.columns || []).map(c => c.column_name);
            // Check if lines table has amount-producing columns
            for (const pattern of PRICE_QTY_PATTERNS) {
              const priceCol = linesCols.find(n => pattern.price.test(n));
              const qtyCol = linesCols.find(n => pattern.qty.test(n));
              if (priceCol && qtyCol) {
                const crossFormula = `SUM("${linesTableName}"."${priceCol}" * "${linesTableName}"."${qtyCol}")`;
                const exists = newMeasures.some(m => m.join_table && m.join_table.toUpperCase() === linesTableName.toUpperCase());
                if (!exists) {
                  // Find the join column (usually *_ID matching the header PK)
                  const headerPk = colNames.find(n => /^(HEADER_ID|ORDER_HEADER_ID)$/i.test(n)) || colNames.find(n => n.endsWith('_ID') && cols.find(c => c.column_name === n)?.column_role === 'surrogate_key');
                  const linesFk = (linesTable.columns || []).find(c => c.column_name.toUpperCase() === (headerPk || '').toUpperCase());
                  if (headerPk && linesFk) {
                    newMeasures.push({
                      name: 'Total Order Value',
                      formula: crossFormula,
                      join_table: linesTableName,
                      join_on: `"${headerPk}" = "${linesFk.column_name}"`,
                      description: `Total dollar value of the order (sum of all line amounts). Use when users ask for "order dollar amount", "order value", "sales amount", "how much". Requires JOIN to ${linesTableName}.`
                    });
                    added = true;
                  }
                }
              }
            }
          }
        }

        if (added) {
          meta.computed_measures = newMeasures;
          await query('UPDATE app_tables SET entity_metadata = $1 WHERE id = $2', [JSON.stringify(meta), tableRow.id]);
          measuresAdded += newMeasures.length - existingMeasures.length;
        }
      }

      console.log(`Computed measure discovery: ${measuresAdded} measures added across ${enrichedTablesForMeasures.rows.length} tables`);
    } catch (measureErr) {
      console.warn('Computed measure discovery warning (non-fatal):', measureErr.message);
    }

    await updateStage(runId, 'enrich', 'completed');
    } // end if (!skipEnrich)

    checkCancelled(runId);
    await snapshotProgress(runId);

    // --- STAGE 5: REVIEW (seed patterns from sample questions, then manual curation) ---
    if (!skipEnrich) {
    await updateStage(runId, 'review', 'running');
    progress.current_stage = 'review';

    // Seed Pattern Library from AI-generated sample questions
    console.log('Seeding Pattern Library from enrichment...');
    await query('DELETE FROM query_patterns WHERE app_id = $1', [appId]);

    const enrichedTables = await query(
      `SELECT table_name, entity_metadata FROM app_tables
       WHERE app_id = $1 AND entity_metadata IS NOT NULL`,
      [appId]
    );

    let patternCount = 0;
    for (const t of enrichedTables.rows) {
      const meta = t.entity_metadata || {};
      const questions = meta.sample_questions || [];
      for (const q of questions) {
        try {
          await query(
            `INSERT INTO query_patterns (app_id, pattern_name, nl_template, sql_template, tables_used, status, usage_count, confidence)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (app_id, pattern_name) DO NOTHING`,
            [appId, `${t.table_name}: ${q.substring(0, 80)}`, q, '',
             `{${t.table_name}}`, 'ai_discovered', 0, 0.7]
          );
          patternCount++;
        } catch (patErr) { /* ignore duplicates */ }
      }
    }
    console.log(`  Seeded ${patternCount} query patterns`);

    await updateStage(runId, 'review', 'completed');
    } else {
      // Enrichment was skipped — mark review as skipped too
      await updateStage(runId, 'review', 'skipped');
    }

    checkCancelled(runId);
    await snapshotProgress(runId);

    // --- GATE: QPD SEEDING APPROVAL ---
    let skipValidate = false;
    {
      const sourceDataForGate = await hasSourceData(appId);
      const enrichedCount = await query(
        `SELECT COUNT(*) as cnt FROM app_tables WHERE app_id = $1 AND enrichment_status = 'ai_enriched'`,
        [appId]
      );
      const enrichedTableCount = parseInt(enrichedCount.rows[0].cnt);
      const qpdEstimate = Math.ceil(enrichedTableCount * 1.5); // rough: ~1.5 min per entity

      const gateSummary = {
        enriched_tables: enrichedTableCount,
        has_source_data: sourceDataForGate,
        estimated_minutes: qpdEstimate,
        description: sourceDataForGate
          ? `Generate and test sample queries for ${enrichedTableCount} business objects (~${qpdEstimate} min)`
          : 'No source data available — QPD seeding will be skipped automatically',
      };

      await updateStage(runId, 'validate', 'awaiting_approval');
      progress.current_stage = 'validate';
      console.log(`[Pipeline] Gate: awaiting approval for QPD seeding (${enrichedTableCount} entities, ~${qpdEstimate} min)`);

      const gateAction = await waitForGateApproval(runId, 'validate', gateSummary);
      checkCancelled(runId);

      if (gateAction === 'skip') {
        console.log(`[Pipeline] QPD Seeding SKIPPED by user`);
        skipValidate = true;
        await updateStage(runId, 'validate', 'skipped');
      }
    }

    if (!skipValidate) {
    // --- STAGE 5.5: VALIDATE (QPD Seeding — generate + test sample queries) ---
    await updateStage(runId, 'validate', 'running');
    progress.current_stage = 'validate';
    console.log('QPD Seeding: generating and testing sample queries per business object...');

    try {
      const sourceDataAvailableForQPD = await hasSourceData(appId);
      if (sourceDataAvailableForQPD) {
        // Clear previous system-seeded QPD entries
        await query(
          `DELETE FROM test_queries WHERE app_id = $1 AND confidence = 0.90`,
          [appId]
        );

        // Collect sample questions grouped by business object (entity)
        const entitiesForQPD = await query(
          `SELECT entity_name, table_name, entity_metadata FROM app_tables
           WHERE app_id = $1 AND entity_metadata IS NOT NULL ORDER BY entity_name, table_name`,
          [appId]
        );

        const entityQuestions = {};
        for (const row of entitiesForQPD.rows) {
          const entityKey = row.entity_name || row.table_name;
          const meta = row.entity_metadata || {};
          const questions = meta.sample_questions || [];
          if (!entityQuestions[entityKey]) {
            entityQuestions[entityKey] = { tables: [], questions: new Set() };
          }
          entityQuestions[entityKey].tables.push(row.table_name);
          for (const q of questions) entityQuestions[entityKey].questions.add(q);
        }

        const totalQuestions = Object.values(entityQuestions).reduce((sum, e) => sum + e.questions.size, 0);
        console.log(`QPD Seeding: ${Object.keys(entityQuestions).length} business objects, ${totalQuestions} questions`);

        // Build BOKG context and value dictionaries once
        const bokgContext = await buildBOKGContext(appId);
        const valueDictionaries = await loadValueDictionaries(appId);
        let seeded = 0, failed = 0;

        const QPD_TIMEOUT_MS = 90000; // 90s timeout per QPD question
        const withTimeout = (promise, ms) => Promise.race([
          promise,
          new Promise((_, reject) => setTimeout(() => reject(new Error(`QPD timeout after ${ms/1000}s`)), ms))
        ]);

        for (const [entityName, entityData] of Object.entries(entityQuestions)) {
          checkCancelled(runId);
          progress.current_table = entityName;
          for (const question of entityData.questions) {
            try {
              const genResult = await withTimeout(
                generateAndExecuteWithRetry(
                  appId, question, bokgContext, app.name || 'Unknown', '', valueDictionaries
                ),
                QPD_TIMEOUT_MS
              );

              if (!genResult.execError && genResult.rows.length > 0) {
                await query(
                  `INSERT INTO test_queries (app_id, user_id, nl_query, generated_sql, execution_result, confidence, feedback, created_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())`,
                  [appId, 1, question, genResult.sql,
                   JSON.stringify({ rows: genResult.rows.slice(0, 5), row_count: genResult.rows.length, source: 'qpd_seed', entity: entityName }),
                   0.90, 'thumbs_up']
                );
                seeded++;
                console.log(`  ✓ [${entityName}] "${question.substring(0, 50)}..." (${genResult.rows.length} rows)`);
              } else {
                failed++;
                console.log(`  ✗ [${entityName}] "${question.substring(0, 50)}..." ${genResult.execError || '0 rows'}`);
              }
            } catch (qErr) {
              failed++;
              console.log(`  ✗ [${entityName}] error: ${qErr.message.substring(0, 80)}`);
            }
          }
        }

        console.log(`QPD Seeding complete: ${seeded} seeded, ${failed} failed out of ${totalQuestions}`);
        progress.qpd_results = {
          entities: Object.keys(entityQuestions).length,
          total_questions: totalQuestions,
          seeded,
          failed,
        };
      } else {
        console.log('QPD Seeding skipped: no source data available');
        progress.qpd_results = { skipped: true, reason: 'no source data' };
      }
    } catch (qpdErr) {
      // QPD seeding is non-critical — log but don't fail the pipeline
      console.error('QPD Seeding error (non-fatal):', qpdErr.message);
      progress.qpd_results = { error: qpdErr.message };
    }

    await updateStage(runId, 'validate', 'completed');
    } // end if (!skipValidate)

    // --- STAGE 6: PUBLISH (stays pending until user publishes) ---
    // Don't mark completed — user does this manually

    // Mark run as completed
    const finalStages = activeRuns[runId]?.stages || {};
    await query(
      `UPDATE pipeline_runs SET status = 'completed', completed_at = NOW(), stages = $1 WHERE id = $2`,
      [JSON.stringify(finalStages), runId]
    );

    await query("UPDATE applications SET status = 'in_review', updated_at = NOW() WHERE id = $1", [appId]);

    console.log(`Pipeline ${runId} completed: ${progress.columns_done}/${progress.columns_total} columns enriched`);
  } catch (err) {
    const progress = activeRuns[runId];
    const isCancelled = err instanceof PipelineCancelled || (progress && progress.cancelled);

    if (isCancelled) {
      // --- CANCELLATION: graceful stop, preserve partial work ---
      console.log(`Pipeline ${runId} CANCELLED by user`);

      if (progress && progress.stages) {
        for (const [stageName, stage] of Object.entries(progress.stages)) {
          if (stage.status === 'running' || stage.status === 'awaiting_approval') {
            stage.status = 'cancelled';
            stage.completed_at = new Date().toISOString();
            if (stage.started_at) stage.duration_ms = new Date() - new Date(stage.started_at);
          }
        }
      }

      const finalStages = activeRuns[runId]?.stages || {};
      await query(
        `UPDATE pipeline_runs SET status = 'cancelled', completed_at = NOW(), stages = $1 WHERE id = $2`,
        [JSON.stringify(finalStages), runId]
      );
      // Set app status based on how far we got
      const enrichCompleted = finalStages.enrich?.status === 'completed';
      const newAppStatus = enrichCompleted ? 'in_review' : 'profiling';
      await query("UPDATE applications SET status = $1, updated_at = NOW() WHERE id = $2", [newAppStatus, appId]);
    } else {
      // --- REAL FAILURE ---
      console.error(`Pipeline ${runId} failed:`, err);

      // Mark any 'running' or 'awaiting_approval' stages as 'failed'
      if (progress && progress.stages) {
        for (const [stageName, stage] of Object.entries(progress.stages)) {
          if (stage.status === 'running' || stage.status === 'awaiting_approval') {
            stage.status = 'failed';
            stage.completed_at = new Date().toISOString();
            stage.error = err.message;
            if (stage.started_at) stage.duration_ms = new Date() - new Date(stage.started_at);
          }
        }
      }

      const finalStages = activeRuns[runId]?.stages || {};
      await query(
        `UPDATE pipeline_runs SET status = 'failed', completed_at = NOW(), stages = $1 WHERE id = $2`,
        [JSON.stringify(finalStages), runId]
      );
      await query("UPDATE applications SET status = 'profiling', updated_at = NOW() WHERE id = $1", [appId]);
    }
  }
}

async function updateStage(runId, stageName, status) {
  const progress = activeRuns[runId];
  if (!progress) return;

  const now = new Date().toISOString();
  const stage = progress.stages[stageName];

  if (status === 'running') {
    stage.status = 'running';
    stage.started_at = now;
  } else if (status === 'completed' || status === 'skipped' || status === 'cancelled') {
    stage.status = status;
    stage.completed_at = now;
    if (stage.started_at) stage.duration_ms = new Date(now) - new Date(stage.started_at);
  } else if (status === 'awaiting_approval') {
    stage.status = 'awaiting_approval';
    if (!stage.started_at) stage.started_at = now;
  } else {
    stage.status = status;
  }

  await query(
    `UPDATE pipeline_runs SET stages = $1 WHERE id = $2`,
    [JSON.stringify(progress.stages), runId]
  );
}

// GET /api/pipeline/:appId/status
router.get('/:appId/status', async (req, res) => {
  try {
    const { appId } = req.params;

    const result = await query(
      `SELECT id, app_id, status, started_at, completed_at, stages, progress_snapshot
       FROM pipeline_runs WHERE app_id = $1 ORDER BY started_at DESC LIMIT 1`,
      [appId]
    );

    if (result.rows.length === 0) {
      return res.json({ status: 'no_runs', stages: null });
    }

    const run = result.rows[0];

    // Use in-memory progress if available, otherwise fall back to DB snapshot
    let progress = activeRuns[run.id] || {};
    let fromSnapshot = false;
    if (!activeRuns[run.id] && run.status === 'running' && run.progress_snapshot) {
      try {
        progress = typeof run.progress_snapshot === 'string'
          ? JSON.parse(run.progress_snapshot) : run.progress_snapshot;
        fromSnapshot = true;
      } catch (e) { /* ignore parse error */ }
    }

    // Calculate ETA based on average per-table enrichment time
    let eta_seconds = null;
    const tableTimes = progress.enrich_table_times || [];
    if (tableTimes.length > 0 && (progress.tables_total - progress.tables_done) > 0) {
      const avgMs = tableTimes.reduce((a, b) => a + b, 0) / tableTimes.length;
      eta_seconds = Math.round((avgMs * (progress.tables_total - progress.tables_done)) / 1000);
    }

    // Check for pending gate (approval required)
    const pendingGate = progress.pendingGate ? {
      stage: progress.pendingGate.stage,
      summary: progress.pendingGate.summary,
      created_at: progress.pendingGate.created_at,
    } : null;

    res.json({
      run_id: run.id,
      status: run.status,
      started_at: run.started_at,
      completed_at: run.completed_at,
      stages: run.stages,
      from_snapshot: fromSnapshot,
      pending_gate: pendingGate,
      progress: {
        tables_total: progress.tables_total || 0,
        tables_done: progress.tables_done || 0,
        columns_total: progress.columns_total || 0,
        columns_done: progress.columns_done || 0,
        current_table: progress.current_table || null,
        current_stage: progress.current_stage || null,
        errors: progress.errors || [],
        eta_seconds: eta_seconds,
        token_usage: progress.token_usage || { input_tokens: 0, output_tokens: 0, total_tokens: 0 },
        workers: progress.workers || [],
        concurrency: progress.concurrency || 1,
        metadata_discovery: progress.metadata_discovery || null,
        qpd_results: progress.qpd_results || null,
      },
    });
  } catch (err) {
    console.error('Get pipeline status error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/pipeline/:appId/history
router.get('/:appId/history', async (req, res) => {
  try {
    const { appId } = req.params;
    const limit = req.query.limit || 50;

    const result = await query(
      `SELECT pr.id, pr.app_id, pr.triggered_by, pr.status, pr.started_at, pr.completed_at, pr.stages,
              u.name as triggered_by_name
       FROM pipeline_runs pr
       LEFT JOIN users u ON pr.triggered_by = u.id
       WHERE pr.app_id = $1 ORDER BY pr.started_at DESC LIMIT $2`,
      [appId, limit]
    );

    res.json({ runs: result.rows });
  } catch (err) {
    console.error('Get pipeline history error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/pipeline/:appId/cancel - cancel a running pipeline
router.post('/:appId/cancel', async (req, res) => {
  try {
    const { appId } = req.params;

    // Find the active run for this app
    const runResult = await query(
      "SELECT id FROM pipeline_runs WHERE app_id = $1 AND status = 'running' ORDER BY started_at DESC LIMIT 1",
      [appId]
    );

    if (runResult.rows.length === 0) {
      return res.status(404).json({ error: 'No running pipeline found for this application' });
    }

    const runId = runResult.rows[0].id;
    const progress = activeRuns[runId];

    if (progress) {
      progress.cancelled = true;
      console.log(`[Pipeline] Cancel requested for run ${runId} (app ${appId})`);

      // If the pipeline is waiting at a gate, reject the gate to unblock it
      if (progress.pendingGate && progress.pendingGate.reject) {
        progress.pendingGate.reject(new PipelineCancelled());
        progress.pendingGate = null;
      }

      res.json({ message: `Cancel signal sent to pipeline run #${runId}. The current step will finish and then the pipeline will stop.`, run_id: runId });
    } else {
      // Run is in DB as 'running' but not in memory — likely a zombie from a restart
      await query(
        `UPDATE pipeline_runs SET status = 'cancelled', completed_at = NOW() WHERE id = $1`,
        [runId]
      );
      await query("UPDATE applications SET status = 'profiling', updated_at = NOW() WHERE id = $1", [appId]);
      res.json({ message: `Pipeline run #${runId} was stale (no active process). Marked as cancelled.`, run_id: runId });
    }
  } catch (err) {
    console.error('Cancel pipeline error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/pipeline/:appId/approve - approve or skip a gated stage
router.post('/:appId/approve', async (req, res) => {
  try {
    const { appId } = req.params;
    const { action } = req.body; // 'approve' or 'skip'

    if (!action || !['approve', 'skip'].includes(action)) {
      return res.status(400).json({ error: 'action must be "approve" or "skip"' });
    }

    // Find the active run
    const runResult = await query(
      "SELECT id FROM pipeline_runs WHERE app_id = $1 AND status = 'running' ORDER BY started_at DESC LIMIT 1",
      [appId]
    );

    if (runResult.rows.length === 0) {
      return res.status(404).json({ error: 'No running pipeline found' });
    }

    const runId = runResult.rows[0].id;
    const progress = activeRuns[runId];

    if (!progress || !progress.pendingGate) {
      return res.status(400).json({ error: 'No pending approval gate for this pipeline run' });
    }

    const gateStageName = progress.pendingGate.stage;
    console.log(`[Pipeline] Gate "${gateStageName}" ${action}d by user for run ${runId}`);

    // Resolve the gate promise — the pipeline loop will continue
    progress.pendingGate.resolve(action);
    progress.pendingGate = null;

    res.json({ message: `Stage "${gateStageName}" ${action === 'approve' ? 'approved' : 'skipped'}`, run_id: runId, stage: gateStageName, action });
  } catch (err) {
    console.error('Approve gate error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/pipeline/:appId/reset - full reset (drops source data too)
router.post('/:appId/reset', async (req, res) => {
  try {
    const { appId } = req.params;

    const appCheck = await query('SELECT id, name FROM applications WHERE id = $1', [appId]);
    if (appCheck.rows.length === 0) {
      return res.status(404).json({ error: 'Application not found' });
    }

    // Reset columns
    const colReset = await query(
      `UPDATE app_columns
       SET business_name = NULL, description = NULL, confidence_score = 0,
           enrichment_status = 'draft', enriched_by = NULL, enriched_at = NULL,
           value_mapping = NULL
       WHERE table_id IN (SELECT id FROM app_tables WHERE app_id = $1)
       RETURNING id`,
      [appId]
    );

    // Reset table descriptions and entity metadata, but keep row_count if preserving data
    await query(
      `UPDATE app_tables SET description = NULL, row_count = 0, entity_metadata = NULL WHERE app_id = $1`,
      [appId]
    );

    // Clear pipeline runs, curation logs, test queries, patterns, relationships, token usage
    await query('DELETE FROM pipeline_runs WHERE app_id = $1', [appId]);
    await query(
      `DELETE FROM curation_log WHERE column_id IN (
        SELECT ac.id FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1
      )`,
      [appId]
    );
    await query('DELETE FROM test_queries WHERE app_id = $1', [appId]);
    await query('DELETE FROM query_patterns WHERE app_id = $1', [appId]);
    await query('DELETE FROM app_relationships WHERE app_id = $1', [appId]);
    await query('DELETE FROM token_usage WHERE app_id = $1', [appId]);

    // Drop source data schema (will be reloaded on next pipeline run)
    try {
      await dropSourceData(appId);
    } catch (err) {
      console.warn('Failed to drop source data schema:', err.message);
    }

    // Clear enrichment_snapshot from config (prevents stale before/after comparison)
    await query(
      "UPDATE applications SET config = COALESCE(config, '{}'::jsonb) - 'enrichment_snapshot' WHERE id = $1",
      [appId]
    );

    // Reset app status
    await query("UPDATE applications SET status = 'draft', updated_at = NOW() WHERE id = $1", [appId]);

    res.json({
      message: `Application "${appCheck.rows[0].name}" fully reset (including source data)`,
      columns_reset: colReset.rowCount,
    });
  } catch (err) {
    console.error('Reset error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/pipeline/:appId/reset-enrichments - reset enrichments only, keep source data + profiling
router.post('/:appId/reset-enrichments', async (req, res) => {
  try {
    const { appId } = req.params;

    const appCheck = await query('SELECT id, name FROM applications WHERE id = $1', [appId]);
    if (appCheck.rows.length === 0) {
      return res.status(404).json({ error: 'Application not found' });
    }

    // Reset column enrichments but keep value_mapping (profiling data)
    const colReset = await query(
      `UPDATE app_columns
       SET business_name = NULL, description = NULL, confidence_score = 0,
           enrichment_status = 'draft', enriched_by = NULL, enriched_at = NULL
       WHERE table_id IN (SELECT id FROM app_tables WHERE app_id = $1)
       RETURNING id`,
      [appId]
    );

    // Reset table descriptions and entity metadata, but preserve row_count (from profiling)
    await query(
      `UPDATE app_tables SET description = NULL, entity_metadata = NULL WHERE app_id = $1`,
      [appId]
    );

    // Clear enrichment-related data: relationships, patterns, test queries, token usage
    await query('DELETE FROM app_relationships WHERE app_id = $1', [appId]);
    await query('DELETE FROM query_patterns WHERE app_id = $1', [appId]);
    await query('DELETE FROM test_queries WHERE app_id = $1', [appId]);
    await query('DELETE FROM token_usage WHERE app_id = $1', [appId]);
    await query(
      `DELETE FROM curation_log WHERE column_id IN (
        SELECT ac.id FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1
      )`,
      [appId]
    );
    // Clear pipeline runs so it starts fresh
    await query('DELETE FROM pipeline_runs WHERE app_id = $1', [appId]);

    // Set status back to 'profiled' — data is loaded and profiled, ready for enrichment
    await query("UPDATE applications SET status = 'profiled', updated_at = NOW() WHERE id = $1", [appId]);

    res.json({
      message: `Enrichments reset for "${appCheck.rows[0].name}" — source data and profiling preserved`,
      columns_reset: colReset.rowCount,
      data_preserved: true,
    });
  } catch (err) {
    console.error('Reset enrichments error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/pipeline/:appId/re-enrich-with-context
// One-click: snapshot current enrichments → reset enrichments → auto-run pipeline
// Used by Context Coach panel to avoid multi-step confirmation flow
router.post('/:appId/re-enrich-with-context', async (req, res) => {
  const { appId } = req.params;
  let step = 'init';
  try {
    step = 'app_check';
    const appCheck = await query('SELECT id, name, type, status, config FROM applications WHERE id = $1', [appId]);
    if (appCheck.rows.length === 0) {
      return res.status(404).json({ error: 'Application not found' });
    }

    // Step 1: Snapshot current enrichments for before/after comparison
    step = 'snapshot_query';
    const snapshotResult = await query(
      `SELECT ac.id AS column_id, ac.column_name, ac.business_name, ac.description,
              ac.confidence_score, ac.enrichment_status, ac.enriched_by,
              at.table_name, at.id AS table_id
       FROM app_columns ac
       JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = $1
       ORDER BY at.table_name, ac.column_name`,
      [appId]
    );

    step = 'build_snapshot';
    const snapshot = {
      taken_at: new Date().toISOString(),
      column_count: snapshotResult.rows.length,
      columns: snapshotResult.rows.map(r => ({
        column_id: r.column_id,
        table_name: r.table_name,
        table_id: r.table_id,
        column_name: r.column_name,
        business_name: r.business_name,
        description: r.description,
        confidence_score: parseFloat(r.confidence_score) || 0,
        enrichment_status: r.enrichment_status,
        enriched_by: r.enriched_by,
      })),
    };

    step = 'save_snapshot';
    await query(
      `UPDATE applications
       SET config = COALESCE(config, '{}'::jsonb) || jsonb_build_object('enrichment_snapshot', $1::jsonb)
       WHERE id = $2`,
      [JSON.stringify(snapshot), appId]
    );

    // Step 2: Reset enrichments (keep source data + profiling)
    step = 'reset_columns';
    await query(
      `UPDATE app_columns
       SET business_name = NULL, description = NULL, confidence_score = 0,
           enrichment_status = 'draft', enriched_by = NULL, enriched_at = NULL
       WHERE table_id IN (SELECT id FROM app_tables WHERE app_id = $1)`,
      [appId]
    );
    step = 'reset_tables';
    await query(`UPDATE app_tables SET description = NULL, entity_metadata = NULL WHERE app_id = $1`, [appId]);
    step = 'delete_relationships';
    await query('DELETE FROM app_relationships WHERE app_id = $1', [appId]);
    step = 'delete_patterns';
    await query('DELETE FROM query_patterns WHERE app_id = $1', [appId]);
    step = 'delete_test_queries';
    await query('DELETE FROM test_queries WHERE app_id = $1', [appId]);
    step = 'delete_token_usage';
    await query('DELETE FROM token_usage WHERE app_id = $1', [appId]);
    step = 'delete_curation_log';
    await query(
      `DELETE FROM curation_log WHERE column_id IN (
        SELECT ac.id FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1
      )`,
      [appId]
    );
    step = 'delete_pipeline_runs';
    await query('DELETE FROM pipeline_runs WHERE app_id = $1', [appId]);
    step = 'update_status_profiled';
    await query("UPDATE applications SET status = 'profiled', updated_at = NOW() WHERE id = $1", [appId]);

    // Step 3: Auto-start pipeline (same as POST /run)
    step = 'create_pipeline_run';
    const runResult = await query(
      `INSERT INTO pipeline_runs (app_id, triggered_by, status, started_at, stages)
       VALUES ($1, $2, 'running', NOW(), $3)
       RETURNING *`,
      [appId, req.user?.id || 1, JSON.stringify({
        ingest: { status: 'pending' }, profile: { status: 'pending' },
        infer: { status: 'pending' }, context: { status: 'pending' },
        validate: { status: 'pending' },
        enrich: { status: 'pending' }, review: { status: 'pending' },
        publish: { status: 'pending' },
      })]
    );

    step = 'post_insert';
    const run = runResult.rows[0];
    await query("UPDATE applications SET status = 'enriching', updated_at = NOW() WHERE id = $1", [appId]);

    // Initialize activeRuns tracking (same as POST /run)
    step = 'init_active_runs';
    const stagesObj = typeof run.stages === 'string' ? JSON.parse(run.stages) : (run.stages || {});
    activeRuns[run.id] = {
      stages: stagesObj,
      tables_total: 0, tables_done: 0, columns_total: 0, columns_done: 0,
      current_table: null, current_stage: 'ingest', errors: [],
      enrich_start_time: null, enrich_table_times: [],
      token_usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 },
      workers: [], concurrency: 1,
    };

    // Fire and forget — pipeline runs in background
    step = 'start_pipeline';
    const app = appCheck.rows[0];
    runEnrichmentPipeline(run.id, appId, app).catch(err => {
      console.error(`Background pipeline error for app ${appId}:`, err);
    });

    step = 'send_response';
    res.json({
      message: `Re-enrichment started for "${appCheck.rows[0].name}" with context`,
      run: run,
      snapshot_columns: snapshot.column_count,
    });
  } catch (err) {
    console.error(`Re-enrich with context error at step "${step}":`, err);
    res.status(500).json({ error: 'Internal server error', step: step, detail: err.message });
  }
});

// GET /api/pipeline/:appId/runs/:runId
router.get('/:appId/runs/:runId', async (req, res) => {
  try {
    const { appId, runId } = req.params;

    const result = await query(
      `SELECT * FROM pipeline_runs WHERE id = $1 AND app_id = $2`,
      [runId, appId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Run not found' });
    }

    const run = result.rows[0];
    const totalDuration = run.completed_at ? new Date(run.completed_at) - new Date(run.started_at) : null;

    res.json({ run: { ...run, total_duration_ms: totalDuration } });
  } catch (err) {
    console.error('Get run detail error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// =============================================
// Context Coach Endpoints
// =============================================

// POST /api/pipeline/:appId/snapshot-enrichments
// Save current enrichment state before re-enrichment for before/after comparison
router.post('/:appId/snapshot-enrichments', async (req, res) => {
  try {
    const { appId } = req.params;

    // Grab current enrichment data for all columns
    const result = await query(
      `SELECT ac.id AS column_id, ac.column_name, ac.business_name, ac.description,
              ac.confidence_score, ac.enrichment_status, ac.enriched_by,
              at.table_name, at.id AS table_id
       FROM app_columns ac
       JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = $1
       ORDER BY at.table_name, ac.column_name`,
      [appId]
    );

    const snapshot = {
      taken_at: new Date().toISOString(),
      column_count: result.rows.length,
      columns: result.rows.map(r => ({
        column_id: r.column_id,
        table_name: r.table_name,
        table_id: r.table_id,
        column_name: r.column_name,
        business_name: r.business_name,
        description: r.description,
        confidence_score: parseFloat(r.confidence_score) || 0,
        enrichment_status: r.enrichment_status,
        enriched_by: r.enriched_by,
      })),
    };

    // Store snapshot in application config JSONB
    await query(
      `UPDATE applications
       SET config = COALESCE(config, '{}'::jsonb) || jsonb_build_object('enrichment_snapshot', $1::jsonb)
       WHERE id = $2`,
      [JSON.stringify(snapshot), appId]
    );

    const avgConfidence = snapshot.columns.length > 0
      ? snapshot.columns.reduce((sum, c) => sum + c.confidence_score, 0) / snapshot.columns.length
      : 0;

    res.json({
      message: 'Enrichment snapshot saved',
      column_count: snapshot.column_count,
      avg_confidence: Math.round(avgConfidence * 10) / 10,
    });
  } catch (err) {
    console.error('Snapshot enrichments error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/pipeline/:appId/enrichment-diff
// Compare current enrichments against the saved snapshot
router.get('/:appId/enrichment-diff', async (req, res) => {
  try {
    const { appId } = req.params;

    // Get snapshot from app config
    const appResult = await query(
      `SELECT config FROM applications WHERE id = $1`,
      [appId]
    );

    if (appResult.rows.length === 0) {
      return res.status(404).json({ error: 'Application not found' });
    }

    const config = appResult.rows[0].config || {};
    const snapshot = config.enrichment_snapshot;

    if (!snapshot) {
      return res.json({ has_snapshot: false, message: 'No enrichment snapshot available' });
    }

    // Get current enrichment data
    const currentResult = await query(
      `SELECT ac.id AS column_id, ac.column_name, ac.business_name, ac.description,
              ac.confidence_score, ac.enrichment_status, ac.enriched_by,
              at.table_name, at.id AS table_id
       FROM app_columns ac
       JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = $1
       ORDER BY at.table_name, ac.column_name`,
      [appId]
    );

    // Build lookup of snapshot by column_id
    const snapshotLookup = {};
    for (const col of snapshot.columns) {
      snapshotLookup[col.column_id] = col;
    }

    // Compare
    const improved = [];
    const regressed = [];
    const unchanged = [];
    const newEnrichments = [];
    let prevTotal = 0, currTotal = 0, prevCount = 0, currCount = 0;

    for (const curr of currentResult.rows) {
      const prev = snapshotLookup[curr.column_id];
      const currConf = parseFloat(curr.confidence_score) || 0;

      if (!prev) {
        // Column added after snapshot (shouldn't happen normally)
        newEnrichments.push({
          column_id: curr.column_id, table_name: curr.table_name,
          column_name: curr.column_name,
          new_business_name: curr.business_name, new_confidence: currConf,
        });
        currTotal += currConf;
        currCount++;
        continue;
      }

      const prevConf = prev.confidence_score || 0;
      prevTotal += prevConf;
      prevCount++;
      currTotal += currConf;
      currCount++;

      const delta = currConf - prevConf;
      const entry = {
        column_id: curr.column_id,
        table_name: curr.table_name,
        column_name: curr.column_name,
        old_business_name: prev.business_name,
        new_business_name: curr.business_name,
        old_confidence: prevConf,
        new_confidence: currConf,
        delta: Math.round(delta * 10) / 10,
      };

      if (prevConf === 0 && currConf > 0) {
        newEnrichments.push(entry);
      } else if (delta >= 10) {
        improved.push(entry);
      } else if (delta <= -10) {
        regressed.push(entry);
      } else {
        unchanged.push(entry);
      }
    }

    const prevAvg = prevCount > 0 ? prevTotal / prevCount : 0;
    const currAvg = currCount > 0 ? currTotal / currCount : 0;

    res.json({
      has_snapshot: true,
      snapshot_taken_at: snapshot.taken_at,
      summary: {
        total_columns: currentResult.rows.length,
        improved_count: improved.length,
        regressed_count: regressed.length,
        unchanged_count: unchanged.length,
        new_count: newEnrichments.length,
        prev_avg_confidence: Math.round(prevAvg * 10) / 10,
        curr_avg_confidence: Math.round(currAvg * 10) / 10,
        confidence_lift: Math.round((currAvg - prevAvg) * 10) / 10,
      },
      improved: improved.sort((a, b) => b.delta - a.delta),
      regressed: regressed.sort((a, b) => a.delta - b.delta),
      new_enrichments: newEnrichments,
      unchanged_low: unchanged.filter(c => c.new_confidence < 50)
        .sort((a, b) => a.new_confidence - b.new_confidence),
    });
  } catch (err) {
    console.error('Enrichment diff error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/pipeline/:appId/confidence-summary
// Get confidence distribution for the Context Coach panel
router.get('/:appId/confidence-summary', async (req, res) => {
  try {
    const { appId } = req.params;

    const result = await query(
      `SELECT ac.id AS column_id, ac.column_name, ac.business_name, ac.description,
              ac.confidence_score, ac.enrichment_status, ac.enriched_by,
              ac.is_pk, ac.is_fk,
              at.table_name, at.id AS table_id
       FROM app_columns ac
       JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = $1
       ORDER BY ac.confidence_score ASC, at.table_name, ac.column_name`,
      [appId]
    );

    if (result.rows.length === 0) {
      return res.json({ has_enrichments: false });
    }

    // Check if any enrichments exist
    const enrichedCount = result.rows.filter(r => r.enrichment_status && r.enrichment_status !== 'draft').length;
    if (enrichedCount === 0) {
      return res.json({ has_enrichments: false });
    }

    const columns = result.rows.map(r => ({
      column_id: r.column_id,
      table_name: r.table_name,
      table_id: r.table_id,
      column_name: r.column_name,
      business_name: r.business_name,
      description: r.description,
      confidence_score: parseFloat(r.confidence_score) || 0,
      enrichment_status: r.enrichment_status,
      enriched_by: r.enriched_by,
      is_pk: r.is_pk,
      is_fk: r.is_fk,
    }));

    // Thresholds aligned with curation page (schema.js): 90+ = auto-approve, 60-89 = review, <60 = manual
    const high = columns.filter(c => c.confidence_score >= 90);
    const moderate = columns.filter(c => c.confidence_score >= 60 && c.confidence_score < 90);
    const low = columns.filter(c => c.confidence_score < 60);

    // Group low-confidence columns by table
    const lowByTable = {};
    for (const col of low) {
      if (!lowByTable[col.table_name]) lowByTable[col.table_name] = [];
      lowByTable[col.table_name].push(col);
    }

    // Group moderate columns by table too (for knowledge entry)
    const moderateByTable = {};
    for (const col of moderate) {
      if (!moderateByTable[col.table_name]) moderateByTable[col.table_name] = [];
      moderateByTable[col.table_name].push(col);
    }

    const totalConf = columns.reduce((sum, c) => sum + c.confidence_score, 0);
    const avgConf = totalConf / columns.length;

    // Check if there's a snapshot for comparison
    const appResult = await query(`SELECT config FROM applications WHERE id = $1`, [appId]);
    const hasSnapshot = !!(appResult.rows[0]?.config?.enrichment_snapshot);

    res.json({
      has_enrichments: true,
      total_columns: columns.length,
      avg_confidence: Math.round(avgConf * 10) / 10,
      tiers: {
        high: { count: high.length, pct: Math.round((high.length / columns.length) * 100) },
        moderate: { count: moderate.length, pct: Math.round((moderate.length / columns.length) * 100) },
        low: { count: low.length, pct: Math.round((low.length / columns.length) * 100) },
      },
      low_by_table: lowByTable,
      moderate_by_table: moderateByTable,
      has_snapshot: hasSnapshot,
      context_opportunity: (low.length + moderate.length) / columns.length > 0.2,
    });
  } catch (err) {
    console.error('Confidence summary error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/pipeline/:appId/save-user-knowledge
// Save user-entered column knowledge as a structured context document
router.post('/:appId/save-user-knowledge', async (req, res) => {
  try {
    const { appId } = req.params;
    const { entries } = req.body;
    // entries: [{ table_name, column_name, business_name, description }]

    if (!entries || !Array.isArray(entries) || entries.length === 0) {
      return res.status(400).json({ error: 'No knowledge entries provided' });
    }

    // Build CSV content
    const csvLines = ['Table,Column,Business Name,Description,Source'];
    for (const entry of entries) {
      const escapeCsv = (val) => {
        if (!val) return '';
        val = String(val);
        if (val.includes(',') || val.includes('"') || val.includes('\n')) {
          return '"' + val.replace(/"/g, '""') + '"';
        }
        return val;
      };
      csvLines.push([
        escapeCsv(entry.table_name),
        escapeCsv(entry.column_name),
        escapeCsv(entry.business_name),
        escapeCsv(entry.description || ''),
        'User Knowledge',
      ].join(','));
    }
    const csvContent = csvLines.join('\n');

    // Check if user_knowledge document already exists for this app
    const existingDoc = await query(
      `SELECT id, extracted_text FROM context_documents
       WHERE app_id = $1 AND filename = 'user_knowledge_entries.csv'`,
      [appId]
    );

    let docId;
    if (existingDoc.rows.length > 0) {
      // Merge: parse existing CSV, upsert new entries
      const existing = existingDoc.rows[0];
      const existingLines = existing.extracted_text.split('\n');
      const header = existingLines[0];
      const existingEntries = {};

      for (let i = 1; i < existingLines.length; i++) {
        const line = existingLines[i].trim();
        if (!line) continue;
        // Simple CSV parse (handles our own output format)
        const parts = line.split(',');
        const key = `${parts[0]}.${parts[1]}`.toLowerCase();
        existingEntries[key] = line;
      }

      // Upsert new entries
      for (const entry of entries) {
        const key = `${entry.table_name}.${entry.column_name}`.toLowerCase();
        const escapeCsv = (val) => {
          if (!val) return '';
          val = String(val);
          if (val.includes(',') || val.includes('"') || val.includes('\n')) {
            return '"' + val.replace(/"/g, '""') + '"';
          }
          return val;
        };
        existingEntries[key] = [
          escapeCsv(entry.table_name),
          escapeCsv(entry.column_name),
          escapeCsv(entry.business_name),
          escapeCsv(entry.description || ''),
          'User Knowledge',
        ].join(',');
      }

      const mergedCsv = [header, ...Object.values(existingEntries)].join('\n');

      await query(
        `UPDATE context_documents
         SET extracted_text = $1, file_size = $2, uploaded_at = NOW()
         WHERE id = $3`,
        [mergedCsv, Buffer.byteLength(mergedCsv), existing.id]
      );
      docId = existing.id;
    } else {
      // Create new document
      const insertResult = await query(
        `INSERT INTO context_documents (app_id, filename, file_type, file_size, extracted_text, description, uploaded_by, uploaded_at)
         VALUES ($1, 'user_knowledge_entries.csv', 'text/csv', $2, $3, 'Column knowledge entered directly by the builder operator via Context Coach', $4, NOW())
         RETURNING id`,
        [appId, Buffer.byteLength(csvContent), csvContent, req.user?.id || 1]
      );
      docId = insertResult.rows[0].id;
    }

    res.json({
      message: `Saved knowledge for ${entries.length} column${entries.length !== 1 ? 's' : ''}`,
      document_id: docId,
      entries_saved: entries.length,
    });
  } catch (err) {
    console.error('Save user knowledge error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/pipeline/:appId/enrich-only
// Triggers ONLY the AI enrichment stage — no reset, no ingest, no relationship deletion.
// Uses existing context documents and column metadata. Works without source data on server.
router.post('/:appId/enrich-only', async (req, res) => {
  const { appId } = req.params;
  try {
    const appCheck = await query('SELECT id, name, type, config FROM applications WHERE id = $1', [appId]);
    if (appCheck.rows.length === 0) return res.status(404).json({ error: 'Application not found' });
    const app = appCheck.rows[0];

    // Get tables and columns
    const tablesResult = await query(
      'SELECT id, table_name, entity_name FROM app_tables WHERE app_id = $1 ORDER BY table_name', [appId]);
    const tables = tablesResult.rows;
    if (tables.length === 0) return res.status(400).json({ error: 'No tables found' });

    const allTableNames = tables.map(t => t.entity_name || t.table_name);

    // Load context documents for enrichment
    const contextDocsResult = await query(
      'SELECT filename, extracted_text FROM context_documents WHERE app_id = $1 ORDER BY id', [appId]);
    const contextDocs = contextDocsResult.rows;

    // Build table-scoped context map
    function getTableContext(tableName) {
      const relevant = contextDocs.filter(d =>
        d.filename && d.filename.replace('.csv', '').toLowerCase() === tableName.toLowerCase()
      );
      return relevant.map(d => d.extracted_text).join('\n\n');
    }

    // Respond immediately — enrichment runs in background
    res.json({
      message: `Enrichment started for ${tables.length} tables`,
      app_id: appId,
      tables: tables.length,
      context_docs: contextDocs.length,
    });

    // Run enrichment in background
    console.log(`[enrich-only] Starting AI enrichment for app ${appId} (${app.name}): ${tables.length} tables, ${contextDocs.length} context docs`);

    let enriched = 0;
    let errors = 0;

    for (const table of tables) {
      const tableName = table.table_name;
      try {
        const columnsResult = await query(
          'SELECT id, column_name, data_type, is_pk, is_fk, fk_reference, business_name, description FROM app_columns WHERE table_id = $1',
          [table.id]
        );
        const columns = columnsResult.rows.map(c => ({
          id: c.id,
          column_name: c.column_name,
          data_type: c.data_type,
          is_pk: c.is_pk,
          is_fk: c.is_fk,
          fk_reference: c.fk_reference,
          business_name: c.business_name,
          description: c.description,
        }));

        console.log(`[enrich-only] Enriching "${tableName}" (${columns.length} cols)...`);

        const enrichContext = {
          app_name: app.name,
          app_type: app.type,
          related_tables: allTableNames.filter(t => t !== tableName).slice(0, 20),
          context_documents: getTableContext(tableName),
          domain_taxonomy: null,
        };

        // Batch large tables to avoid Claude API timeout (>40 columns)
        const BATCH_SIZE = 30;
        let allEnrichedCols = [];
        let tableResult = null;

        if (columns.length <= BATCH_SIZE + 10) {
          // Small enough to do in one call
          const result = await enrichTable(tableName, columns, enrichContext, null);
          allEnrichedCols = result.columns || [];
          tableResult = result;
        } else {
          // Batch: split columns into chunks, enrich each
          console.log(`[enrich-only] Large table — batching ${columns.length} cols into chunks of ${BATCH_SIZE}`);
          for (let i = 0; i < columns.length; i += BATCH_SIZE) {
            const batch = columns.slice(i, i + BATCH_SIZE);
            const batchNum = Math.floor(i / BATCH_SIZE) + 1;
            const totalBatches = Math.ceil(columns.length / BATCH_SIZE);
            console.log(`[enrich-only]   Batch ${batchNum}/${totalBatches}: cols ${i+1}-${i+batch.length}`);
            try {
              const batchResult = await enrichTable(tableName, batch, enrichContext, null);
              allEnrichedCols.push(...(batchResult.columns || []));
              // Use first batch for table-level enrichment
              if (!tableResult) tableResult = batchResult;
            } catch (batchErr) {
              console.error(`[enrich-only]   Batch ${batchNum} failed: ${batchErr.message}`);
              // Mark these columns as needs_review rather than losing them
              for (const col of batch) {
                allEnrichedCols.push({ ...col, confidence_score: 40, enrichment_status: 'needs_review' });
              }
            }
          }
        }

        if (!tableResult) tableResult = { columns: allEnrichedCols };

        // Save column enrichments
        for (const enrichedCol of allEnrichedCols) {
          if (!enrichedCol.id) continue;
          const status = tableResult.enrichment_status === 'failed' ? 'draft'
            : enrichedCol.confidence_score >= 70 ? 'ai_enriched' : 'needs_review';
          const valueMapping = enrichedCol.value_dictionary ? JSON.stringify(enrichedCol.value_dictionary) : null;

          await query(
            `UPDATE app_columns SET business_name = $1, description = $2, confidence_score = $3,
             enrichment_status = $4, enriched_by = 'ai', enriched_at = NOW(), value_mapping = COALESCE($5, value_mapping),
             column_role = $6 WHERE id = $7`,
            [enrichedCol.business_name, enrichedCol.description, enrichedCol.confidence_score,
             status, valueMapping, enrichedCol.column_role || null, enrichedCol.id]
          );
        }

        // Save table-level enrichment
        const entityMetadata = {
          entity_type: tableResult.entity_type || 'UNKNOWN',
          domain: tableResult.module || tableResult.domain || 'General',
          business_name: tableResult.table_description ? (table.entity_name || tableName) : null,
          sample_questions: tableResult.sample_questions || [],
          computed_measures: tableResult.computed_measures || [],
        };
        await query(
          `UPDATE app_tables SET description = $1, entity_metadata = $2, enrichment_status = 'ai_enriched',
           enriched_by = 'ai', enriched_at = NOW() WHERE id = $3`,
          [tableResult.table_description || '', JSON.stringify(entityMetadata), table.id]
        );

        enriched++;
        console.log(`[enrich-only] ✓ ${tableName} enriched (${enriched}/${tables.length})`);
      } catch (err) {
        errors++;
        console.error(`[enrich-only] ✗ ${tableName} failed:`, err.message);
      }
    }

    console.log(`[enrich-only] Complete: ${enriched} enriched, ${errors} errors`);

    // Auto-publish after enrichment
    await query("UPDATE applications SET status = 'published', updated_at = NOW() WHERE id = $1", [appId]);

  } catch (err) {
    console.error('Enrich-only error:', err);
    if (!res.headersSent) {
      res.status(500).json({ error: err.message });
    }
  }
});

// ============================================================
//  GUIDED WORKFLOW — Run individual pipeline steps
// ============================================================

// Shared step-runner state (keyed by appId)
const activeStepRuns = {};

// POST /api/pipeline/:appId/run-step
//   Body: { step: 'ingest' | 'profile' | 'discover' | 'enrich' | 'synonyms' | 'validate' | 'publish' }
//   Runs a single pipeline step and returns results
router.post('/:appId/run-step', async (req, res) => {
  try {
    const { appId } = req.params;
    const { step, action } = req.body;  // action: 'complete', 'skip', 'reset' for human steps

    if (!step) return res.status(400).json({ error: 'step is required' });

    const appResult = await query('SELECT id, name, type, status, config FROM applications WHERE id = $1', [appId]);
    if (appResult.rows.length === 0) return res.status(404).json({ error: 'Application not found' });
    const app = appResult.rows[0];

    // Handle human step actions (context, curate)
    if (action === 'complete' || action === 'skip') {
      const status = action === 'skip' ? 'skipped' : 'completed';
      await query(
        `UPDATE pipeline_steps SET status = $1, completed_at = NOW(), updated_at = NOW() WHERE app_id = $2 AND step_name = $3`,
        [status, appId, step]
      );
      return res.json({ step, success: true, action, guidance: action === 'skip' ? `${step} step skipped.` : `${step} step marked as complete.` });
    }

    if (action === 'reset') {
      // Reset this step and all downstream steps
      const STEP_ORDER = ['connect', 'profile', 'discover', 'context', 'enrich', 'synonyms', 'curate', 'index', 'validate', 'publish'];
      const stepIdx = STEP_ORDER.indexOf(step);
      const downstreamSteps = STEP_ORDER.slice(stepIdx);
      for (let i = 0; i < downstreamSteps.length; i++) {
        const ds = downstreamSteps[i];
        const dsOrder = stepIdx + i + 1;
        // Use upsert to ensure pipeline_steps row exists even for apps that predate the workflow
        await query(
          `INSERT INTO pipeline_steps (app_id, step_name, step_order, status, quality_score, quality_details, started_at, completed_at, updated_at)
           VALUES ($1, $2, $3, 'not_started', NULL, NULL, NULL, NULL, NOW())
           ON CONFLICT (app_id, step_name) DO UPDATE SET
             status = 'not_started', quality_score = NULL, quality_details = NULL,
             started_at = NULL, completed_at = NULL, updated_at = NOW()`,
          [appId, ds, dsOrder]
        );
      }
      // Step-specific data cleanup when resetting
      if (downstreamSteps.includes('enrich')) {
        // Reset column enrichment data back to draft so re-enrichment processes them
        await query(
          `UPDATE app_columns
           SET business_name = NULL, description = NULL, confidence_score = 0,
               enrichment_status = 'draft', enriched_by = NULL, enriched_at = NULL
           WHERE table_id IN (SELECT id FROM app_tables WHERE app_id = $1)`,
          [appId]
        );
        // Reset table descriptions and entity metadata
        await query(
          `UPDATE app_tables SET description = NULL, entity_metadata = NULL,
                  enrichment_status = 'draft', enriched_by = NULL, enriched_at = NULL
           WHERE app_id = $1`,
          [appId]
        );
        // Clear enrichment-dependent data
        await query('DELETE FROM query_patterns WHERE app_id = $1', [appId]);
        await query('DELETE FROM test_queries WHERE app_id = $1', [appId]);
        await query('DELETE FROM pipeline_runs WHERE app_id = $1', [appId]);
        console.log(`[Workflow] Reset enrichment data for app ${appId}`);
      }
      if (downstreamSteps.includes('synonyms')) {
        // Deactivate AI-generated synonyms (keep manual ones)
        await query(
          `UPDATE app_synonyms SET status = 'inactive' WHERE app_id = $1 AND source = 'ai_generated'`,
          [appId]
        );
      }
      // Mark KG as unpublished if any step before publish is reset
      if (stepIdx < STEP_ORDER.indexOf('publish')) {
        await query("UPDATE applications SET status = 'draft', updated_at = NOW() WHERE id = $1 AND status = 'published'", [appId]);
      }
      return res.json({ step, success: true, action: 'reset', kgStatusChanged: app.status === 'published',
        guidance: `Step "${step}" and all downstream steps have been reset.${app.status === 'published' ? ' Knowledge graph marked as unpublished.' : ''}` });
    }

    // Prevent concurrent step runs on same app
    if (activeStepRuns[appId]) {
      return res.status(409).json({ error: `Step "${activeStepRuns[appId]}" is already running for this app` });
    }
    activeStepRuns[appId] = step;

    // Check dependencies
    const STEP_DEPS = {
      connect: [],
      profile: ['connect'],
      discover: ['connect', 'profile'],
      context: ['connect', 'profile', 'discover'],
      enrich: ['connect', 'profile', 'discover'],
      synonyms: ['connect', 'profile', 'discover', 'enrich'],
      curate: ['connect', 'profile', 'discover', 'enrich'],
      index: ['connect', 'profile', 'discover', 'enrich', 'curate'],
      validate: ['connect', 'profile', 'discover', 'enrich', 'curate', 'index'],
      publish: ['connect', 'profile', 'discover', 'enrich', 'curate', 'index', 'validate'],
    };
    const deps = STEP_DEPS[step] || [];
    if (deps.length > 0) {
      const depCheck = await query(
        `SELECT step_name, status FROM pipeline_steps WHERE app_id = $1 AND step_name = ANY($2)`,
        [appId, deps]
      );
      const incomplete = deps.filter(d => {
        const row = depCheck.rows.find(r => r.step_name === d);
        return !row || (row.status !== 'completed' && row.status !== 'skipped');
      });
      if (incomplete.length > 0) {
        delete activeStepRuns[appId];
        return res.status(400).json({ error: `Prerequisites not met: ${incomplete.join(', ')}`, missingDeps: incomplete });
      }
    }

    // If re-running a previously completed step, mark KG as unpublished
    let kgStatusChanged = false;
    if (app.status === 'published' && step !== 'publish') {
      const currentStep = await query('SELECT status FROM pipeline_steps WHERE app_id = $1 AND step_name = $2', [appId, step]);
      if (currentStep.rows[0]?.status === 'completed') {
        await query("UPDATE applications SET status = 'draft', updated_at = NOW() WHERE id = $1", [appId]);
        kgStatusChanged = true;
        // Also reset downstream steps
        const STEP_ORDER = ['connect', 'profile', 'discover', 'context', 'enrich', 'synonyms', 'curate', 'index', 'validate', 'publish'];
        const stepIdx = STEP_ORDER.indexOf(step);
        const downstreamSteps = STEP_ORDER.slice(stepIdx + 1);
        for (const ds of downstreamSteps) {
          await query(
            `UPDATE pipeline_steps SET status = 'not_started', quality_score = NULL, quality_details = NULL, started_at = NULL, completed_at = NULL, updated_at = NOW() WHERE app_id = $1 AND step_name = $2`,
            [appId, ds]
          );
        }
      }
    }

    let result = {};

    try {
      switch (step) {

        // ── STEP 1: CONNECT & LOAD ──
        case 'ingest':
        case 'connect': {
          const sqlitePath = findSqlitePath(app);
          let sourceAvailable = await hasSourceData(appId);
          let loaded = false;

          // If SQLite file exists, always load (or reload) from it
          if (sqlitePath) {
            console.log(`[run-step connect] Loading from SQLite: ${sqlitePath}`);
            await loadSqliteToPostgres(appId, sqlitePath, { dropExisting: true });
            loaded = true;
            sourceAvailable = true;
          } else if (!sourceAvailable) {
            // No SQLite file AND no existing data — check if metadata exists (from prior benchmark run)
            const metaCheck = await query('SELECT COUNT(*) as cnt FROM app_tables WHERE app_id = $1', [appId]);
            if (parseInt(metaCheck.rows[0].cnt) > 0) {
              console.log(`[run-step connect] No SQLite file, but metadata exists (${metaCheck.rows[0].cnt} tables). Reporting as connected.`);
            }
          }

          if (sourceAvailable) {
            await syncColumnsFromSource(appId).catch(e => console.warn('Sync warning:', e.message));
          }
          const tableCount = await query('SELECT COUNT(*) as cnt FROM app_tables WHERE app_id = $1', [appId]);
          const colCount = await query(
            `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1`, [appId]
          );
          // Get table listing with row counts and column counts
          const tableList = await query(
            `SELECT at.table_name, at.row_count,
                    COUNT(ac.id) as column_count
             FROM app_tables at
             LEFT JOIN app_columns ac ON at.id = ac.table_id
             WHERE at.app_id = $1
             GROUP BY at.id, at.table_name, at.row_count
             ORDER BY COALESCE(at.row_count, 0) DESC`, [appId]
          );
          const emptyTables = tableList.rows.filter(t => !t.row_count || parseInt(t.row_count) === 0);
          const t = parseInt(tableCount.rows[0].cnt);
          const c = parseInt(colCount.rows[0].cnt);
          const totalRows = tableList.rows.reduce((sum, t) => sum + (parseInt(t.row_count) || 0), 0);
          result = {
            success: t > 0,
            freshLoad: loaded,
            sourceAvailable,
            sourceType: app.type || 'SQLite',
            sourcePath: sqlitePath || null,
            tables: t,
            columns: c,
            totalRows,
            tableList: tableList.rows.slice(0, 20), // top 20 for display
            emptyTables: emptyTables.map(et => ({ table_name: et.table_name, column_count: parseInt(et.column_count) })),
            emptyTableCount: emptyTables.length,
            guidance: t > 0 && loaded
              ? `Loaded ${t} tables with ${c} columns from ${app.type || 'SQLite'} source (${totalRows.toLocaleString()} total rows).${emptyTables.length > 0 ? ` ⚠ ${emptyTables.length} empty tables flagged for review.` : ''} Review the Schema Explorer to confirm all expected tables are present.`
              : t > 0 && sourceAvailable
              ? `Source data already loaded: ${t} tables, ${c} columns (${totalRows.toLocaleString()} rows). No reload needed.`
              : t > 0
              ? `Schema metadata present: ${t} tables, ${c} columns. Source data file not found on server — data loading may need to be run from the Pipeline page. Schema and enrichments are available for review.`
              : `No source data or metadata found. Use the data-source API (PUT /api/pipeline/${appId}/data-source) to set the SQLite path, or add the .sqlite.gz file to the data/ folder and redeploy.`,
          };
          await query(
            `UPDATE pipeline_steps SET status = $1, quality_score = $2, quality_details = $3, completed_at = NOW(), updated_at = NOW() WHERE app_id = $4 AND step_name = 'connect'`,
            [t > 0 ? 'completed' : 'needs_attention', t > 0 ? 100 : 0, JSON.stringify({ tables: t, columns: c, totalRows, emptyTables: emptyTables.length }), appId]
          );
          await query(
            `UPDATE pipeline_steps SET status = $1, quality_score = $2, quality_details = $3, completed_at = NOW(), updated_at = NOW() WHERE app_id = $4 AND step_name = 'load'`,
            [t > 0 ? 'completed' : 'needs_attention', t > 0 ? 100 : 0, JSON.stringify({ tables: t, columns: c, totalRows, emptyTables: emptyTables.length }), appId]
          );
          break;
        }

        case 'load': {
          // Same as ingest — shared step
          const tables = await query('SELECT COUNT(*) as cnt FROM app_tables WHERE app_id = $1', [appId]);
          const cols = await query(
            `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1`, [appId]
          );
          const t = parseInt(tables.rows[0].cnt);
          const c = parseInt(cols.rows[0].cnt);
          result = { success: true, tables: t, columns: c,
            guidance: t > 0
              ? `${t} tables and ${c} columns loaded. Review in Schema Explorer before proceeding.`
              : 'No tables loaded yet. Run Connect step first or check data source configuration.'
          };
          await query(
            `UPDATE pipeline_steps SET status = $1, quality_score = $2, quality_details = $3, completed_at = NOW(), updated_at = NOW() WHERE app_id = $4 AND step_name = 'load'`,
            [t > 0 ? 'completed' : 'needs_attention', t > 0 ? 100 : 0, JSON.stringify(result), appId]
          );
          break;
        }

        // ── STEP 2: PROFILE ──
        case 'profile': {
          const tablesResult = await query(
            'SELECT id, table_name, entity_name FROM app_tables WHERE app_id = $1 ORDER BY table_name', [appId]
          );
          const sourceTables = tablesResult.rows;
          const sourceAvail = await hasSourceData(appId);
          let profiledCount = 0;
          const appCfg = typeof app.config === 'string' ? JSON.parse(app.config) : (app.config || {});
          const sampleRows = (appCfg.pipeline || {}).sample_row_count || 10;

          if (sourceAvail && sourceTables.length > 0) {
            for (const table of sourceTables) {
              try {
                const colNames = (await query('SELECT column_name FROM app_columns WHERE table_id = $1', [table.id])).rows.map(r => r.column_name);
                const profile = await profileTable(appId, table.table_name, colNames, sampleRows);
                await query('UPDATE app_tables SET row_count = $1 WHERE id = $2', [profile.row_count, table.id]);
                profiledCount++;
              } catch (e) { console.warn(`Profile skip ${table.table_name}: ${e.message}`); }
            }
          }

          // If source data wasn't available, check for existing profile data in AKG
          // (from prior pipeline runs or benchmark seeding)
          let existingProfileCount = 0;
          if (profiledCount === 0 && sourceTables.length > 0) {
            const existingProfiles = await query(
              `SELECT COUNT(*) as cnt FROM app_tables WHERE app_id = $1 AND row_count IS NOT NULL AND row_count > 0`, [appId]
            );
            existingProfileCount = parseInt(existingProfiles.rows[0].cnt);
            if (existingProfileCount > 0) {
              console.log(`[run-step profile] Source data unavailable but ${existingProfileCount}/${sourceTables.length} tables have existing profile data.`);
            }
          }

          const total = sourceTables.length;
          const effectiveProfiled = profiledCount > 0 ? profiledCount : existingProfileCount;
          const score = total > 0 ? Math.round((effectiveProfiled / total) * 100) : 0;
          // Count columns with profiling data
          const profiledCols = await query(
            `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
             WHERE at.app_id = $1 AND (ac.data_type IS NOT NULL OR ac.value_mapping IS NOT NULL)`, [appId]
          );
          const totalCols = await query(
            `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1`, [appId]
          );
          // Get top tables by row count for display
          const topTables = await query(
            `SELECT table_name, row_count FROM app_tables WHERE app_id = $1 AND row_count IS NOT NULL ORDER BY row_count DESC LIMIT 10`, [appId]
          );
          // Detect temporal/date columns
          const dateCols = await query(
            `SELECT ac.column_name, at.table_name FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
             WHERE at.app_id = $1 AND LOWER(ac.data_type) IN ('date', 'timestamp', 'datetime', 'timestamptz')`, [appId]
          );
          // Detect low-cardinality columns (value_mapping has content)
          const lowCardCols = await query(
            `SELECT ac.column_name, at.table_name, ac.value_mapping FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
             WHERE at.app_id = $1 AND ac.value_mapping IS NOT NULL AND ac.value_mapping != '{}'
             ORDER BY at.table_name LIMIT 20`, [appId]
          );

          const stepSuccess = profiledCount > 0 || existingProfileCount > 0;
          result = {
            success: stepSuccess, profiledTables: profiledCount, totalTables: total,
            existingProfiles: existingProfileCount,
            profiledColumns: parseInt(profiledCols.rows[0].cnt),
            totalColumns: parseInt(totalCols.rows[0].cnt),
            topTables: topTables.rows,
            dateColumns: dateCols.rows,
            dateColumnCount: dateCols.rows.length,
            lowCardColumns: lowCardCols.rows.map(r => ({ column: r.column_name, table: r.table_name })),
            lowCardCount: lowCardCols.rows.length,
            guidance: profiledCount > 0
              ? `Profiled ${profiledCount}/${total} tables. ${dateCols.rows.length} date/temporal columns detected. ${lowCardCols.rows.length} low-cardinality columns will get value dictionaries during enrichment. Review in Data Browser.`
              : existingProfileCount > 0
              ? `Using existing profile data for ${existingProfileCount}/${total} tables (source data not available for re-profiling). ${dateCols.rows.length} date/temporal columns, ${lowCardCols.rows.length} low-cardinality columns. Review in Data Browser.`
              : 'No tables profiled. Ensure source data is loaded (run Connect & Load first).',
          };
          await query(
            `UPDATE pipeline_steps SET status = $1, quality_score = $2, quality_details = $3, completed_at = NOW(), updated_at = NOW() WHERE app_id = $4 AND step_name = 'profile'`,
            [stepSuccess ? 'completed' : 'needs_attention', score, JSON.stringify({ profiledTables: profiledCount, existingProfiles: existingProfileCount, totalTables: total, dateCols: dateCols.rows.length, lowCardCols: lowCardCols.rows.length }), appId]
          );
          break;
        }

        // ── STEP 3: DISCOVER ──
        case 'discover': {
          // Run relationship detection (same logic as pipeline Stage 3)
          const tablesResult = await query(
            'SELECT id, table_name, entity_name FROM app_tables WHERE app_id = $1 ORDER BY table_name', [appId]
          );
          const discTables = tablesResult.rows;
          await query('DELETE FROM app_relationships WHERE app_id = $1', [appId]);

          // Build lookup
          const tblLookup = {};
          for (const t of discTables) {
            const pkCols = await query('SELECT column_name FROM app_columns WHERE table_id = $1 AND is_pk = true', [t.id]);
            tblLookup[t.table_name.toLowerCase()] = { table: t, pkColumns: pkCols.rows.map(r => r.column_name) };
          }

          // Strategy 1+2: FK flag + naming convention
          for (const table of discTables) {
            const allCols = await query('SELECT id, column_name, is_fk, fk_reference FROM app_columns WHERE table_id = $1', [table.id]);
            for (const col of allCols.rows) {
              let targetEntry = null, toColumn = null;
              if (col.is_fk) {
                const baseName = col.column_name.replace(/_id$/i, '').replace(/Id$/, '');
                const fkRef = col.fk_reference || '';
                targetEntry = tblLookup[baseName.toLowerCase()] || tblLookup[baseName.toLowerCase() + 's']
                  || Object.values(tblLookup).find(e => fkRef.toLowerCase().includes(e.table.table_name.toLowerCase()));
              }
              if (!targetEntry) {
                const camelMatch = col.column_name.match(/^([a-z]+)Id$/);
                const snakeMatch = col.column_name.match(/^([a-z_]+)_id$/i);
                const baseName = camelMatch ? camelMatch[1] : (snakeMatch ? snakeMatch[1] : null);
                if (baseName) {
                  const ownPKs = tblLookup[table.table_name.toLowerCase()]?.pkColumns || [];
                  const isSolePK = ownPKs.includes(col.column_name) && ownPKs.length === 1;
                  const matchesOwn = baseName.toLowerCase() === table.table_name.toLowerCase();
                  if (!(isSolePK || (ownPKs.includes(col.column_name) && matchesOwn))) {
                    targetEntry = tblLookup[baseName.toLowerCase()] || tblLookup[baseName.toLowerCase() + 's'];
                  }
                }
              }
              if (targetEntry && targetEntry.table.id !== table.id) {
                toColumn = targetEntry.pkColumns[0] || col.column_name;
                await query(
                  `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality)
                   VALUES ($1, $2, $3, $4, $5, 'fk', 'many_to_one') ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING`,
                  [appId, table.id, col.column_name, targetEntry.table.id, toColumn]
                );
                if (!col.is_fk) await query('UPDATE app_columns SET is_fk = true WHERE id = $1', [col.id]);
              }
            }
          }

          // Strategy 3: Shared column name
          const colsByName = {};
          const tblColCounts = {};
          for (const t of discTables) {
            const cols = await query('SELECT id, column_name, is_pk, is_fk FROM app_columns WHERE table_id = $1', [t.id]);
            tblColCounts[t.id] = cols.rows.length;
            for (const c of cols.rows) {
              const key = c.column_name.toLowerCase();
              if (!colsByName[key]) colsByName[key] = [];
              colsByName[key].push({ table: t, col: c, isPK: c.is_pk, isFK: c.is_fk });
            }
          }
          for (const [cn, entries] of Object.entries(colsByName)) {
            if (entries.length < 2) continue;
            const pkEntries = entries.filter(e => e.isPK);
            if (pkEntries.length === 0) continue;
            const pkEntry = pkEntries.reduce((best, e) => (tblColCounts[e.table.id] || 0) > (tblColCounts[best.table.id] || 0) ? e : best);
            for (const entry of entries) {
              if (entry.table.id === pkEntry.table.id) continue;
              const card = entry.isPK ? 'one_to_one' : 'many_to_one';
              await query(
                `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality)
                 VALUES ($1, $2, $3, $4, $5, 'fk', $6) ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING`,
                [appId, entry.table.id, entry.col.column_name, pkEntry.table.id, pkEntry.col.column_name, card]
              );
              if (!entry.isFK) await query('UPDATE app_columns SET is_fk = true WHERE id = $1', [entry.col.id]);
            }
          }

          // Strategy 3b: Shared column matching for schemas WITHOUT declared PKs
          // (SAP, data warehouses, flat-file exports — same logic as full pipeline Strategy 3b)
          console.log('  Strategy 3b: shared column matching (no-PK, value-overlap confirmed)...');
          try {
            const s3bSrcAvail = await hasSourceData(appId);
            if (s3bSrcAvail) {
              const { executeOnSourceData: execS3b } = require('../services/data-loader');
              const totalTblCnt = discTables.length;
              const ubiqThresh = Math.max(5, Math.floor(totalTblCnt * 0.5));
              const skipCols = new Set([
                'mandt', 'id', 'name', 'description', 'status', 'type',
                'created_at', 'updated_at', 'created_by', 'modified_by',
                'last_update_date', 'creation_date', 'last_updated_by',
                'record_id', 'row_id', 'seq', 'sequence', 'sort_order',
                'is_active', 'is_deleted', 'flag', 'comments', 'notes',
                'remark', 'remarks', 'erdat', 'ernam', 'aedat', 'aenam',
                'loekz', 'spras', 'budat', 'bldat', 'cpudt', 'cputm', 'usnam', 'tcode',
              ]);

              const existRels3b = await query(
                `SELECT from_table_id, from_column, to_table_id, to_column FROM app_relationships WHERE app_id = $1`, [appId]
              );
              const existSet3b = new Set(existRels3b.rows.map(r => `${r.from_table_id}:${r.from_column}:${r.to_table_id}:${r.to_column}`));

              // Pre-fetch row counts
              const rc3b = {};
              for (const t of discTables) {
                const rcRow = await query('SELECT row_count FROM app_tables WHERE id = $1', [t.id]);
                rc3b[t.id] = parseInt(rcRow.rows[0]?.row_count || 0);
              }

              let s3bCnt = 0;
              for (const [cn, entries] of Object.entries(colsByName)) {
                if (entries.some(e => e.isPK)) continue;
                if (entries.length < 2 || entries.length > ubiqThresh) continue;
                if (skipCols.has(cn) || cn.length < 3) continue;

                const withData = entries.filter(e => (rc3b[e.table.id] || 0) > 0);
                if (withData.length < 2) continue;

                // Find anchor table (highest uniqueness ratio)
                let bestR = 0, anchor = null;
                for (const entry of withData) {
                  try {
                    const dr = await execS3b(appId,
                      `SELECT COUNT(DISTINCT "${entry.col.column_name}") as d, COUNT(*) as t
                       FROM "${entry.table.table_name}" WHERE "${entry.col.column_name}" IS NOT NULL`,
                      { timeout: 5000 });
                    const d = parseInt(dr.rows[0]?.d || 0), t = parseInt(dr.rows[0]?.t || 0);
                    if (t === 0 || d === 0) continue;
                    const r = d / t;
                    if (r > bestR || (r === bestR && anchor && t < anchor.tc)) {
                      bestR = r; anchor = { ...entry, dc: d, tc: t };
                    }
                  } catch (e) { /* skip */ }
                }
                if (!anchor || bestR < 0.5) continue;

                for (const entry of withData) {
                  if (entry.table.id === anchor.table.id) continue;
                  const rk = `${entry.table.id}:${entry.col.column_name}:${anchor.table.id}:${anchor.col.column_name}`;
                  const rvk = `${anchor.table.id}:${anchor.col.column_name}:${entry.table.id}:${entry.col.column_name}`;
                  if (existSet3b.has(rk) || existSet3b.has(rvk)) continue;

                  try {
                    const cs = await execS3b(appId,
                      `SELECT DISTINCT "${entry.col.column_name}" as val FROM "${entry.table.table_name}"
                       WHERE "${entry.col.column_name}" IS NOT NULL LIMIT 50`, { timeout: 5000 });
                    if (cs.rows.length === 0) continue;
                    const vals = cs.rows.map(r => r.val);
                    const esc = vals.map(v => v === null ? 'NULL' : `'${String(v).replace(/'/g, "''")}'`).join(', ');
                    const am = await execS3b(appId,
                      `SELECT COUNT(DISTINCT "${anchor.col.column_name}") as cnt FROM "${anchor.table.table_name}"
                       WHERE "${anchor.col.column_name}" IN (${esc})`, { timeout: 5000 });
                    const mc = parseInt(am.rows[0]?.cnt || 0);
                    const or2 = mc / vals.length;
                    const minM = Math.min(2, vals.length);
                    if (or2 >= 0.5 && mc >= minM) {
                      const conf = Math.min(85, Math.round(or2 * 100));
                      await query(
                        `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality, confidence_score)
                         VALUES ($1, $2, $3, $4, $5, 'inferred', 'many_to_one', $6)
                         ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING`,
                        [appId, entry.table.id, entry.col.column_name, anchor.table.id, anchor.col.column_name, conf]);
                      existSet3b.add(rk);
                      s3bCnt++;
                      console.log(`    Shared-col: ${entry.table.table_name}.${entry.col.column_name} → ${anchor.table.table_name}.${anchor.col.column_name} (${Math.round(or2 * 100)}%)`);
                    }
                  } catch (e) { /* skip */ }
                }
              }
              console.log(`  Strategy 3b: found ${s3bCnt} new relationships`);
            } else {
              console.log('  Strategy 3b skipped — no source data');
            }
          } catch (s3bE) {
            strategyLog.s3b = `error:${s3bE.message}`;
            console.warn(`  Strategy 3b warning: ${s3bE.message}`);
          }

          // Strategy 4: Application-agnostic metadata FK resolution
          console.log('  Strategy 4: metadata-driven FK resolution...');
          try {
            const s4avail = await hasSourceData(appId);
            if (s4avail) {
              const { executeOnSourceData: execS4 } = require('../services/data-loader');
              const allTblNamesLower = discTables.map(t => (t.entity_name || t.table_name).toLowerCase());

              const FK_PATTERNS = [
                { table: 'fnd_resolved_fks', fromT: 'FROM_TABLE', fromC: 'FROM_COLUMN', toT: 'TO_TABLE', toC: 'TO_COLUMN',
                  filter: `COALESCE("ENABLED_FLAG", 'Y') != 'N'` },
                { table: 'dd05s', fromT: 'TABNAME', fromC: 'FIELDNAME', toT: 'CHECKTABLE', toC: 'CHECKFIELD' },
                { table: 'dd08l', fromT: 'TABNAME', fromC: 'FIELDNAME', toT: 'CHECKTABLE', toC: 'CHECKFIELD' },
                { table: 'psrecfielddb', fromT: 'RECNAME', fromC: 'FIELDNAME', toT: 'REFTBLRECNAME', toC: null },
              ];

              const srcTbls = await execS4(appId,
                `SELECT LOWER(table_name) as tbl FROM information_schema.tables WHERE table_schema = 'appdata_${appId}'`);
              const srcSet = new Set(srcTbls.rows.map(r => r.tbl));

              let s4cnt = 0;
              for (const pat of FK_PATTERNS) {
                if (!srcSet.has(pat.table)) continue;
                console.log(`    Found FK metadata: ${pat.table}`);
                try {
                  const fc = pat.filter ? `AND ${pat.filter}` : '';
                  const tcSel = pat.toC ? `, "${pat.toC}" as to_column` : '';
                  const fkr = await execS4(appId,
                    `SELECT "${pat.fromT}" as from_table, "${pat.fromC}" as from_column,
                            "${pat.toT}" as to_table ${tcSel}
                     FROM "${pat.table.toUpperCase()}"
                     WHERE "${pat.fromT}" IS NOT NULL AND "${pat.toT}" IS NOT NULL ${fc}`,
                    { maxRows: 0, timeout: 60000 });
                  for (const row of fkr.rows) {
                    if (!allTblNamesLower.includes(row.from_table?.toLowerCase()) ||
                        !allTblNamesLower.includes(row.to_table?.toLowerCase())) continue;
                    const fe = tblLookup[row.from_table.toLowerCase()];
                    const te = tblLookup[row.to_table.toLowerCase()];
                    if (!fe || !te || fe.table.id === te.table.id) continue;
                    await query(
                      `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality, confidence_score)
                       VALUES ($1, $2, $3, $4, $5, 'fk', 'many_to_one', 95)
                       ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING`,
                      [appId, fe.table.id, row.from_column, te.table.id, row.to_column || row.from_column]);
                    s4cnt++;
                  }
                  console.log(`    ${pat.table}: ${fkr.rows.length} entries, ${s4cnt} matched`);
                } catch (pe) { console.warn(`    ${pat.table} failed: ${pe.message}`); }
              }

              // Oracle EBS raw FND 6-table JOIN fallback
              if (srcSet.has('fnd_foreign_keys') && !srcSet.has('fnd_resolved_fks')) {
                try {
                  const fndRaw = await execS4(appId, `
                    SELECT ft_from."TABLE_NAME" as from_table, fc_from."COLUMN_NAME" as from_column,
                           ft_to."TABLE_NAME" as to_table, COALESCE(fc_to."COLUMN_NAME", fc_from."COLUMN_NAME") as to_column
                    FROM "FND_FOREIGN_KEYS" fk
                    JOIN "FND_FOREIGN_KEY_COLUMNS" fkc ON fkc."FOREIGN_KEY_ID" = fk."FOREIGN_KEY_ID" AND fkc."TABLE_ID" = fk."TABLE_ID"
                    JOIN "FND_TABLES" ft_from ON ft_from."TABLE_ID" = fk."TABLE_ID"
                    JOIN "FND_TABLES" ft_to ON ft_to."TABLE_ID" = fk."PRIMARY_KEY_TABLE_ID"
                    JOIN "FND_COLUMNS" fc_from ON fc_from."COLUMN_ID" = fkc."COLUMN_ID" AND fc_from."TABLE_ID" = fk."TABLE_ID"
                    LEFT JOIN "FND_PRIMARY_KEY_COLUMNS" pkc ON pkc."PRIMARY_KEY_ID" = fk."PRIMARY_KEY_ID"
                      AND pkc."PRIMARY_KEY_SEQUENCE" = fkc."FOREIGN_KEY_SEQUENCE" AND pkc."TABLE_ID" = fk."PRIMARY_KEY_TABLE_ID"
                    LEFT JOIN "FND_COLUMNS" fc_to ON fc_to."COLUMN_ID" = pkc."COLUMN_ID" AND fc_to."TABLE_ID" = fk."PRIMARY_KEY_TABLE_ID"
                    WHERE fk."ENABLED_FLAG" IS DISTINCT FROM 'N'`, { maxRows: 0, timeout: 120000 });
                  for (const row of fndRaw.rows) {
                    if (!allTblNamesLower.includes(row.from_table?.toLowerCase()) ||
                        !allTblNamesLower.includes(row.to_table?.toLowerCase())) continue;
                    const fe = tblLookup[row.from_table.toLowerCase()];
                    const te = tblLookup[row.to_table.toLowerCase()];
                    if (!fe || !te || fe.table.id === te.table.id) continue;
                    await query(
                      `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality, confidence_score)
                       VALUES ($1, $2, $3, $4, $5, 'fk', 'many_to_one', 95)
                       ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING`,
                      [appId, fe.table.id, row.from_column, te.table.id, row.to_column]);
                    s4cnt++;
                  }
                } catch (fndE) { console.warn(`    FND raw JOIN: ${fndE.message}`); }
              }
              if (s4cnt > 0) console.log(`  Strategy 4 total: ${s4cnt} FK relationships`);
              else console.log('  Strategy 4: no FK metadata tables found');
            }
          } catch (s4E) {
            console.warn(`  Strategy 4 warning: ${s4E.message}`);
          }

          const relCount = await query('SELECT COUNT(*) as cnt FROM app_relationships WHERE app_id = $1', [appId]);
          const rels = parseInt(relCount.rows[0].cnt);
          const tCount = discTables.length;
          const score = tCount > 1 ? Math.min(100, Math.round((rels / (tCount - 1)) * 100)) : (rels > 0 ? 100 : 0);

          // Get relationship details for display
          const relDetails = await query(
            `SELECT ft.table_name as from_table, ar.from_column, tt.table_name as to_table, ar.to_column, ar.cardinality
             FROM app_relationships ar
             JOIN app_tables ft ON ar.from_table_id = ft.id
             JOIN app_tables tt ON ar.to_table_id = tt.id
             WHERE ar.app_id = $1 ORDER BY ft.table_name LIMIT 25`, [appId]
          );

          result = {
            success: true, relationships: rels, tables: tCount,
            details: relDetails.rows,
            guidance: rels > 0
              ? `Found ${rels} relationships across ${tCount} tables. Review in Knowledge Graph to verify join paths are correct before enrichment.`
              : 'No relationships detected. Check if tables share column names or have declared foreign keys.',
          };
          await query(
            `UPDATE pipeline_steps SET status = $1, quality_score = $2, quality_details = $3, completed_at = NOW(), updated_at = NOW() WHERE app_id = $4 AND step_name = 'discover'`,
            [rels > 0 ? 'completed' : 'needs_attention', score, JSON.stringify({ relationships: rels, tables: tCount }), appId]
          );
          break;
        }

        // ── STEP 4: CONTEXT (human step — handled above via action param) ──
        case 'context': {
          // If we get here without action, report status
          const docs = await query(
            `SELECT COUNT(*) as cnt FROM doc_sources WHERE app_id = $1`, [appId]
          );
          const d = parseInt(docs.rows[0].cnt);
          result = {
            success: true, documents: d,
            guidance: d > 0
              ? `${d} context document${d !== 1 ? 's' : ''} uploaded. Review in Context Library. Mark as complete or add more documents.`
              : 'No context documents uploaded yet. Upload data dictionaries, ERDs, or business rules in the Context Library to improve enrichment quality.',
          };
          break;
        }

        // ── STEP 5: ENRICH ──
        case 'enrich': {
          // Check if already running
          const enrichRunningCheck = await query(
            "SELECT id FROM pipeline_runs WHERE app_id = $1 AND status = 'running'", [appId]
          );
          if (enrichRunningCheck.rows.length > 0) {
            delete activeStepRuns[appId];
            return res.status(409).json({ error: 'Enrichment already running for this application' });
          }

          // Update workflow step status
          await query(
            `UPDATE pipeline_steps SET status = 'in_progress', started_at = NOW(), updated_at = NOW() WHERE app_id = $1 AND step_name = 'enrich'`, [appId]
          );

          // Create pipeline run with proper stage tracking (same as /run endpoint)
          const enrichStages = {
            ingest: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
            profile: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
            infer: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
            context: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
            enrich: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
            review: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
            validate: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
            publish: { status: 'pending', started_at: null, completed_at: null, duration_ms: null },
          };
          const enrichRunData = await query(
            `INSERT INTO pipeline_runs (app_id, status, triggered_by, stages, started_at)
             VALUES ($1, 'running', $2, $3, NOW()) RETURNING *`,
            [appId, req.user?.id || 1, JSON.stringify(enrichStages)]
          );
          const enrichRunId = enrichRunData.rows[0].id;

          // Initialize activeRuns for progress tracking (same structure as /run endpoint)
          activeRuns[enrichRunId] = {
            stages: enrichStages,
            tables_total: 0, tables_done: 0,
            columns_total: 0, columns_done: 0,
            current_table: null, current_stage: 'ingest',
            errors: [],
            enrich_start_time: null, enrich_table_times: [],
            token_usage: { input_tokens: 0, output_tokens: 0, total_tokens: 0 },
            workers: [], concurrency: 1,
            cancelled: false, pendingGate: null,
          };

          await query("UPDATE applications SET status = 'enriching', updated_at = NOW() WHERE id = $1", [appId]);

          // Auto-approve ALL pipeline gates for workflow-triggered enrichment.
          // The builder already decided to enrich by clicking "Run Step", so we don't
          // need the interactive approval gates that the Pipeline monitor uses.
          // The pipeline has gates for both 'enrich' and 'validate' (QPD) stages.
          const autoApproveTimer = setInterval(() => {
            const progress = activeRuns[enrichRunId];
            if (progress && progress.pendingGate) {
              console.log(`[Workflow] Auto-approving "${progress.pendingGate.stage}" gate for run ${enrichRunId}`);
              progress.pendingGate.resolve('approve');
              progress.pendingGate = null;
            }
          }, 500);
          // Safety: clear the timer after 10 minutes (enrichment + QPD can take a while)
          setTimeout(() => clearInterval(autoApproveTimer), 600000);

          // Launch background enrichment using the real pipeline function
          runEnrichmentPipeline(enrichRunId, appId, app).then(async () => {
            // On completion, update workflow step status
            try {
              const enrichedCount = await query(
                `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
                 WHERE at.app_id = $1 AND ac.enrichment_status IN ('ai_enriched', 'approved')`, [appId]
              );
              const totalColCount = await query(
                `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1`, [appId]
              );
              const ec = parseInt(enrichedCount.rows[0].cnt);
              const tcc = parseInt(totalColCount.rows[0].cnt);
              const score = tcc > 0 ? Math.round((ec / tcc) * 100) : 0;
              await query(
                `UPDATE pipeline_steps SET status = 'completed', quality_score = $1, quality_details = $2, completed_at = NOW(), updated_at = NOW() WHERE app_id = $3 AND step_name = 'enrich'`,
                [score, JSON.stringify({ enriched: ec, total: tcc }), appId]
              );
            } catch (stepErr) {
              console.error('Error updating enrich step status:', stepErr);
            }
          }).catch(async (err) => {
            console.error('Enrich pipeline error:', err);
            try {
              await query(
                `UPDATE pipeline_steps SET status = 'needs_attention', quality_details = $1, updated_at = NOW() WHERE app_id = $2 AND step_name = 'enrich'`,
                [JSON.stringify({ error: err.message }), appId]
              );
            } catch {}
          });

          // Return immediately — enrichment runs in background with progress polling
          result = {
            success: true, backgroundJob: true, runId: enrichRunId,
            guidance: 'AI Enrichment started in background. Progress will update automatically — watch for worker status, current table, and ETA below.',
          };
          break;
        }

        // ── STEP 6: SYNONYMS ──
        case 'synonyms': {
          const syns = await query(
            `SELECT COUNT(*) as cnt, COUNT(DISTINCT column_id) as cols_covered FROM app_synonyms WHERE app_id = $1 AND status = 'active'`, [appId]
          );
          const totalCols = await query(
            `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1`, [appId]
          );
          const s = parseInt(syns.rows[0].cnt);
          const covered = parseInt(syns.rows[0].cols_covered);
          const t = parseInt(totalCols.rows[0].cnt);
          const score = t > 0 ? Math.round((covered / t) * 100) : 0;
          result = {
            success: true, totalSynonyms: s, columnsCovered: covered, totalColumns: t,
            guidance: s > 0
              ? `${s} active synonyms covering ${covered}/${t} columns. Review in the Synonym Manager — add domain packs or manual terms to improve matching.`
              : 'No synonyms configured yet. Go to the Synonym Manager to add business terms, apply domain packs, or use AI Generate.',
            navigateTo: 'synonyms',
          };
          // Mark step as completed in pipeline_steps so chevron turns green
          await query(
            `UPDATE pipeline_steps SET status = $1, quality_score = $2, quality_details = $3, completed_at = NOW(), updated_at = NOW() WHERE app_id = $4 AND step_name = 'synonyms'`,
            [s > 0 ? 'completed' : 'not_started', score, JSON.stringify({ synonyms: s, covered, total: t }), appId]
          );
          break;
        }

        // ── STEP 7: CURATE (human step — complete/skip/reset handled above) ──
        case 'curate': {
          // If we get here without action, report status
          const totalCols = await query(
            `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1`, [appId]
          );
          const approved = await query(
            `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
             WHERE at.app_id = $1 AND ac.enrichment_status = 'approved'`, [appId]
          );
          const aiEnriched = await query(
            `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
             WHERE at.app_id = $1 AND ac.enrichment_status = 'ai_enriched'`, [appId]
          );
          const t = parseInt(totalCols.rows[0].cnt);
          const a = parseInt(approved.rows[0].cnt);
          const ai = parseInt(aiEnriched.rows[0].cnt);
          result = {
            success: true, totalColumns: t, approved: a, aiEnriched: ai, pendingReview: ai,
            guidance: ai > 0
              ? `${ai} columns have AI enrichments pending review. ${a} already approved. Review in Curation, Knowledge Graph, Patterns, and Quality dashboard. Click "Mark as Reviewed" when satisfied.`
              : a > 0
                ? `All ${a} enriched columns approved. Mark as Reviewed to proceed.`
                : 'No enrichments to review. Run Enrich step first.',
          };
          break;
        }

        // ── STEP 8: INDEX (vector embeddings) ──
        case 'index': {
          await query(
            `UPDATE pipeline_steps SET status = 'in_progress', started_at = NOW(), updated_at = NOW() WHERE app_id = $1 AND step_name = 'index'`, [appId]
          );

          // Actually build vector embeddings
          let embResult;
          try {
            const embeddingService = require('../services/embedding-service');
            embResult = await embeddingService.generateEmbeddingsForApp(appId, { force: true });
            console.log(`[Workflow] Built embeddings for app ${appId}: ${embResult.generated} generated, ${embResult.errors} errors`);
          } catch (embErr) {
            console.error(`[Workflow] Embedding build error for app ${appId}:`, embErr.message);
            embResult = { generated: 0, errors: 1, errorMessage: embErr.message };
          }

          // Update app config to mark embeddings as built
          const appCfg = typeof app.config === 'string' ? JSON.parse(app.config) : (app.config || {});
          if (embResult.generated > 0) {
            appCfg.embeddings_built = true;
            appCfg.vector_index_built = true;
            appCfg.embeddings_built_at = new Date().toISOString();
            await query(
              'UPDATE applications SET config = $1, updated_at = NOW() WHERE id = $2',
              [JSON.stringify(appCfg), appId]
            );
          }

          // Get synonym count to include in result
          let synonymCount = 0;
          try {
            const synRes = await query(`SELECT COUNT(*) as cnt FROM app_synonyms WHERE app_id = $1 AND status = 'active'`, [appId]);
            synonymCount = parseInt(synRes.rows[0].cnt) || 0;
          } catch (e) { /* ignore */ }

          const success = embResult.generated > 0;
          result = {
            success,
            embeddingsBuilt: success,
            tablesEmbedded: embResult.generated || 0,
            synonymsIncluded: synonymCount,
            errors: embResult.errors || 0,
            guidance: success
              ? `Vector embeddings built for ${embResult.generated} tables${synonymCount > 0 ? ` with ${synonymCount} synonyms included` : ''}. Semantic schema linking is now active — each embedding captures business names, descriptions, query patterns, and synonyms for meaning-based table matching.`
              : `Embedding build failed: ${embResult.errorMessage || 'unknown error'}. Check that OPENAI_API_KEY is set.`,
          };
          await query(
            `UPDATE pipeline_steps SET status = $1, quality_score = $2, quality_details = $3, ${success ? 'completed_at = NOW(),' : ''} updated_at = NOW() WHERE app_id = $4 AND step_name = 'index'`,
            [success ? 'completed' : 'needs_attention', success ? 100 : 0, JSON.stringify({ generated: embResult.generated, errors: embResult.errors }), appId]
          );
          break;
        }

        // ── STEP 9: VALIDATE ──
        case 'validate': {
          console.log(`[Workflow] Validate step starting for app ${appId}`);
          let t = 0, p = 0, f = 0, recentTests = [];
          try {
            const tests = await query(
              `SELECT COUNT(*) as total,
                      COUNT(CASE WHEN feedback = 'thumbs_up' THEN 1 END) as passed,
                      COUNT(CASE WHEN feedback = 'thumbs_down' THEN 1 END) as failed
               FROM test_queries WHERE app_id = $1`, [appId]
            );
            t = parseInt(tests.rows[0].total);
            p = parseInt(tests.rows[0].passed);
            f = parseInt(tests.rows[0].failed);
            const recentResult = await query(
              `SELECT question, feedback, created_at FROM test_queries WHERE app_id = $1 ORDER BY created_at DESC LIMIT 10`, [appId]
            );
            recentTests = recentResult.rows;
          } catch (qErr) {
            console.error(`[Workflow] Validate query error:`, qErr.message);
          }
          const score = t > 0 ? Math.round((p / t) * 100) : 0;
          result = {
            success: true, totalTests: t, passed: p, failed: f,
            recentTests,
            guidance: t > 0
              ? `${p}/${t} test queries passed (${score}%). ${f} failed. Review failing queries in Test Console and adjust enrichments or synonyms to improve accuracy.`
              : 'No test queries yet. Go to Test Console to ask sample questions and validate NL→SQL accuracy.',
            navigateTo: 'test',
          };
          try {
            await query(
              `UPDATE pipeline_steps SET status = $1, quality_score = $2, quality_details = $3, completed_at = NOW(), updated_at = NOW() WHERE app_id = $4 AND step_name = 'validate'`,
              [t > 0 ? 'completed' : 'not_started', score, JSON.stringify({ total: t, passed: p, failed: f }), appId]
            );
          } catch (uErr) {
            console.error(`[Workflow] Validate pipeline_steps update error:`, uErr.message);
          }
          console.log(`[Workflow] Validate step completed for app ${appId}: ${p}/${t}`);
          break;
        }

        // ── STEP 9: PUBLISH ──
        case 'publish': {
          await query("UPDATE applications SET status = 'published', published_at = NOW(), published_by = $1, updated_at = NOW() WHERE id = $2", [req.user?.id || 1, appId]);

          // Auto-create workspace + assign all users (so app appears in Data Ask)
          let workspaceCreated = false;
          try {
            const existingWs = await query('SELECT id FROM workspaces WHERE app_id = $1', [appId]);
            if (existingWs.rows.length === 0) {
              const ws = await query(
                `INSERT INTO workspaces (name, description, app_id, is_default)
                 VALUES ($1, $2, $3, FALSE) RETURNING id`,
                [app.name, `Workspace for ${app.name} (${app.type})`, appId]
              );
              const wsId = ws.rows[0].id;
              // Assign all users to this workspace
              const allUsers = await query('SELECT id FROM users');
              for (const u of allUsers.rows) {
                await query(
                  `INSERT INTO workspace_members (workspace_id, user_id, role, is_default, enabled)
                   VALUES ($1, $2, 'reader', TRUE, TRUE)
                   ON CONFLICT (workspace_id, user_id) DO NOTHING`,
                  [wsId, u.id]
                );
              }
              workspaceCreated = true;
              console.log(`[Publish] Created workspace "${app.name}" (id=${wsId}) with ${allUsers.rows.length} members`);
            }
          } catch (wsErr) {
            console.error('[Publish] Workspace creation failed (non-fatal):', wsErr.message);
          }

          result = {
            success: true, workspaceCreated,
            guidance: 'Application published and available for Data Ask queries. End users can now ask natural language questions against this knowledge graph.' + (workspaceCreated ? ' A workspace was automatically created for Data Ask access.' : ''),
          };
          await query(
            `UPDATE pipeline_steps SET status = 'completed', quality_score = 100, completed_at = NOW(), updated_at = NOW() WHERE app_id = $1 AND step_name = 'publish'`, [appId]
          );
          break;
        }

        default:
          result = { success: false, error: `Unknown step: ${step}` };
      }

    } finally {
      delete activeStepRuns[appId];
    }

    res.json({ step, kgStatusChanged, ...result });

  } catch (err) {
    delete activeStepRuns[appId];
    console.error(`Run step error (${req.body.step}):`, err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/pipeline/:appId/step-status — get quick status of all workflow steps
router.get('/:appId/step-status', async (req, res) => {
  try {
    const { appId } = req.params;

    // Gather live metrics for each step
    const tables = await query('SELECT COUNT(*) as cnt FROM app_tables WHERE app_id = $1', [appId]);
    const cols = await query(
      `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1`, [appId]
    );
    const profiledCols = await query(
      `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = $1 AND ac.data_type IS NOT NULL`, [appId]
    );
    const rels = await query('SELECT COUNT(*) as cnt FROM app_relationships WHERE app_id = $1', [appId]);
    const enriched = await query(
      `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = $1 AND ac.enrichment_status IN ('ai_enriched', 'approved')`, [appId]
    );
    const approved = await query(
      `SELECT COUNT(*) as cnt FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = $1 AND ac.enrichment_status = 'approved'`, [appId]
    );
    const syns = await query(
      `SELECT COUNT(*) as cnt FROM app_synonyms WHERE app_id = $1 AND status = 'active'`, [appId]
    );
    const tests = await query(
      `SELECT COUNT(*) as total, COUNT(CASE WHEN feedback = 'thumbs_up' THEN 1 END) as passed FROM test_queries WHERE app_id = $1`, [appId]
    );
    const appStatus = await query('SELECT status, config FROM applications WHERE id = $1', [appId]);
    const cfg = appStatus.rows[0]?.config || {};

    const t = parseInt(tables.rows[0].cnt);
    const c = parseInt(cols.rows[0].cnt);
    const pc = parseInt(profiledCols.rows[0].cnt);
    const r = parseInt(rels.rows[0].cnt);
    const e = parseInt(enriched.rows[0].cnt);
    const a = parseInt(approved.rows[0].cnt);
    const s = parseInt(syns.rows[0].cnt);
    const tt = parseInt(tests.rows[0].total);
    const tp = parseInt(tests.rows[0].passed);

    // Check pipeline_steps for human step statuses and reset tracking
    let pipelineSteps = await query(
      `SELECT step_name, status FROM pipeline_steps WHERE app_id = $1`, [appId]
    );

    // Auto-initialize pipeline_steps if none exist (required for reset tracking)
    if (pipelineSteps.rows.length === 0) {
      const STEP_NAMES = ['connect', 'profile', 'discover', 'context', 'enrich', 'synonyms', 'curate', 'index', 'validate', 'publish'];
      for (let i = 0; i < STEP_NAMES.length; i++) {
        await query(
          `INSERT INTO pipeline_steps (app_id, step_name, step_order, status)
           VALUES ($1, $2, $3, 'not_started')
           ON CONFLICT (app_id, step_name) DO NOTHING`,
          [appId, STEP_NAMES[i], i + 1]
        );
      }
      pipelineSteps = await query(
        `SELECT step_name, status FROM pipeline_steps WHERE app_id = $1`, [appId]
      );
    }

    const stepMap = {};
    for (const ps of pipelineSteps.rows) stepMap[ps.step_name] = ps.status;

    const contextDone = stepMap.context === 'completed' || stepMap.context === 'skipped';
    const curateDone = stepMap.curate === 'completed' || stepMap.curate === 'skipped';

    // Helper: a step is "done" only if data exists AND pipeline_steps is completed/skipped.
    // This ensures reset cascade (which sets pipeline_steps to not_started) correctly shows steps as undone.
    const isStepDone = (dataCheck, stepName) => {
      const ps = stepMap[stepName];
      if (ps === 'not_started') return false; // Reset overrides data presence
      return dataCheck;
    };

    // Count context docs
    let docs = 0;
    try {
      const docCount = await query('SELECT COUNT(*) as cnt FROM doc_sources WHERE app_id = $1', [appId]);
      docs = parseInt(docCount.rows[0].cnt);
    } catch {}

    // Count domains (from entity_metadata)
    let domainCount = 0;
    try {
      const domRes = await query(
        `SELECT COUNT(DISTINCT COALESCE(NULLIF(NULLIF(NULLIF(entity_metadata->>'domain', 'General'), 'UNKNOWN'), ''), 'Unclassified')) as cnt
         FROM app_tables WHERE app_id = $1 AND entity_metadata IS NOT NULL`, [appId]);
      domainCount = parseInt(domRes.rows[0].cnt) || 0;
    } catch {}

    res.json({
      connect:  { done: isStepDone(t > 0, 'connect'), summary: `${t} tables, ${c} columns` },
      profile:  { done: isStepDone(pc > 0, 'profile'), summary: `${pc}/${c} columns profiled` },
      discover: { done: isStepDone(r > 0, 'discover'), summary: `${r} relationships${domainCount > 0 ? `, ${domainCount} domains` : ''}` },
      context:  { done: contextDone, summary: contextDone ? (docs > 0 ? `${docs} documents` : 'Skipped') : (docs > 0 ? `${docs} documents uploaded` : 'No context yet') },
      enrich:   { done: isStepDone(e > 0, 'enrich'), summary: `${e}/${c} columns enriched` },
      synonyms: { done: isStepDone(s > 0, 'synonyms'), summary: `${s} active synonyms` },
      curate:   { done: curateDone, summary: curateDone ? `Reviewed (${a}/${c} approved)` : (a > 0 ? `${a}/${c} approved` : 'Not reviewed') },
      index:    { done: isStepDone(!!(cfg.embeddings_built || cfg.vector_index_built), 'index'), summary: cfg.embeddings_built ? 'Embeddings built' : 'Not built' },
      validate: { done: isStepDone(tt > 0, 'validate'), summary: tt > 0 ? `${tp}/${tt} passed` : 'No tests' },
      publish:  { done: appStatus.rows[0]?.status === 'published', summary: appStatus.rows[0]?.status || 'draft' },
    });
  } catch (err) {
    console.error('Step status error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/pipeline/:appId/profile-review — profiling data for Profile Review screen
router.get('/:appId/profile-review', async (req, res) => {
  try {
    const { appId } = req.params;

    // Table-level summary: row count, column count, has-data flag
    const tableSummary = await query(
      `SELECT at.id, at.table_name, at.entity_name, at.row_count,
              COUNT(ac.id) as column_count,
              COUNT(ac.id) FILTER (WHERE ac.data_type IS NOT NULL) as profiled_columns,
              COUNT(ac.id) FILTER (WHERE LOWER(ac.data_type) IN ('date', 'timestamp', 'datetime', 'timestamptz')) as date_columns,
              COUNT(ac.id) FILTER (WHERE ac.value_mapping IS NOT NULL AND ac.value_mapping != '{}') as low_card_columns
       FROM app_tables at
       LEFT JOIN app_columns ac ON at.id = ac.table_id
       WHERE at.app_id = $1
       GROUP BY at.id, at.table_name, at.entity_name, at.row_count
       ORDER BY COALESCE(at.row_count, 0) DESC`,
      [appId]
    );

    // Column-level detail: data type, keys, value_mapping snippet
    const columnDetail = await query(
      `SELECT ac.id, ac.column_name, ac.data_type, ac.is_pk, ac.is_fk,
              ac.value_mapping, ac.column_role, at.table_name, at.id as table_id
       FROM app_columns ac
       JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = $1
       ORDER BY at.table_name, ac.column_name`,
      [appId]
    );

    // Summary stats
    const totalTables = tableSummary.rows.length;
    const emptyTables = tableSummary.rows.filter(t => !t.row_count || parseInt(t.row_count) === 0).length;
    const totalCols = columnDetail.rows.length;
    const profiledCols = columnDetail.rows.filter(c => c.data_type).length;
    const dateCols = columnDetail.rows.filter(c => ['date', 'timestamp', 'datetime', 'timestamptz'].includes((c.data_type || '').toLowerCase()));
    const lowCardCols = columnDetail.rows.filter(c => c.value_mapping && c.value_mapping !== '{}');
    const pkCols = columnDetail.rows.filter(c => c.is_pk);
    const fkCols = columnDetail.rows.filter(c => c.is_fk);

    // Data type distribution
    const typeDistribution = {};
    for (const c of columnDetail.rows) {
      const dt = c.data_type || 'unknown';
      typeDistribution[dt] = (typeDistribution[dt] || 0) + 1;
    }

    // Table size distribution (by row count ranges)
    const sizeDistribution = { 'empty (0)': 0, '1–100': 0, '101–1K': 0, '1K–10K': 0, '10K–100K': 0, '100K+': 0 };
    for (const t of tableSummary.rows) {
      const rc = parseInt(t.row_count) || 0;
      if (rc === 0) sizeDistribution['empty (0)']++;
      else if (rc <= 100) sizeDistribution['1–100']++;
      else if (rc <= 1000) sizeDistribution['101–1K']++;
      else if (rc <= 10000) sizeDistribution['1K–10K']++;
      else if (rc <= 100000) sizeDistribution['10K–100K']++;
      else sizeDistribution['100K+']++;
    }

    res.json({
      summary: {
        totalTables, emptyTables,
        totalColumns: totalCols, profiledColumns: profiledCols,
        dateColumns: dateCols.length, lowCardColumns: lowCardCols.length,
        pkColumns: pkCols.length, fkColumns: fkCols.length,
        typeDistribution, sizeDistribution,
      },
      tables: tableSummary.rows.map(t => ({
        id: t.id, tableName: t.table_name, entityName: t.entity_name,
        rowCount: parseInt(t.row_count) || 0, columnCount: parseInt(t.column_count),
        profiledColumns: parseInt(t.profiled_columns), dateColumns: parseInt(t.date_columns),
        lowCardColumns: parseInt(t.low_card_columns),
        classification: 'unclassified',
        isEmpty: !t.row_count || parseInt(t.row_count) === 0,
      })),
      columns: columnDetail.rows.map(c => ({
        id: c.id, columnName: c.column_name, tableName: c.table_name, tableId: c.table_id,
        dataType: c.data_type || 'unknown', isPk: c.is_pk, isFk: c.is_fk, role: c.column_role || null,
        hasValueMap: !!(c.value_mapping && c.value_mapping !== '{}'),
        valueMapPreview: c.value_mapping && c.value_mapping !== '{}'
          ? (() => { try { const vm = typeof c.value_mapping === 'string' ? JSON.parse(c.value_mapping) : c.value_mapping; const keys = Object.keys(vm); return keys.slice(0, 5).join(', ') + (keys.length > 5 ? ` (+${keys.length - 5} more)` : ''); } catch { return ''; } })()
          : null,
      })),
    });
  } catch (err) {
    console.error('Profile review error:', err);
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/pipeline/:appId/data-source — update the SQLite data source path
//   Body: { sqlite_path: '/path/to/file.sqlite' }
//   Used to fix broken/expired paths for BIRD benchmark databases
router.put('/:appId/data-source', async (req, res) => {
  try {
    const { appId } = req.params;
    const { sqlite_path } = req.body;

    if (!sqlite_path) {
      return res.status(400).json({ error: 'sqlite_path is required' });
    }

    const appResult = await query('SELECT id, name, config FROM applications WHERE id = $1', [appId]);
    if (appResult.rows.length === 0) return res.status(404).json({ error: 'Application not found' });

    const app = appResult.rows[0];
    const config = typeof app.config === 'string' ? JSON.parse(app.config) : (app.config || {});

    // Check if the file exists
    const fileExists = fs.existsSync(sqlite_path);

    // Update config
    config.sqlite_path = sqlite_path;
    await query(
      'UPDATE applications SET config = $1, updated_at = NOW() WHERE id = $2',
      [JSON.stringify(config), appId]
    );

    res.json({
      success: true,
      appId: parseInt(appId),
      appName: app.name,
      sqlite_path,
      fileExists,
      message: fileExists
        ? `Data source path updated and file verified for "${app.name}".`
        : `Data source path updated for "${app.name}", but file not found at "${sqlite_path}". The path will be tried at next Connect step — if it points to a BIRD benchmark DB, the auto-resolver will also check data/ and BIRD Benchmark/ folders.`,
    });
  } catch (err) {
    console.error('Update data source error:', err);
    res.status(500).json({ error: err.message });
  }
});

// PUT /api/pipeline/:appId/clear-data-source — clear stale sqlite_path so auto-resolver can find the file
router.put('/:appId/clear-data-source', async (req, res) => {
  try {
    const { appId } = req.params;

    const appResult = await query('SELECT id, name, config FROM applications WHERE id = $1', [appId]);
    if (appResult.rows.length === 0) return res.status(404).json({ error: 'Application not found' });

    const app = appResult.rows[0];
    const config = typeof app.config === 'string' ? JSON.parse(app.config) : (app.config || {});
    const oldPath = config.sqlite_path;

    delete config.sqlite_path;
    await query(
      'UPDATE applications SET config = $1, updated_at = NOW() WHERE id = $2',
      [JSON.stringify(config), appId]
    );

    // Now check if findSqlitePath can resolve it automatically
    const resolved = findSqlitePath({ ...app, config });
    res.json({
      success: true,
      appId: parseInt(appId),
      appName: app.name,
      clearedPath: oldPath || null,
      autoResolved: resolved || null,
      message: resolved
        ? `Cleared stale path. Auto-resolver found SQLite at: ${resolved}`
        : `Cleared stale path. Auto-resolver could not find a SQLite file — the database may need to be added to the data/ folder.`,
    });
  } catch (err) {
    console.error('Clear data source error:', err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/pipeline/:appId/data-source — check current data source status
router.get('/:appId/data-source', async (req, res) => {
  try {
    const { appId } = req.params;

    const appResult = await query('SELECT id, name, type, config FROM applications WHERE id = $1', [appId]);
    if (appResult.rows.length === 0) return res.status(404).json({ error: 'Application not found' });

    const app = appResult.rows[0];
    const config = typeof app.config === 'string' ? JSON.parse(app.config) : (app.config || {});
    const configPath = config.sqlite_path || null;
    const configPathExists = configPath ? fs.existsSync(configPath) : false;
    const resolvedPath = findSqlitePath(app);
    const sourceAvailable = await hasSourceData(appId);
    const tableCount = await query('SELECT COUNT(*) as cnt FROM app_tables WHERE app_id = $1', [appId]);

    res.json({
      appId: parseInt(appId),
      appName: app.name,
      appType: app.type,
      configPath,
      configPathExists,
      resolvedPath,
      sourceDataLoaded: sourceAvailable,
      tablesInAKG: parseInt(tableCount.rows[0].cnt),
      status: resolvedPath ? 'ok' : (sourceAvailable ? 'loaded_no_file' : (parseInt(tableCount.rows[0].cnt) > 0 ? 'metadata_only' : 'not_configured')),
    });
  } catch (err) {
    console.error('Data source status error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ─── Diagnostic: Strategy 3b trace for debugging relationship discovery ───
router.get('/:appId/diagnose-relationships', async (req, res) => {
  const appId = parseInt(req.params.appId);
  const targetCols = (req.query.columns || 'bukrs,lifnr,kunnr,vbeln,ebeln,werks,matnr').toLowerCase().split(',');

  try {
    const { hasSourceData, executeOnSourceData } = require('../services/data-loader');

    // Get all tables
    const tablesResult = await query('SELECT id, table_name, row_count FROM app_tables WHERE app_id = $1', [appId]);
    const tables = tablesResult.rows;
    const totalTableCount = tables.length;
    const ubiquityThreshold = Math.max(5, Math.floor(totalTableCount * 0.5));

    // Get existing relationships
    const existRels = await query(
      'SELECT from_table_id, from_column, to_table_id, to_column FROM app_relationships WHERE app_id = $1',
      [appId]
    );

    // Build allColsByName
    const allColsByName = {};
    for (const table of tables) {
      const cols = await query(
        'SELECT id, column_name, is_pk, is_fk FROM app_columns WHERE table_id = $1',
        [table.id]
      );
      for (const col of cols.rows) {
        const key = col.column_name.toLowerCase();
        if (!allColsByName[key]) allColsByName[key] = [];
        allColsByName[key].push({ table, col, isPK: col.is_pk, isFK: col.is_fk });
      }
    }

    // Skip columns set
    const skipColumns = new Set([
      'mandt', 'id', 'name', 'description', 'status', 'type',
      'created_at', 'updated_at', 'created_by', 'modified_by',
      'last_update_date', 'creation_date', 'last_updated_by',
      'record_id', 'row_id', 'seq', 'sequence', 'sort_order',
      'is_active', 'is_deleted', 'flag', 'comments', 'notes', 'remark', 'remarks',
      'erdat', 'ernam', 'aedat', 'aenam', 'loekz', 'spras',
      'budat', 'bldat', 'cpudt', 'cputm', 'usnam', 'tcode',
    ]);

    const sourceAvail = await hasSourceData(appId);
    const results = {};

    for (const colName of targetCols) {
      const entries = allColsByName[colName];
      const diag = {
        column: colName,
        found: !!entries,
        tableCount: entries ? entries.length : 0,
        ubiquityThreshold,
        skipColumnsHit: skipColumns.has(colName),
        tooShort: colName.length < 3,
        hasPK: entries ? entries.some(e => e.isPK) : false,
        filterReason: null,
        tables: [],
        anchor: null,
        relationships: [],
      };

      if (!entries) { diag.filterReason = 'column not found in any table'; results[colName] = diag; continue; }
      if (entries.some(e => e.isPK)) { diag.filterReason = 'skipped: has PK entry (Strategy 3 handles)'; results[colName] = diag; continue; }
      if (entries.length < 2) { diag.filterReason = 'only in 1 table'; results[colName] = diag; continue; }
      if (entries.length > ubiquityThreshold) { diag.filterReason = `ubiquitous: ${entries.length} > ${ubiquityThreshold}`; results[colName] = diag; continue; }
      if (skipColumns.has(colName)) { diag.filterReason = 'in skipColumns set'; results[colName] = diag; continue; }
      if (colName.length < 3) { diag.filterReason = 'too short (<3 chars)'; results[colName] = diag; continue; }

      // Check row counts
      const rowCounts = {};
      for (const t of tables) { rowCounts[t.id] = parseInt(t.row_count || 0); }

      const entriesWithData = entries.filter(e => (rowCounts[e.table.id] || 0) > 0);
      diag.tables = entries.map(e => ({
        table: e.table.table_name,
        tableId: e.table.id,
        rowCount: rowCounts[e.table.id] || 0,
        hasData: (rowCounts[e.table.id] || 0) > 0,
      }));

      if (entriesWithData.length < 2) {
        diag.filterReason = `only ${entriesWithData.length} tables have data (need 2+)`;
        results[colName] = diag;
        continue;
      }

      // Anchor detection
      if (sourceAvail) {
        let bestRatio = 0;
        let anchorEntry = null;
        const anchorCandidates = [];

        for (const entry of entriesWithData) {
          try {
            const distinctResult = await executeOnSourceData(appId,
              `SELECT COUNT(DISTINCT "${entry.col.column_name}") as dcnt, COUNT(*) as tcnt FROM "${entry.table.table_name}" WHERE "${entry.col.column_name}" IS NOT NULL`,
              { timeout: 5000 }
            );
            const dcnt = parseInt(distinctResult.rows[0]?.dcnt || 0);
            const tcnt = parseInt(distinctResult.rows[0]?.tcnt || 0);
            const ratio = tcnt > 0 ? dcnt / tcnt : 0;

            anchorCandidates.push({
              table: entry.table.table_name,
              tableId: entry.table.id,
              distinctCount: dcnt,
              totalCount: tcnt,
              uniquenessRatio: ratio,
            });

            if (tcnt > 0 && dcnt > 0) {
              // Prefer highest uniqueness ratio; break ties by MORE distinct values
              // (larger reference table = more complete master data)
              if (ratio > bestRatio || (ratio === bestRatio && anchorEntry && dcnt > anchorEntry.distinctCnt)) {
                bestRatio = ratio;
                anchorEntry = { ...entry, distinctCnt: dcnt, totalCnt: tcnt };
              }
            }
          } catch (e) {
            anchorCandidates.push({ table: entry.table.table_name, error: e.message });
          }
        }

        diag.anchorCandidates = anchorCandidates;

        if (!anchorEntry || bestRatio < 0.5) {
          diag.filterReason = `no suitable anchor: bestRatio=${bestRatio.toFixed(3)} (need >=0.5)`;
          diag.anchor = anchorEntry ? { table: anchorEntry.table.table_name, ratio: bestRatio } : null;
          results[colName] = diag;
          continue;
        }

        diag.anchor = {
          table: anchorEntry.table.table_name,
          tableId: anchorEntry.table.id,
          distinctCount: anchorEntry.distinctCnt,
          totalCount: anchorEntry.totalCnt,
          uniquenessRatio: bestRatio,
        };

        // Value overlap checks for top 5 child tables
        let checked = 0;
        for (const entry of entriesWithData) {
          if (entry.table.id === anchorEntry.table.id) continue;
          if (checked >= 5) break;
          checked++;

          try {
            const childSample = await executeOnSourceData(appId,
              `SELECT DISTINCT "${entry.col.column_name}" as val FROM "${entry.table.table_name}" WHERE "${entry.col.column_name}" IS NOT NULL LIMIT 50`,
              { timeout: 5000 }
            );

            if (childSample.rows.length === 0) {
              diag.relationships.push({ child: entry.table.table_name, status: 'no sample values' });
              continue;
            }

            const sampleValues = childSample.rows.map(r => r.val);
            const escaped = sampleValues.map(v => v === null ? 'NULL' : `'${String(v).replace(/'/g, "''")}'`).join(', ');

            const anchorMatch = await executeOnSourceData(appId,
              `SELECT COUNT(DISTINCT "${anchorEntry.col.column_name}") as cnt FROM "${anchorEntry.table.table_name}" WHERE "${anchorEntry.col.column_name}" IN (${escaped})`,
              { timeout: 5000 }
            );

            const matchCount = parseInt(anchorMatch.rows[0]?.cnt || 0);
            const overlapRatio = matchCount / sampleValues.length;
            const minMatches = Math.min(2, sampleValues.length);
            const passes = overlapRatio >= 0.5 && matchCount >= minMatches;

            diag.relationships.push({
              child: entry.table.table_name,
              sampleCount: sampleValues.length,
              sampleValues: sampleValues.slice(0, 5).map(String),
              matchCount,
              overlapRatio: overlapRatio.toFixed(3),
              minMatches,
              passes,
            });
          } catch (e) {
            diag.relationships.push({ child: entry.table.table_name, error: e.message });
          }
        }
      } else {
        diag.filterReason = 'no source data available';
      }

      results[colName] = diag;
    }

    // Also get current relationship summary
    const relSummary = await query(
      `SELECT r.from_table_id, t1.table_name as src_table, r.from_column, r.to_table_id, t2.table_name as tgt_table, r.to_column, r.confidence_score
       FROM app_relationships r
       JOIN app_tables t1 ON r.from_table_id = t1.id
       JOIN app_tables t2 ON r.to_table_id = t2.id
       WHERE r.app_id = $1
       ORDER BY t2.table_name, r.from_column`,
      [appId]
    );

    res.json({
      appId,
      totalTables: totalTableCount,
      ubiquityThreshold,
      sourceDataAvailable: sourceAvail,
      existingRelationships: relSummary.rows.length,
      relationshipList: relSummary.rows,
      columnDiagnostics: results,
    });
  } catch (err) {
    res.status(500).json({ error: err.message, stack: err.stack });
  }
});

module.exports = router;
