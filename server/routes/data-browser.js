const express = require('express');
const { query } = require('../db');
const { browseTable, hasSourceData, executeOnSourceData } = require('../services/data-loader');

const router = express.Router();

// GET /api/data/:appId/tables - list tables with row counts
router.get('/:appId/tables', async (req, res) => {
  try {
    const { appId } = req.params;

    const dataAvailable = await hasSourceData(appId);
    if (!dataAvailable) {
      return res.json({ available: false, tables: [], message: 'Run the pipeline first to load source data.' });
    }

    const [rows] = await query(
      `SELECT id, table_name, entity_name, row_count, description
       FROM app_tables WHERE app_id = ? ORDER BY table_name`,
      [appId]
    );

    // If any tables have row_count = 0 or null, query actual source data for counts
    const schemaName = `appdata_${appId}`;
    const tables = rows;
    for (const table of tables) {
      if (!table.row_count) {
        try {
          const [countRes] = await query(`SELECT COUNT(*) as cnt FROM \`${schemaName}\`.\`${table.table_name}\``);
          table.row_count = parseInt(countRes[0].cnt);
          // Also update the stored value for next time
          await query('UPDATE app_tables SET row_count = ? WHERE id = ?', [table.row_count, table.id]);
        } catch (e) { /* ignore count errors */ }
      }
    }

    res.json({ available: true, tables });
  } catch (err) {
    console.error('Data browser tables error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/data/:appId/browse/:tableName - browse actual data with pagination
router.get('/:appId/browse/:tableName', async (req, res) => {
  try {
    const { appId, tableName } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 50, 200);
    const offset = parseInt(req.query.offset) || 0;

    const dataAvailable = await hasSourceData(appId);
    if (!dataAvailable) {
      return res.status(404).json({ error: 'Source data not loaded. Run the pipeline first.' });
    }

    const result = await browseTable(appId, tableName, { limit, offset });

    // Also fetch column enrichment metadata
    const [colMeta] = await query(
      `SELECT ac.column_name, ac.business_name, ac.description, ac.enrichment_status, ac.confidence_score
       FROM app_columns ac
       JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = ? AND at.table_name = ?
       ORDER BY ac.column_name`,
      [appId, tableName]
    );

    res.json({
      ...result,
      column_metadata: colMeta,
    });
  } catch (err) {
    console.error('Browse table error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/data/:appId/query - execute a read-only SQL query
router.post('/:appId/query', async (req, res) => {
  try {
    const { appId } = req.params;
    const { sql } = req.body;

    if (!sql) return res.status(400).json({ error: 'SQL query required' });

    // Safety: only allow SELECT
    const trimmed = sql.trim().toUpperCase();
    if (!trimmed.startsWith('SELECT') && !trimmed.startsWith('WITH')) {
      return res.status(400).json({ error: 'Only SELECT queries are allowed' });
    }

    const dataAvailable = await hasSourceData(appId);
    if (!dataAvailable) {
      return res.status(404).json({ error: 'Source data not loaded. Run the pipeline first.' });
    }

    const result = await executeOnSourceData(appId, sql);
    res.json(result);
  } catch (err) {
    console.error('Query execution error:', err);
    res.status(500).json({ error: err.message || 'Query execution failed' });
  }
});

module.exports = router;
