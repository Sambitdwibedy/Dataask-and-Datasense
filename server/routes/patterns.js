const express = require('express');
const { query } = require('../db');

const router = express.Router();

// GET /api/patterns/:appId/patterns - list discovered query patterns
router.get('/:appId/patterns', async (req, res) => {
  try {
    const { appId } = req.params;
    const status = req.query.status || null;

    let sql = `
      SELECT
        id,
        pattern_name,
        nl_template,
        sql_template,
        tables_used,
        status,
        usage_count,
        created_at
      FROM query_patterns
      WHERE app_id = $1
    `;

    const params = [appId];

    if (status) {
      sql += ` AND status = $2`;
      params.push(status);
    }

    sql += ` ORDER BY usage_count DESC, created_at DESC`;

    const result = await query(sql, params);

    res.json({
      patterns: result.rows,
    });
  } catch (err) {
    console.error('Get patterns error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/patterns/:appId/patterns/:patternId - pattern detail
router.get('/:appId/patterns/:patternId', async (req, res) => {
  try {
    const { appId, patternId } = req.params;

    const result = await query(
      `SELECT * FROM query_patterns WHERE id = $1 AND app_id = $2`,
      [patternId, appId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Pattern not found' });
    }

    res.json({
      pattern: result.rows[0],
    });
  } catch (err) {
    console.error('Get pattern detail error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/patterns/:appId/patterns/:patternId/approve - approve pattern
router.post('/:appId/patterns/:patternId/approve', async (req, res) => {
  try {
    const { appId, patternId } = req.params;

    const result = await query(
      `UPDATE query_patterns
       SET status = 'approved'
       WHERE id = $1 AND app_id = $2
       RETURNING *`,
      [patternId, appId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Pattern not found' });
    }

    res.json({
      pattern: result.rows[0],
      message: 'Pattern approved',
    });
  } catch (err) {
    console.error('Approve pattern error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/patterns/:appId/stats - pattern statistics
router.get('/:appId/stats', async (req, res) => {
  try {
    const { appId } = req.params;

    const result = await query(
      `SELECT
        COUNT(DISTINCT id) as total_patterns,
        COUNT(DISTINCT CASE WHEN status = 'approved' THEN id END) as approved_patterns,
        COUNT(DISTINCT CASE WHEN status = 'draft' THEN id END) as draft_patterns,
        SUM(usage_count) as total_usages
      FROM query_patterns
      WHERE app_id = $1`,
      [appId]
    );

    res.json({
      stats: result.rows[0],
    });
  } catch (err) {
    console.error('Get pattern stats error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
