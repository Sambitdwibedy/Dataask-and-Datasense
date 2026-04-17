const express = require('express');
const { query } = require('../db');

const router = express.Router();

// GET /api/consumption/summary - overall token usage summary
router.get('/summary', async (req, res) => {
  try {
    const result = await query(
      `SELECT
        COALESCE(SUM(input_tokens), 0) as total_input_tokens,
        COALESCE(SUM(output_tokens), 0) as total_output_tokens,
        COALESCE(SUM(total_tokens), 0) as total_tokens,
        COALESCE(SUM(cost_estimate), 0) as total_cost,
        COUNT(*) as total_calls
      FROM token_usage`
    );
    res.json(result.rows[0]);
  } catch (err) {
    console.error('Consumption summary error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/consumption/by-app - token usage grouped by application
router.get('/by-app', async (req, res) => {
  try {
    const result = await query(
      `SELECT
        a.id as app_id,
        a.name as app_name,
        a.type as app_type,
        COALESCE(SUM(tu.input_tokens), 0) as input_tokens,
        COALESCE(SUM(tu.output_tokens), 0) as output_tokens,
        COALESCE(SUM(tu.total_tokens), 0) as total_tokens,
        COALESCE(SUM(tu.cost_estimate), 0) as cost_estimate,
        COUNT(tu.id) as api_calls
      FROM applications a
      LEFT JOIN token_usage tu ON a.id = tu.app_id
      GROUP BY a.id, a.name, a.type
      ORDER BY total_tokens DESC`
    );
    res.json({ apps: result.rows });
  } catch (err) {
    console.error('Consumption by-app error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/consumption/by-stage - token usage grouped by pipeline stage
router.get('/by-stage', async (req, res) => {
  try {
    const result = await query(
      `SELECT
        stage,
        COALESCE(SUM(input_tokens), 0) as input_tokens,
        COALESCE(SUM(output_tokens), 0) as output_tokens,
        COALESCE(SUM(total_tokens), 0) as total_tokens,
        COALESCE(SUM(cost_estimate), 0) as cost_estimate,
        COUNT(*) as api_calls,
        COALESCE(AVG(total_tokens), 0) as avg_tokens_per_call
      FROM token_usage
      GROUP BY stage
      ORDER BY total_tokens DESC`
    );
    res.json({ stages: result.rows });
  } catch (err) {
    console.error('Consumption by-stage error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/consumption/by-table/:appId - token usage per table for a specific app
router.get('/by-table/:appId', async (req, res) => {
  try {
    const { appId } = req.params;
    const result = await query(
      `SELECT
        table_name,
        stage,
        input_tokens,
        output_tokens,
        total_tokens,
        cost_estimate,
        model,
        created_at
      FROM token_usage
      WHERE app_id = $1
      ORDER BY created_at DESC`,
      [appId]
    );
    res.json({ usage: result.rows });
  } catch (err) {
    console.error('Consumption by-table error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/consumption/by-run/:runId - token usage for a specific pipeline run
router.get('/by-run/:runId', async (req, res) => {
  try {
    const { runId } = req.params;
    const result = await query(
      `SELECT
        table_name,
        stage,
        input_tokens,
        output_tokens,
        total_tokens,
        cost_estimate,
        model,
        created_at
      FROM token_usage
      WHERE pipeline_run_id = $1
      ORDER BY created_at`,
      [runId]
    );
    res.json({ usage: result.rows });
  } catch (err) {
    console.error('Consumption by-run error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/consumption/runs/:appId - pipeline run history with per-run token totals
router.get('/runs/:appId', async (req, res) => {
  try {
    const { appId } = req.params;
    const result = await query(
      `SELECT
        pr.id as run_id,
        pr.status,
        pr.started_at,
        pr.completed_at,
        COALESCE(SUM(tu.input_tokens), 0) as input_tokens,
        COALESCE(SUM(tu.output_tokens), 0) as output_tokens,
        COALESCE(SUM(tu.total_tokens), 0) as total_tokens,
        COALESCE(SUM(tu.cost_estimate), 0) as cost_estimate,
        COUNT(tu.id) as api_calls
      FROM pipeline_runs pr
      LEFT JOIN token_usage tu ON pr.id = tu.pipeline_run_id
      WHERE pr.app_id = $1
      GROUP BY pr.id, pr.status, pr.started_at, pr.completed_at
      ORDER BY pr.started_at DESC
      LIMIT 20`,
      [appId]
    );
    res.json({ runs: result.rows });
  } catch (err) {
    console.error('Consumption runs error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
