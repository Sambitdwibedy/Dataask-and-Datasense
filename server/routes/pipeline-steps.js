const express = require('express');
const { query } = require('../db');

const router = express.Router();

// The 10-step guided workflow (V2 — with Context step)
const STEP_DEFINITIONS = [
  { name: 'connect',   order: 1,  label: 'Connect & Load', description: 'Configure data source and load schema metadata' },
  { name: 'profile',   order: 2,  label: 'Profile & Classify', description: 'Statistical profiling, data type detection, table classification' },
  { name: 'discover',  order: 3,  label: 'Discover',  description: 'Detect relationships and foreign keys' },
  { name: 'context',   order: 4,  label: 'Add Context', description: 'Provide business rules, domain docs, and field mappings' },
  { name: 'enrich',    order: 5,  label: 'Enrich',    description: 'AI enrichment of business names and descriptions' },
  { name: 'synonyms',  order: 6,  label: 'Synonyms',  description: 'Apply and curate column synonyms' },
  { name: 'curate',    order: 7,  label: 'Curate & Review', description: 'Human review and quality gate' },
  { name: 'index',     order: 8,  label: 'Build Index', description: 'Build vector embeddings and semantic index' },
  { name: 'validate',  order: 9,  label: 'Validate',  description: 'Test NL→SQL accuracy with sample queries' },
  { name: 'publish',   order: 10, label: 'Publish',   description: 'Make knowledge graph available for Data Ask' },
];

// ═══════════════════════════════════════════════════════════════════
//  Pipeline Steps — Guided Workflow Tracking
// ═══════════════════════════════════════════════════════════════════

// GET /api/pipeline-steps/:appId — get all steps for an application
//   Initializes steps if they don't exist yet
router.get('/:appId', async (req, res) => {
  try {
    const { appId } = req.params;

    // Check if steps exist
    let result = await query(
      `SELECT * FROM pipeline_steps WHERE app_id = $1 ORDER BY step_order`,
      [appId]
    );

    // Auto-initialize if no steps exist
    if (result.rows.length === 0) {
      for (const step of STEP_DEFINITIONS) {
        await query(
          `INSERT INTO pipeline_steps (app_id, step_name, step_order, status)
           VALUES ($1, $2, $3, 'not_started')
           ON CONFLICT (app_id, step_name) DO NOTHING`,
          [appId, step.name, step.order]
        );
      }
      result = await query(
        `SELECT * FROM pipeline_steps WHERE app_id = $1 ORDER BY step_order`,
        [appId]
      );
    }

    // Merge step definitions with stored state
    const steps = result.rows.map(row => {
      const def = STEP_DEFINITIONS.find(d => d.name === row.step_name) || {};
      return {
        ...row,
        label: def.label || row.step_name,
        description: def.description || '',
      };
    });

    // Compute overall progress
    const completed = steps.filter(s => s.status === 'completed').length;
    const total = steps.length;
    const currentStep = steps.find(s => s.status === 'in_progress') ||
                        steps.find(s => s.status === 'not_started') ||
                        steps[steps.length - 1];

    res.json({
      steps,
      progress: {
        completed,
        total,
        percentage: total > 0 ? Math.round((completed / total) * 100) : 0,
        currentStep: currentStep?.step_name || null,
      },
    });
  } catch (err) {
    console.error('Get pipeline steps error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// PUT /api/pipeline-steps/:appId/:stepName — update a step's status
//   Body: { status, qualityScore?, qualityDetails?, notes? }
router.put('/:appId/:stepName', async (req, res) => {
  try {
    const { appId, stepName } = req.params;
    const { status, qualityScore, qualityDetails, notes } = req.body;

    if (!status) {
      return res.status(400).json({ error: 'status is required' });
    }

    const validStatuses = ['not_started', 'in_progress', 'completed', 'needs_attention', 'skipped'];
    if (!validStatuses.includes(status)) {
      return res.status(400).json({ error: `Invalid status. Must be one of: ${validStatuses.join(', ')}` });
    }

    // Check dependency: cannot start a step if previous required step is not completed
    const stepDef = STEP_DEFINITIONS.find(d => d.name === stepName);
    if (stepDef && status === 'in_progress' && stepDef.order > 1) {
      const prevSteps = await query(
        `SELECT step_name, status FROM pipeline_steps
         WHERE app_id = $1 AND step_order < $2 AND status NOT IN ('completed', 'skipped')
         ORDER BY step_order`,
        [appId, stepDef.order]
      );

      // Warn but don't block — some steps can be run out of order by advanced users
      if (prevSteps.rows.length > 0) {
        const incomplete = prevSteps.rows.map(r => r.step_name).join(', ');
        // We'll include a warning in the response but allow proceeding
        req._stepWarning = `Previous steps not completed: ${incomplete}`;
      }
    }

    const sets = ['status = $3', 'updated_at = NOW()'];
    const params = [appId, stepName, status];
    let idx = 4;

    if (status === 'in_progress') {
      sets.push(`started_at = COALESCE(started_at, NOW())`);
    }
    if (status === 'completed') {
      sets.push(`completed_at = NOW()`);
      sets.push(`completed_by = $${idx++}`);
      params.push(req.user?.id || null);
    }
    if (qualityScore !== undefined) {
      sets.push(`quality_score = $${idx++}`);
      params.push(qualityScore);
    }
    if (qualityDetails !== undefined) {
      sets.push(`quality_details = $${idx++}`);
      params.push(JSON.stringify(qualityDetails));
    }
    if (notes !== undefined) {
      sets.push(`notes = $${idx++}`);
      params.push(notes);
    }

    const result = await query(
      `UPDATE pipeline_steps SET ${sets.join(', ')} WHERE app_id = $1 AND step_name = $2 RETURNING *`,
      params
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Step not found' });
    }

    const response = { step: result.rows[0] };
    if (req._stepWarning) {
      response.warning = req._stepWarning;
    }

    res.json(response);
  } catch (err) {
    console.error('Update pipeline step error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/pipeline-steps/:appId/:stepName/verify — run verification for a step
//   Returns quality metrics based on the step type
router.post('/:appId/:stepName/verify', async (req, res) => {
  try {
    const { appId, stepName } = req.params;

    let qualityScore = 0;
    let qualityDetails = {};

    switch (stepName) {
      case 'load': {
        // Verify: tables and columns loaded
        const tables = await query(`SELECT COUNT(*) as count FROM app_tables WHERE app_id = $1`, [appId]);
        const columns = await query(
          `SELECT COUNT(*) as count FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1`,
          [appId]
        );
        qualityDetails = {
          tables_loaded: parseInt(tables.rows[0].count),
          columns_loaded: parseInt(columns.rows[0].count),
        };
        qualityScore = qualityDetails.tables_loaded > 0 ? 100 : 0;
        break;
      }

      case 'profile': {
        // Verify: columns have profiling data (data_type, value_mapping)
        const total = await query(
          `SELECT COUNT(*) as count FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1`,
          [appId]
        );
        const profiled = await query(
          `SELECT COUNT(*) as count FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
           WHERE at.app_id = $1 AND (ac.data_type IS NOT NULL OR ac.value_mapping IS NOT NULL)`,
          [appId]
        );
        const t = parseInt(total.rows[0].count);
        const p = parseInt(profiled.rows[0].count);
        qualityScore = t > 0 ? Math.round((p / t) * 100) : 0;
        qualityDetails = { total_columns: t, profiled_columns: p };
        break;
      }

      case 'discover': {
        // Verify: relationships detected
        const rels = await query(`SELECT COUNT(*) as count FROM app_relationships WHERE app_id = $1`, [appId]);
        const tables = await query(`SELECT COUNT(*) as count FROM app_tables WHERE app_id = $1`, [appId]);
        const r = parseInt(rels.rows[0].count);
        const t = parseInt(tables.rows[0].count);
        // Heuristic: expect at least (tables - 1) relationships for a connected graph
        qualityScore = t > 1 ? Math.min(100, Math.round((r / (t - 1)) * 100)) : (r > 0 ? 100 : 0);
        qualityDetails = { relationships: r, tables: t };
        break;
      }

      case 'enrich': {
        // Verify: columns have enrichment (business_name, description)
        const total = await query(
          `SELECT COUNT(*) as count FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1`,
          [appId]
        );
        const enriched = await query(
          `SELECT COUNT(*) as count FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
           WHERE at.app_id = $1 AND ac.enrichment_status IN ('ai_enriched', 'approved')`,
          [appId]
        );
        const t = parseInt(total.rows[0].count);
        const e = parseInt(enriched.rows[0].count);
        qualityScore = t > 0 ? Math.round((e / t) * 100) : 0;
        qualityDetails = { total_columns: t, enriched_columns: e };
        break;
      }

      case 'synonyms': {
        // Verify: synonyms applied
        const syns = await query(
          `SELECT COUNT(*) as count, COUNT(DISTINCT column_id) as columns_covered
           FROM app_synonyms WHERE app_id = $1 AND status = 'active'`,
          [appId]
        );
        const totalCols = await query(
          `SELECT COUNT(*) as count FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1`,
          [appId]
        );
        const s = parseInt(syns.rows[0].count);
        const covered = parseInt(syns.rows[0].columns_covered);
        const t = parseInt(totalCols.rows[0].count);
        qualityScore = t > 0 ? Math.round((covered / t) * 100) : 0;
        qualityDetails = { total_synonyms: s, columns_with_synonyms: covered, total_columns: t };
        break;
      }

      case 'curate': {
        // Verify: human review completion
        const total = await query(
          `SELECT COUNT(*) as count FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id WHERE at.app_id = $1`,
          [appId]
        );
        const reviewed = await query(
          `SELECT COUNT(*) as count FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
           WHERE at.app_id = $1 AND ac.enrichment_status IN ('approved', 'rejected')`,
          [appId]
        );
        const t = parseInt(total.rows[0].count);
        const r = parseInt(reviewed.rows[0].count);
        qualityScore = t > 0 ? Math.round((r / t) * 100) : 0;
        qualityDetails = { total_columns: t, reviewed_columns: r };
        break;
      }

      case 'index': {
        // Verify: vector embeddings exist (check app config or embeddings table)
        const app = await query(`SELECT config FROM applications WHERE id = $1`, [appId]);
        const config = app.rows[0]?.config || {};
        const hasEmbeddings = config.embeddings_built || config.vector_index_built;
        qualityScore = hasEmbeddings ? 100 : 0;
        qualityDetails = { embeddings_built: !!hasEmbeddings };
        break;
      }

      case 'validate': {
        // Verify: test queries run with acceptable accuracy
        const tests = await query(
          `SELECT COUNT(*) as total,
                  COUNT(CASE WHEN feedback = 'thumbs_up' THEN 1 END) as passed,
                  COUNT(CASE WHEN feedback = 'thumbs_down' THEN 1 END) as failed
           FROM test_queries WHERE app_id = $1`,
          [appId]
        );
        const t = parseInt(tests.rows[0].total);
        const p = parseInt(tests.rows[0].passed);
        qualityScore = t > 0 ? Math.round((p / t) * 100) : 0;
        qualityDetails = {
          total_tests: t,
          passed: p,
          failed: parseInt(tests.rows[0].failed),
          unrated: t - p - parseInt(tests.rows[0].failed),
        };
        break;
      }

      default:
        qualityDetails = { message: 'No automated verification available for this step' };
        qualityScore = null;
    }

    // Save quality score to the step
    if (qualityScore !== null) {
      await query(
        `UPDATE pipeline_steps SET quality_score = $1, quality_details = $2, updated_at = NOW()
         WHERE app_id = $3 AND step_name = $4`,
        [qualityScore, JSON.stringify(qualityDetails), appId, stepName]
      );
    }

    res.json({ stepName, qualityScore, qualityDetails });
  } catch (err) {
    console.error('Verify pipeline step error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/pipeline-steps/:appId/reset/:stepName — reset a step (and optionally downstream)
//   Body: { cascade?: boolean } — if true, also reset all downstream steps
router.post('/:appId/reset/:stepName', async (req, res) => {
  try {
    const { appId, stepName } = req.params;
    const { cascade = false } = req.body || {};

    const stepDef = STEP_DEFINITIONS.find(d => d.name === stepName);
    if (!stepDef) {
      return res.status(404).json({ error: `Unknown step: ${stepName}` });
    }

    let resetCondition = `step_name = $2`;
    if (cascade) {
      resetCondition = `step_order >= $2`;
    }

    const result = await query(
      `UPDATE pipeline_steps
       SET status = 'not_started', quality_score = NULL, quality_details = '{}',
           started_at = NULL, completed_at = NULL, completed_by = NULL, updated_at = NOW()
       WHERE app_id = $1 AND ${cascade ? 'step_order >= $2' : 'step_name = $2'}
       RETURNING step_name`,
      [appId, cascade ? stepDef.order : stepName]
    );

    res.json({ reset: result.rows.map(r => r.step_name) });
  } catch (err) {
    console.error('Reset pipeline step error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/pipeline-steps/definitions — return the step definitions (for UI rendering)
router.get('/definitions/all', async (req, res) => {
  res.json({ steps: STEP_DEFINITIONS });
});

module.exports = router;
