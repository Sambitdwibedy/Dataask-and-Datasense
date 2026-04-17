const express = require('express');
const { query } = require('../db');

const router = express.Router();

// GET /api/applications - list all applications with summary stats
router.get('/', async (req, res) => {
  try {
    const [rows] = await query(`
      SELECT
        a.id,
        a.name,
        a.type,
        a.description,
        a.status,
        a.created_at,
        a.updated_at,
        COUNT(DISTINCT at.id) as table_count,
        COUNT(DISTINCT ac.id) as column_count,
        COALESCE(ROUND(100.0 * SUM(CASE WHEN ac.enrichment_status IN ('ai_enriched', 'approved', 'needs_review') THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT ac.id), 0), 2), 0) as enrichment_percentage
      FROM applications a
      LEFT JOIN app_tables at ON a.id = at.app_id
      LEFT JOIN app_columns ac ON at.id = ac.table_id
      GROUP BY a.id, a.name, a.type, a.description, a.status, a.created_at, a.updated_at
      ORDER BY a.created_at DESC
    `);

    res.json({
      applications: rows,
    });
  } catch (err) {
    console.error('Get applications error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/applications/:id - get application detail with domains (DB: app_modules)
router.get('/:id', async (req, res) => {
  try {
    const { id } = req.params;

    const [appRows] = await query('SELECT * FROM applications WHERE id = ?', [id]);

    if (appRows.length === 0) {
      return res.status(404).json({ error: 'Application not found' });
    }

    const [modulesRows] = await query('SELECT * FROM app_modules WHERE app_id = ? ORDER BY code', [id]);

    res.json({
      application: appRows[0],
      modules: modulesRows,
    });
  } catch (err) {
    console.error('Get application error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/applications/:id/builder-progress - get builder guide step status
router.get('/:id/builder-progress', async (req, res) => {
  try {
    const { id } = req.params;

    // Gather all signals in parallel
    const [[appR], [tableR], [enrichR], [contextR], [pipelineR], [relR], [patternR]] = await Promise.all([
      query('SELECT status FROM applications WHERE id = ?', [id]),
      query('SELECT COUNT(*) as cnt FROM app_tables WHERE app_id = ?', [id]),
      query(`SELECT COUNT(*) as total, SUM(CASE WHEN enrichment_status IN ('ai_enriched','approved','needs_review') THEN 1 ELSE 0 END) as enriched FROM app_columns WHERE table_id IN (SELECT id FROM app_tables WHERE app_id = ?)`, [id]),
      query('SELECT COUNT(*) as cnt FROM context_documents WHERE app_id = ?', [id]),
      query('SELECT id, status, stages FROM pipeline_runs WHERE app_id = ? ORDER BY started_at DESC LIMIT 1', [id]),
      query('SELECT COUNT(*) as cnt FROM app_relationships WHERE app_id = ?', [id]),
      query('SELECT COUNT(*) as cnt FROM test_queries WHERE app_id = ?', [id]),
    ]);

    const appStatus = appR[0]?.status || 'new';
    const tableCount = parseInt(tableR[0]?.cnt || 0);
    const totalCols = parseInt(enrichR[0]?.total || 0);
    const enrichedCols = parseInt(enrichR[0]?.enriched || 0);
    const contextDocs = parseInt(contextR[0]?.cnt || 0);
    const lastRun = pipelineR[0] || null;
    const relCount = parseInt(relR[0]?.cnt || 0);
    const patternCount = parseInt(patternR[0]?.cnt || 0);

    res.json({
      steps: {
        ingest: { complete: tableCount > 0, tables: tableCount, columns: totalCols, relationships: relCount },
        context: { complete: contextDocs > 0, documents: contextDocs },
        enrich: { complete: enrichedCols > 0, enriched: enrichedCols, total: totalCols, pct: totalCols > 0 ? Math.round(100 * enrichedCols / totalCols) : 0 },
        review: { complete: false }, // tracked client-side
        iterate: { complete: false }, // ongoing activity
        publish: { complete: appStatus === 'published', status: appStatus, patterns: patternCount },
      },
      last_pipeline_run: lastRun ? { id: lastRun.id, status: lastRun.status, stages: lastRun.stages } : null,
    });
  } catch (err) {
    console.error('Builder progress error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/applications - create new application
router.post('/', async (req, res) => {
  try {
    const { name, type, description, config } = req.body;

    if (!name || !type) {
      return res.status(400).json({ error: 'Name and type required' });
    }

    const [result] = await query(
      `INSERT INTO applications (name, type, description, config, status, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, NOW(), NOW())`,
      [name, type, description || null, JSON.stringify(config || {}), 'draft']
    );

    const [newRows] = await query('SELECT * FROM applications WHERE id = ?', [result.insertId]);
    res.status(201).json({
      application: newRows[0],
    });
  } catch (err) {
    console.error('Create application error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// PUT /api/applications/:id - update application
router.put('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { name, type, description, config, status } = req.body;

    const updates = [];
    const params = [];

    if (name !== undefined) {
      updates.push(`name = ?`);
      params.push(name);
    }
    if (type !== undefined) {
      updates.push(`type = ?`);
      params.push(type);
    }
    if (description !== undefined) {
      updates.push(`description = ?`);
      params.push(description);
    }
    if (config !== undefined) {
      updates.push(`config = ?`);
      params.push(JSON.stringify(config));
    }
    if (status !== undefined) {
      updates.push(`status = ?`);
      params.push(status);
    }

    updates.push(`updated_at = NOW()`);
    params.push(id);

    if (updates.length === 1) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    const [result] = await query(`UPDATE applications SET ${updates.join(', ')} WHERE id = ?`, params);

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Application not found' });
    }

    const [updatedRows] = await query('SELECT * FROM applications WHERE id = ?', [id]);
    res.json({
      application: updatedRows[0],
    });
  } catch (err) {
    console.error('Update application error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// DELETE /api/applications/:id - delete application
router.delete('/:id', async (req, res) => {
  try {
    const { id } = req.params;

    // Fetch before delete so we can return the deleted record
    const [appRows] = await query('SELECT * FROM applications WHERE id = ?', [id]);
    if (appRows.length === 0) {
      return res.status(404).json({ error: 'Application not found' });
    }

    await query('DELETE FROM applications WHERE id = ?', [id]);

    res.json({
      message: 'Application deleted',
      application: appRows[0],
    });
  } catch (err) {
    console.error('Delete application error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/applications/:id/publish - publish BOKG for end-user consumption
router.post('/:id/publish', async (req, res) => {
  try {
    const { id } = req.params;

    // Check app exists and has enrichment data
    const [appCheckRows] = await query(
      `SELECT a.id, a.status,
              COUNT(DISTINCT ac.id) as column_count,
              SUM(CASE WHEN ac.enrichment_status IN ('ai_enriched', 'approved', 'modified', 'needs_review') THEN 1 ELSE 0 END) as enriched_count
       FROM applications a
       LEFT JOIN app_tables at ON a.id = at.app_id
       LEFT JOIN app_columns ac ON at.id = ac.table_id
       WHERE a.id = ?
       GROUP BY a.id, a.status`,
      [id]
    );

    if (appCheckRows.length === 0) {
      return res.status(404).json({ error: 'Application not found' });
    }

    const app = appCheckRows[0];
    if (parseInt(app.enriched_count) === 0) {
      return res.status(400).json({ error: 'Cannot publish — no enrichment data. Run the pipeline first.' });
    }

    await query(
      `UPDATE applications SET status = 'published', published_at = NOW(), published_by = ?, updated_at = NOW()
       WHERE id = ?`,
      [req.user.id, id]
    );

    // Generate semantic embeddings for schema linking (non-blocking, non-critical)
    try {
      const { generateEmbeddingsForApp } = require('../services/embedding-service');
      generateEmbeddingsForApp(parseInt(id), { force: true }).then(embResult => {
        console.log(`[Publish] Embeddings generated for appId=${id}:`, embResult);
      }).catch(embErr => {
        console.warn(`[Publish] Embedding generation failed (non-critical):`, embErr.message);
      });
    } catch (e) { /* embedding service not available */ }

    const [publishedRows] = await query('SELECT * FROM applications WHERE id = ?', [id]);
    res.json({
      application: publishedRows[0],
      message: 'BOKG published for end-user access',
      stats: { columns: parseInt(app.column_count), enriched: parseInt(app.enriched_count) }
    });
  } catch (err) {
    console.error('Publish error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/applications/:id/generate-embeddings - generate semantic embeddings for schema linking
router.post('/:id/generate-embeddings', async (req, res) => {
  try {
    const { id } = req.params;
    const force = req.query.force === 'true';

    const [appCheckRows] = await query('SELECT id, name, status FROM applications WHERE id = ?', [id]);
    if (appCheckRows.length === 0) {
      return res.status(404).json({ error: 'Application not found' });
    }

    const { generateEmbeddingsForApp } = require('../services/embedding-service');
    const result = await generateEmbeddingsForApp(parseInt(id), { force });

    res.json({
      application: appCheckRows[0].name,
      embeddings: result,
    });
  } catch (err) {
    console.error('Generate embeddings error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/applications/:id/unpublish - take BOKG offline for further editing
router.post('/:id/unpublish', async (req, res) => {
  try {
    const { id } = req.params;
    const [result] = await query(
      `UPDATE applications SET status = 'in_review', updated_at = NOW()
       WHERE id = ?`,
      [id]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: 'Application not found' });
    }

    const [updatedRows] = await query('SELECT * FROM applications WHERE id = ?', [id]);
    res.json({ application: updatedRows[0], message: 'BOKG unpublished — back in review mode' });
  } catch (err) {
    console.error('Unpublish error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/applications/published - list only published apps (for end-users)
router.get('/published/list', async (req, res) => {
  try {
    const [rows] = await query(`
      SELECT
        a.id, a.name, a.type, a.description, a.published_at, a.config,
        COUNT(DISTINCT at.id) as table_count,
        COUNT(DISTINCT ac.id) as column_count,
        u.name as published_by_name
      FROM applications a
      LEFT JOIN app_tables at ON a.id = at.app_id
      LEFT JOIN app_columns ac ON at.id = ac.table_id
      LEFT JOIN users u ON a.published_by = u.id
      WHERE a.status = 'published'
      GROUP BY a.id, a.name, a.type, a.description, a.published_at, u.name
      ORDER BY a.published_at DESC
    `);
    res.json({ applications: rows });
  } catch (err) {
    console.error('Get published apps error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/applications/:id/modules - list domains for an application (DB: app_modules)
router.get('/:id/modules', async (req, res) => {
  try {
    const { id } = req.params;

    const [rows] = await query('SELECT * FROM app_modules WHERE app_id = ? ORDER BY code', [id]);

    res.json({
      modules: rows,
    });
  } catch (err) {
    console.error('Get modules error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/applications/:id/modules/:moduleId/objects - list business objects in a domain (DB: app_modules)
router.get('/:id/modules/:moduleId/objects', async (req, res) => {
  try {
    const { id, moduleId } = req.params;

    const [rows] = await query(
      `SELECT * FROM app_tables WHERE app_id = ? AND module_id = ? ORDER BY entity_name`,
      [id, moduleId]
    );

    res.json({
      objects: rows,
    });
  } catch (err) {
    console.error('Get module objects error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
