const express = require('express');
const { query } = require('../db');

const router = express.Router();

// GET /api/curation/:appId/review-queue - items needing human review
router.get('/:appId/review-queue', async (req, res) => {
  try {
    const { appId } = req.params;
    const limit = req.query.limit || 50;

    const result = await query(
      `SELECT
        ac.id,
        ac.column_name,
        at.table_name,
        ac.business_name,
        ac.description,
        ac.value_mapping,
        ac.enrichment_status,
        ac.confidence_score,
        ac.enriched_at
      FROM app_columns ac
      JOIN app_tables at ON ac.table_id = at.id
      WHERE at.app_id = $1 AND ac.enrichment_status IN ('draft', 'needs_review')
      ORDER BY ac.confidence_score DESC, ac.enriched_at DESC
      LIMIT $2`,
      [appId, limit]
    );

    res.json({
      items: result.rows,
    });
  } catch (err) {
    console.error('Get review queue error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// PUT /api/curation/:appId/columns/:colId - update enrichment
router.put('/:appId/columns/:colId', async (req, res) => {
  try {
    const { appId, colId } = req.params;
    const { business_name, description, value_mapping, enrichment_status } = req.body;

    // Get current values for curation log
    const currentResult = await query(
      `SELECT business_name, description, value_mapping FROM app_columns WHERE id = $1`,
      [colId]
    );

    if (currentResult.rows.length === 0) {
      return res.status(404).json({ error: 'Column not found' });
    }

    const current = currentResult.rows[0];
    const updates = [];
    const params = [];
    let paramCount = 1;

    if (business_name !== undefined) {
      updates.push(`business_name = $${paramCount++}`);
      params.push(business_name);
    }
    if (description !== undefined) {
      updates.push(`description = $${paramCount++}`);
      params.push(description);
    }
    if (value_mapping !== undefined) {
      updates.push(`value_mapping = $${paramCount++}`);
      params.push(JSON.stringify(value_mapping));
    }
    if (enrichment_status !== undefined) {
      updates.push(`enrichment_status = $${paramCount++}`);
      params.push(enrichment_status);
    }

    if (updates.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    params.push(colId);

    const updateResult = await query(
      `UPDATE app_columns SET ${updates.join(', ')} WHERE id = $${paramCount} RETURNING *`,
      params
    );

    // Log the changes
    if (business_name !== undefined && business_name !== current.business_name) {
      await query(
        `INSERT INTO curation_log (column_id, user_id, action, old_value, new_value, created_at)
         VALUES ($1, $2, $3, $4, $5, NOW())`,
        [colId, req.user.id, 'update_business_name', current.business_name, business_name]
      );
    }

    res.json({
      column: updateResult.rows[0],
    });
  } catch (err) {
    console.error('Update enrichment error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/curation/:appId/columns/:colId/approve - approve enrichment
router.post('/:appId/columns/:colId/approve', async (req, res) => {
  try {
    const { appId, colId } = req.params;

    const result = await query(
      `UPDATE app_columns
       SET enrichment_status = 'approved', enriched_by = $1
       WHERE id = $2
       RETURNING *`,
      [req.user.id, colId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Column not found' });
    }

    // Log approval
    await query(
      `INSERT INTO curation_log (column_id, user_id, action, created_at)
       VALUES ($1, $2, $3, NOW())`,
      [colId, req.user.id, 'approve']
    );

    res.json({
      column: result.rows[0],
      message: 'Enrichment approved',
    });
  } catch (err) {
    console.error('Approve enrichment error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/curation/:appId/columns/:colId/reject - reject enrichment
router.post('/:appId/columns/:colId/reject', async (req, res) => {
  try {
    const { appId, colId } = req.params;
    const { feedback } = req.body;

    const result = await query(
      `UPDATE app_columns
       SET enrichment_status = 'rejected'
       WHERE id = $1
       RETURNING *`,
      [colId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Column not found' });
    }

    // Log rejection
    await query(
      `INSERT INTO curation_log (column_id, user_id, action, new_value, created_at)
       VALUES ($1, $2, $3, $4, NOW())`,
      [colId, req.user.id, 'reject', feedback || null]
    );

    res.json({
      column: result.rows[0],
      message: 'Enrichment rejected',
    });
  } catch (err) {
    console.error('Reject enrichment error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/curation/:appId/bulk-approve - bulk approve selected items
router.post('/:appId/bulk-approve', async (req, res) => {
  try {
    const { appId } = req.params;
    const { columnIds } = req.body;

    if (!Array.isArray(columnIds) || columnIds.length === 0) {
      return res.status(400).json({ error: 'Column IDs array required' });
    }

    const placeholders = columnIds.map((_, i) => `$${i + 1}`).join(',');

    const result = await query(
      `UPDATE app_columns
       SET enrichment_status = 'approved', enriched_by = $${columnIds.length + 1}
       WHERE id IN (${placeholders})
       RETURNING id`,
      [...columnIds, req.user.id]
    );

    // Log approvals
    for (const colId of columnIds) {
      await query(
        `INSERT INTO curation_log (column_id, user_id, action, created_at)
         VALUES ($1, $2, $3, NOW())`,
        [colId, req.user.id, 'bulk_approve']
      );
    }

    res.json({
      approved_count: result.rows.length,
      message: `${result.rows.length} items approved`,
    });
  } catch (err) {
    console.error('Bulk approve error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/curation/:appId/stats - curation statistics
router.get('/:appId/stats', async (req, res) => {
  try {
    const { appId } = req.params;

    const result = await query(
      `SELECT
        COUNT(DISTINCT CASE WHEN enrichment_status = 'approved' THEN id END) as approved_count,
        COUNT(DISTINCT CASE WHEN enrichment_status IN ('draft', 'needs_review') THEN id END) as needs_review_count,
        COUNT(DISTINCT CASE WHEN enrichment_status = 'draft' THEN id END) as draft_count,
        COUNT(DISTINCT CASE WHEN enrichment_status = 'rejected' THEN id END) as rejected_count,
        COUNT(DISTINCT id) as total_columns
      FROM app_columns
      JOIN app_tables ON app_columns.table_id = app_tables.id
      WHERE app_tables.app_id = $1`,
      [appId]
    );

    res.json({
      stats: result.rows[0],
    });
  } catch (err) {
    console.error('Get curation stats error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
