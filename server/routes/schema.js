const express = require('express');
const { query } = require('../db');

const router = express.Router();

// GET /api/schema/:appId/tables - list tables with column counts, enrichment status, entity metadata
router.get('/:appId/tables', async (req, res) => {
  try {
    const { appId } = req.params;

    const result = await query(
      `SELECT
        at.id,
        at.table_name,
        at.entity_name,
        at.row_count,
        at.description,
        at.entity_metadata,
        at.enrichment_status,
        at.confidence_score,
        at.enriched_by,
        at.enriched_at,
        COUNT(DISTINCT ac.id) as column_count,
        COUNT(DISTINCT CASE WHEN ac.enrichment_status IN ('ai_enriched', 'approved', 'needs_review') THEN ac.id END) as enriched_column_count,
        COALESCE(ROUND(100.0 * COUNT(DISTINCT CASE WHEN ac.enrichment_status IN ('ai_enriched', 'approved', 'needs_review') THEN ac.id END) / NULLIF(COUNT(DISTINCT ac.id), 0), 2), 0) as enrichment_percentage
      FROM app_tables at
      LEFT JOIN app_columns ac ON at.id = ac.table_id
      WHERE at.app_id = $1
      GROUP BY at.id, at.table_name, at.entity_name, at.row_count, at.description, at.entity_metadata,
               at.enrichment_status, at.confidence_score, at.enriched_by, at.enriched_at
      ORDER BY at.table_name`,
      [appId]
    );

    // Summary counts for entity curation
    const tables = result.rows;
    const entitySummary = {
      total: tables.length,
      approved: tables.filter(t => t.enrichment_status === 'approved').length,
      rejected: tables.filter(t => t.enrichment_status === 'rejected').length,
      pending: tables.filter(t => !t.enrichment_status || t.enrichment_status === 'draft' || t.enrichment_status === 'ai_enriched').length,
    };

    res.json({ tables, entitySummary });
  } catch (err) {
    console.error('Get tables error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/schema/:appId/tables/:tableRef/columns - accepts table ID or table_name
router.get('/:appId/tables/:tableRef/columns', async (req, res) => {
  try {
    const { appId, tableRef } = req.params;

    // Support both numeric IDs and table names
    const isNumeric = /^\d+$/.test(tableRef);
    let tableCondition, tableParam;
    if (isNumeric) {
      tableCondition = 'ac.table_id = $2';
      tableParam = parseInt(tableRef);
    } else {
      tableCondition = 'at.table_name = $2';
      tableParam = tableRef;
    }

    const result = await query(
      `SELECT
        ac.id,
        ac.column_name,
        ac.data_type,
        ac.is_pk,
        ac.is_fk,
        ac.fk_reference,
        ac.business_name,
        ac.description,
        ac.value_mapping,
        ac.enrichment_status,
        ac.confidence_score,
        ac.enriched_by,
        ac.enriched_at,
        at.table_name,
        at.description as table_description,
        at.entity_metadata
      FROM app_columns ac
      JOIN app_tables at ON ac.table_id = at.id
      WHERE at.app_id = $1 AND ${tableCondition}
      ORDER BY ac.column_name`,
      [appId, tableParam]
    );

    // Parse value_mapping from TEXT to JSON for each column
    const columns = result.rows.map(row => {
      let parsedMapping = null;
      if (row.value_mapping) {
        try {
          parsedMapping = JSON.parse(row.value_mapping);
        } catch (e) {
          parsedMapping = row.value_mapping;
        }
      }
      return {
        ...row,
        value_mapping: parsedMapping,
        is_primary_key: row.is_pk,
        is_foreign_key: row.is_fk,
      };
    });

    res.json({ columns });
  } catch (err) {
    console.error('Get columns error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/schema/:appId/relationships - list foreign key relationships with approval status
router.get('/:appId/relationships', async (req, res) => {
  try {
    const { appId } = req.params;

    const result = await query(
      `SELECT
        ar.id,
        ar.rel_type,
        ar.cardinality,
        ar.enrichment_status,
        ar.confidence_score,
        ar.enriched_by,
        ar.enriched_at,
        ar.from_table_id,
        ar.to_table_id,
        ft.table_name as from_table,
        ar.from_column,
        tt.table_name as to_table,
        ar.to_column
      FROM app_relationships ar
      JOIN app_tables ft ON ar.from_table_id = ft.id
      JOIN app_tables tt ON ar.to_table_id = tt.id
      WHERE ar.app_id = $1
      ORDER BY ft.table_name, ar.from_column`,
      [appId]
    );

    const relationships = result.rows;
    const relSummary = {
      total: relationships.length,
      approved: relationships.filter(r => r.enrichment_status === 'approved').length,
      rejected: relationships.filter(r => r.enrichment_status === 'rejected').length,
      pending: relationships.filter(r => !r.enrichment_status || r.enrichment_status === 'ai_enriched' || r.enrichment_status === 'draft').length,
    };

    res.json({ relationships, relSummary });
  } catch (err) {
    console.error('Get relationships error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/schema/:appId/search - search across tables and columns
router.get('/:appId/search', async (req, res) => {
  try {
    const { appId } = req.params;
    const { q } = req.query;

    if (!q) {
      return res.status(400).json({ error: 'Search term required' });
    }

    const searchTerm = `%${q}%`;

    const result = await query(
      `SELECT
        'table' as type,
        at.id,
        at.table_name as name,
        at.entity_name as display_name,
        NULL as parent_table
      FROM app_tables at
      WHERE at.app_id = $1 AND (at.table_name ILIKE $2 OR at.entity_name ILIKE $2)

      UNION ALL

      SELECT
        'column' as type,
        ac.id,
        ac.column_name as name,
        ac.business_name as display_name,
        at.table_name as parent_table
      FROM app_columns ac
      JOIN app_tables at ON ac.table_id = at.id
      WHERE at.app_id = $1 AND (ac.column_name ILIKE $2 OR ac.business_name ILIKE $2 OR ac.description ILIKE $2)

      ORDER BY type, name`,
      [appId, searchTerm]
    );

    res.json({
      results: result.rows,
    });
  } catch (err) {
    console.error('Search error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/schema/:appId/columns - all columns with pagination, search, tier/status filters
router.get('/:appId/columns', async (req, res) => {
  try {
    const { appId } = req.params;
    const { status, tier, search, page = 1, limit = 50 } = req.query;
    const pageNum = Math.max(1, parseInt(page));
    const limitNum = Math.min(200, Math.max(1, parseInt(limit)));
    const offset = (pageNum - 1) * limitNum;

    let filters = '';
    const params = [appId];

    if (status) {
      params.push(status);
      filters += ` AND ac.enrichment_status = $${params.length}`;
    }

    if (tier === 'auto') {
      filters += ' AND ac.confidence_score >= 90';
    } else if (tier === 'review') {
      filters += ' AND ac.confidence_score >= 60 AND ac.confidence_score < 90';
    } else if (tier === 'manual') {
      filters += ' AND ac.confidence_score < 60';
    }

    if (search) {
      params.push(`%${search}%`);
      filters += ` AND (ac.column_name ILIKE $${params.length} OR ac.business_name ILIKE $${params.length} OR at.table_name ILIKE $${params.length} OR ac.description ILIKE $${params.length})`;
    }

    // Get summary counts (always unfiltered by pagination, but respects tier/status/search)
    const countResult = await query(
      `SELECT
        COUNT(*) as total,
        COUNT(*) FILTER (WHERE ac.confidence_score >= 90) as auto_approve,
        COUNT(*) FILTER (WHERE ac.confidence_score >= 60 AND ac.confidence_score < 90) as review,
        COUNT(*) FILTER (WHERE ac.confidence_score < 60) as manual,
        COUNT(*) FILTER (WHERE ac.enrichment_status = 'approved') as approved,
        COUNT(*) FILTER (WHERE ac.enrichment_status = 'rejected') as rejected,
        COUNT(*) FILTER (WHERE ac.enrichment_status = 'ai_enriched') as ai_enriched,
        COUNT(*) FILTER (WHERE ac.enrichment_status = 'draft') as draft
      FROM app_columns ac
      JOIN app_tables at ON ac.table_id = at.id
      WHERE at.app_id = $1`,
      [appId]
    );

    // Get filtered count for pagination
    const filteredCountResult = await query(
      `SELECT COUNT(*) as filtered_total
       FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = $1${filters}`,
      params
    );

    // Get paginated results
    const dataParams = [...params, limitNum, offset];
    const result = await query(
      `SELECT
        ac.id, ac.column_name, ac.data_type, ac.is_pk, ac.is_fk, ac.fk_reference,
        ac.business_name, ac.description, ac.value_mapping,
        ac.enrichment_status, ac.confidence_score, ac.enriched_by, ac.enriched_at,
        ac.table_id, at.table_name, at.entity_name, at.entity_metadata
      FROM app_columns ac
      JOIN app_tables at ON ac.table_id = at.id
      WHERE at.app_id = $1${filters}
      ORDER BY ac.confidence_score ASC, at.table_name, ac.column_name
      LIMIT $${dataParams.length - 1} OFFSET $${dataParams.length}`,
      dataParams
    );

    const columns = result.rows.map(row => {
      let parsedMapping = null;
      if (row.value_mapping) {
        try { parsedMapping = JSON.parse(row.value_mapping); } catch (e) { parsedMapping = row.value_mapping; }
      }
      return { ...row, value_mapping: parsedMapping };
    });

    const summary = countResult.rows[0] || {};
    const filteredTotal = parseInt(filteredCountResult.rows[0]?.filtered_total || 0);

    res.json({
      columns,
      summary: {
        total: parseInt(summary.total || 0),
        auto_approve: parseInt(summary.auto_approve || 0),
        review: parseInt(summary.review || 0),
        manual: parseInt(summary.manual || 0),
        approved: parseInt(summary.approved || 0),
        rejected: parseInt(summary.rejected || 0),
        ai_enriched: parseInt(summary.ai_enriched || 0),
        draft: parseInt(summary.draft || 0),
      },
      pagination: {
        page: pageNum,
        limit: limitNum,
        filtered_total: filteredTotal,
        total_pages: Math.ceil(filteredTotal / limitNum),
      },
    });
  } catch (err) {
    console.error('Get all columns error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// PUT /api/schema/:appId/columns/:columnId - update column enrichment data
router.put('/:appId/columns/:columnId', async (req, res) => {
  try {
    const { appId, columnId } = req.params;
    const { business_name, description, enrichment_status, value_mapping } = req.body;

    // Build dynamic SET clause
    const sets = [];
    const params = [];
    let idx = 1;

    if (business_name !== undefined) { sets.push(`business_name = $${idx++}`); params.push(business_name); }
    if (description !== undefined) { sets.push(`description = $${idx++}`); params.push(description); }
    if (enrichment_status !== undefined) {
      sets.push(`enrichment_status = $${idx++}`); params.push(enrichment_status);
      sets.push(`enriched_by = $${idx++}`); params.push('human');
      sets.push(`enriched_at = NOW()`);
    }
    if (value_mapping !== undefined) {
      sets.push(`value_mapping = $${idx++}`);
      params.push(typeof value_mapping === 'string' ? value_mapping : JSON.stringify(value_mapping));
    }

    if (sets.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    // Verify column belongs to this app
    params.push(columnId);
    params.push(appId);
    const result = await query(
      `UPDATE app_columns ac SET ${sets.join(', ')}
       FROM app_tables at
       WHERE ac.id = $${idx++} AND ac.table_id = at.id AND at.app_id = $${idx}
       RETURNING ac.*`,
      params
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Column not found' });
    }

    res.json({ column: result.rows[0] });
  } catch (err) {
    console.error('Update column error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/schema/:appId/columns/bulk-approve - bulk approve columns by tier
router.post('/:appId/columns/bulk-approve', async (req, res) => {
  try {
    const { appId } = req.params;
    const { min_confidence = 90, column_ids } = req.body;

    let result;
    if (column_ids && column_ids.length > 0) {
      // Approve specific columns
      result = await query(
        `UPDATE app_columns ac SET enrichment_status = 'approved', enriched_by = 'human', enriched_at = NOW()
         FROM app_tables at
         WHERE ac.table_id = at.id AND at.app_id = $1
           AND ac.id = ANY($2)
           AND ac.enrichment_status != 'approved'
         RETURNING ac.id`,
        [appId, column_ids]
      );
    } else {
      // Approve all above confidence threshold
      result = await query(
        `UPDATE app_columns ac SET enrichment_status = 'approved', enriched_by = 'human', enriched_at = NOW()
         FROM app_tables at
         WHERE ac.table_id = at.id AND at.app_id = $1
           AND ac.confidence_score >= $2
           AND ac.enrichment_status IN ('ai_enriched', 'needs_review')
         RETURNING ac.id`,
        [appId, min_confidence]
      );
    }

    res.json({ approved_count: result.rows.length });
  } catch (err) {
    console.error('Bulk approve error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// PUT /api/schema/:appId/tables/:tableId/metadata - update entity metadata + approval status
router.put('/:appId/tables/:tableId/metadata', async (req, res) => {
  try {
    const { appId, tableId } = req.params;
    const { entity_name, description, entity_metadata, enrichment_status } = req.body;

    const sets = [];
    const params = [];
    let idx = 1;

    if (entity_name !== undefined) { sets.push(`entity_name = $${idx++}`); params.push(entity_name); }
    if (description !== undefined) { sets.push(`description = $${idx++}`); params.push(description); }
    if (entity_metadata !== undefined) {
      sets.push(`entity_metadata = $${idx++}`);
      params.push(typeof entity_metadata === 'string' ? entity_metadata : JSON.stringify(entity_metadata));
    }
    if (enrichment_status !== undefined) {
      sets.push(`enrichment_status = $${idx++}`); params.push(enrichment_status);
      sets.push(`enriched_by = $${idx++}`); params.push('human');
      sets.push(`enriched_at = NOW()`);
    }

    if (sets.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    params.push(tableId);
    params.push(appId);
    const result = await query(
      `UPDATE app_tables SET ${sets.join(', ')} WHERE id = $${idx++} AND app_id = $${idx} RETURNING *`,
      params
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Table not found' });
    }

    res.json({ table: result.rows[0] });
  } catch (err) {
    console.error('Update table metadata error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/schema/:appId/tables/bulk-approve - bulk approve entities
router.post('/:appId/tables/bulk-approve', async (req, res) => {
  try {
    const { appId } = req.params;
    const { table_ids } = req.body;

    let result;
    if (table_ids && table_ids.length > 0) {
      result = await query(
        `UPDATE app_tables SET enrichment_status = 'approved', enriched_by = 'human', enriched_at = NOW()
         WHERE app_id = $1 AND id = ANY($2) AND enrichment_status != 'approved'
         RETURNING id`,
        [appId, table_ids]
      );
    } else {
      result = await query(
        `UPDATE app_tables SET enrichment_status = 'approved', enriched_by = 'human', enriched_at = NOW()
         WHERE app_id = $1 AND enrichment_status IN ('draft', 'ai_enriched', 'needs_review')
         RETURNING id`,
        [appId]
      );
    }

    res.json({ approved_count: result.rows.length });
  } catch (err) {
    console.error('Bulk approve entities error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// PUT /api/schema/:appId/relationships/:relId - update relationship metadata + approval
router.put('/:appId/relationships/:relId', async (req, res) => {
  try {
    const { appId, relId } = req.params;
    const { rel_type, cardinality, enrichment_status } = req.body;

    const sets = [];
    const params = [];
    let idx = 1;

    if (rel_type !== undefined) { sets.push(`rel_type = $${idx++}`); params.push(rel_type); }
    if (cardinality !== undefined) { sets.push(`cardinality = $${idx++}`); params.push(cardinality); }
    if (enrichment_status !== undefined) {
      sets.push(`enrichment_status = $${idx++}`); params.push(enrichment_status);
      sets.push(`enriched_by = $${idx++}`); params.push('human');
      sets.push(`enriched_at = NOW()`);
    }

    if (sets.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    params.push(relId);
    params.push(appId);
    const result = await query(
      `UPDATE app_relationships SET ${sets.join(', ')} WHERE id = $${idx++} AND app_id = $${idx} RETURNING *`,
      params
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Relationship not found' });
    }

    res.json({ relationship: result.rows[0] });
  } catch (err) {
    console.error('Update relationship error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/schema/:appId/relationships - create a new relationship manually
router.post('/:appId/relationships', async (req, res) => {
  try {
    const { appId } = req.params;
    const { from_table_name, from_column, to_table_name, to_column, rel_type = 'inferred', cardinality } = req.body;

    if (!from_table_name || !from_column || !to_table_name || !to_column) {
      return res.status(400).json({ error: 'from_table_name, from_column, to_table_name, to_column are required' });
    }

    // Resolve table names to IDs
    const fromTable = await query('SELECT id FROM app_tables WHERE app_id = $1 AND table_name = $2', [appId, from_table_name]);
    const toTable = await query('SELECT id FROM app_tables WHERE app_id = $1 AND table_name = $2', [appId, to_table_name]);

    if (fromTable.rows.length === 0) return res.status(404).json({ error: `Table '${from_table_name}' not found` });
    if (toTable.rows.length === 0) return res.status(404).json({ error: `Table '${to_table_name}' not found` });

    const result = await query(
      `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality, enrichment_status, enriched_by, enriched_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'approved', 'human', NOW())
       ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING
       RETURNING *`,
      [appId, fromTable.rows[0].id, from_column, toTable.rows[0].id, to_column, rel_type, cardinality || null]
    );

    if (result.rows.length === 0) {
      return res.status(409).json({ error: 'Relationship already exists' });
    }

    res.json({ relationship: result.rows[0] });
  } catch (err) {
    console.error('Create relationship error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// DELETE /api/schema/:appId/relationships/:relId - delete a relationship
router.delete('/:appId/relationships/:relId', async (req, res) => {
  try {
    const { appId, relId } = req.params;
    const result = await query(
      'DELETE FROM app_relationships WHERE id = $1 AND app_id = $2 RETURNING id',
      [relId, appId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Relationship not found' });
    }

    res.json({ message: 'Relationship deleted' });
  } catch (err) {
    console.error('Delete relationship error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/schema/:appId/relationships/bulk-approve - bulk approve relationships
router.post('/:appId/relationships/bulk-approve', async (req, res) => {
  try {
    const { appId } = req.params;
    const { rel_ids } = req.body;

    let result;
    if (rel_ids && rel_ids.length > 0) {
      result = await query(
        `UPDATE app_relationships SET enrichment_status = 'approved', enriched_by = 'human', enriched_at = NOW()
         WHERE app_id = $1 AND id = ANY($2) AND enrichment_status != 'approved'
         RETURNING id`,
        [appId, rel_ids]
      );
    } else {
      result = await query(
        `UPDATE app_relationships SET enrichment_status = 'approved', enriched_by = 'human', enriched_at = NOW()
         WHERE app_id = $1 AND enrichment_status IN ('ai_enriched', 'draft', 'needs_review')
         RETURNING id`,
        [appId]
      );
    }

    res.json({ approved_count: result.rows.length });
  } catch (err) {
    console.error('Bulk approve relationships error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/schema/:appId/enrich-bulk - bulk update enrichment metadata for tables and columns
// Used by BIRD benchmark orchestrator to push AI enrichment results
router.post('/:appId/enrich-bulk', async (req, res) => {
  try {
    const { appId } = req.params;
    const { tables } = req.body;

    if (!tables || !Array.isArray(tables)) {
      return res.status(400).json({ error: 'tables array required' });
    }

    const results = { tables_updated: 0, columns_updated: 0 };

    for (const table of tables) {
      // Update table enrichment
      const tableResult = await query(
        `UPDATE app_tables SET
          description = COALESCE($2, description),
          entity_metadata = COALESCE($3, entity_metadata),
          enrichment_status = 'ai_enriched',
          enriched_by = 'ai',
          enriched_at = NOW()
        WHERE app_id = $1 AND table_name = $4
        RETURNING id`,
        [appId, table.description || null,
         table.entity_metadata ? JSON.stringify(table.entity_metadata) : null,
         table.table_name]
      );

      if (tableResult.rows.length === 0) continue;
      const tableId = tableResult.rows[0].id;
      results.tables_updated++;

      // Also update entity_name if provided
      if (table.entity_name) {
        await query('UPDATE app_tables SET entity_name = $1 WHERE id = $2', [table.entity_name, tableId]);
      }

      // Update column enrichments
      if (table.columns && Array.isArray(table.columns)) {
        for (const col of table.columns) {
          const status = (col.confidence_score || 0) >= 70 ? 'ai_enriched' : 'needs_review';
          await query(
            `UPDATE app_columns SET
              business_name = COALESCE($2, business_name),
              description = COALESCE($3, description),
              confidence_score = COALESCE($4, confidence_score),
              enrichment_status = $5,
              enriched_by = 'ai',
              enriched_at = NOW(),
              value_mapping = COALESCE($6, value_mapping)
            WHERE table_id = $1 AND column_name = $7`,
            [tableId, col.business_name || null, col.description || null,
             col.confidence_score || null, status,
             col.value_mapping ? JSON.stringify(col.value_mapping) : null,
             col.column_name]
          );
          results.columns_updated++;
        }
      }
    }

    res.json({ success: true, ...results });
  } catch (err) {
    console.error('Bulk enrich error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/schema/:appId/seed-value-dicts - upload value dictionaries from local profiling
router.post('/:appId/seed-value-dicts', async (req, res) => {
  try {
    const { appId } = req.params;
    const { tables } = req.body;

    if (!tables || !Array.isArray(tables)) {
      return res.status(400).json({ error: 'tables array required' });
    }

    let updated = 0;
    for (const table of tables) {
      if (!table.table_name || !table.value_dictionaries) continue;

      // Get table ID
      const tableResult = await query(
        'SELECT id FROM app_tables WHERE app_id = $1 AND table_name = $2', [appId, table.table_name]);
      if (tableResult.rows.length === 0) continue;
      const tableId = tableResult.rows[0].id;

      // Update each column's value_mapping with the profiled value dictionary
      for (const [colName, valueDict] of Object.entries(table.value_dictionaries)) {
        if (Object.keys(valueDict).length > 0) {
          await query(
            `UPDATE app_columns SET value_mapping = $1 WHERE table_id = $2 AND column_name = $3`,
            [JSON.stringify(valueDict), tableId, colName]
          );
          updated++;
        }
      }
    }

    res.json({ success: true, columns_updated: updated });
  } catch (err) {
    console.error('Seed value dicts error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/schema/:appId/seed-context-docs - upload context documents as text (no file upload needed)
router.post('/:appId/seed-context-docs', async (req, res) => {
  try {
    const { appId } = req.params;
    const { documents } = req.body;

    if (!documents || !Array.isArray(documents)) {
      return res.status(400).json({ error: 'documents array required' });
    }

    let inserted = 0;
    for (const doc of documents) {
      await query(
        `INSERT INTO context_documents (app_id, filename, file_type, file_size, extracted_text, description, uploaded_at)
         VALUES ($1, $2, $3, $4, $5, $6, NOW())
         ON CONFLICT DO NOTHING`,
        [appId, doc.filename, doc.file_type || 'text/csv', doc.text?.length || 0,
         doc.text, doc.description || `BIRD schema description: ${doc.filename}`]
      );
      inserted++;
    }

    res.json({ success: true, documents_inserted: inserted });
  } catch (err) {
    console.error('Seed context docs error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/schema/:appId/seed-sample-questions - upload sample questions as query patterns
router.post('/:appId/seed-sample-questions', async (req, res) => {
  try {
    const { appId } = req.params;
    const { patterns } = req.body;

    if (!patterns || !Array.isArray(patterns)) {
      return res.status(400).json({ error: 'patterns array required' });
    }

    let inserted = 0;
    for (const p of patterns) {
      const patternName = (p.question || '').substring(0, 100);
      const tablesUsed = p.tables || [];
      await query(
        `INSERT INTO query_patterns (app_id, pattern_name, nl_template, sql_template, tables_used, confidence, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT DO NOTHING`,
        [appId, patternName, p.question, p.sql || p.sql_template || null,
         tablesUsed.length > 0 ? `{${tablesUsed.join(',')}}` : null,
         p.confidence || 0.8, 'active']
      );
      inserted++;
    }

    res.json({ success: true, patterns_inserted: inserted });
  } catch (err) {
    console.error('Seed sample questions error:', err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/schema/:appId/import-bulk - bulk import tables, columns, and relationships
// Used by BIRD benchmark seeder to load schema metadata without needing the pipeline
router.post('/:appId/import-bulk', async (req, res) => {
  try {
    const { appId } = req.params;
    const { tables, relationships } = req.body;

    if (!tables || !Array.isArray(tables)) {
      return res.status(400).json({ error: 'tables array required' });
    }

    const results = { tables_created: 0, columns_created: 0, relationships_created: 0 };

    for (const table of tables) {
      // Insert table
      const tableResult = await query(
        `INSERT INTO app_tables (app_id, table_name, entity_name, row_count, description, enrichment_status, confidence_score)
         VALUES ($1, $2, $3, $4, $5, 'approved', 1.0)
         ON CONFLICT (app_id, table_name) DO UPDATE SET entity_name = EXCLUDED.entity_name, row_count = EXCLUDED.row_count
         RETURNING id`,
        [appId, table.table_name, table.entity_name || table.table_name, table.row_count || 0,
         table.description || `Table ${table.table_name}`]
      );
      const tableId = tableResult.rows[0].id;
      results.tables_created++;

      // Insert columns
      if (table.columns && Array.isArray(table.columns)) {
        for (const col of table.columns) {
          await query(
            `INSERT INTO app_columns (table_id, column_name, data_type, is_pk, is_fk, fk_reference, business_name, enrichment_status, confidence_score)
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'approved', 1.0)
             ON CONFLICT (table_id, column_name) DO NOTHING`,
            [tableId, col.column_name, col.data_type || 'TEXT', col.is_pk || false, col.is_fk || false,
             col.fk_reference || null, col.business_name || col.column_name]
          );
          results.columns_created++;
        }
      }
    }

    // Insert relationships
    if (relationships && Array.isArray(relationships)) {
      for (const rel of relationships) {
        // Resolve table names to IDs
        const fromTable = await query(
          'SELECT id FROM app_tables WHERE app_id = $1 AND table_name = $2', [appId, rel.from_table]);
        const toTable = await query(
          'SELECT id FROM app_tables WHERE app_id = $1 AND table_name = $2', [appId, rel.to_table]);

        if (fromTable.rows.length > 0 && toTable.rows.length > 0) {
          await query(
            `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality, enrichment_status, confidence_score)
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'approved', 1.0)
             ON CONFLICT DO NOTHING`,
            [appId, fromTable.rows[0].id, rel.from_column, toTable.rows[0].id, rel.to_column,
             rel.rel_type || 'inferred', rel.cardinality || 'many_to_one']
          );
          results.relationships_created++;
        }
      }
    }

    res.json({ success: true, ...results });
  } catch (err) {
    console.error('Bulk import error:', err);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
