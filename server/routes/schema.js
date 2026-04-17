const express = require('express');
const { query } = require('../db');

const router = express.Router();

// GET /api/schema/:appId/tables - list tables with column counts, enrichment status, entity metadata
router.get('/:appId/tables', async (req, res) => {
  try {
    const { appId } = req.params;

    const [rows] = await query(
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
      WHERE at.app_id = ?
      GROUP BY at.id, at.table_name, at.entity_name, at.row_count, at.description, at.entity_metadata,
               at.enrichment_status, at.confidence_score, at.enriched_by, at.enriched_at
      ORDER BY at.table_name`,
      [appId]
    );

    // Summary counts for entity curation
    const tables = rows;
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
      tableCondition = 'ac.table_id = ?';
      tableParam = parseInt(tableRef);
    } else {
      tableCondition = 'at.table_name = ?';
      tableParam = tableRef;
    }

    const [rows] = await query(
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
      WHERE at.app_id = ? AND ${tableCondition}
      ORDER BY ac.column_name`,
      [appId, tableParam]
    );

    // Parse value_mapping from TEXT to JSON for each column
    const columns = rows.map(row => {
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

    const [rows] = await query(
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
      WHERE ar.app_id = ?
      ORDER BY ft.table_name, ar.from_column`,
      [appId]
    );

    const relationships = rows;
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

    const [rows] = await query(
      `SELECT
        'table' as type,
        at.id,
        at.table_name as name,
        at.entity_name as display_name,
        NULL as parent_table
      FROM app_tables at
      WHERE at.app_id = ? AND (at.table_name LIKE ? OR at.entity_name LIKE ?)

      UNION ALL

      SELECT
        'column' as type,
        ac.id,
        ac.column_name as name,
        ac.business_name as display_name,
        at.table_name as parent_table
      FROM app_columns ac
      JOIN app_tables at ON ac.table_id = at.id
      WHERE at.app_id = ? AND (ac.column_name LIKE ? OR ac.business_name LIKE ? OR ac.description LIKE ?)

      ORDER BY type, name`,
      [appId, searchTerm, searchTerm, appId, searchTerm, searchTerm, searchTerm]
    );

    res.json({
      results: rows,
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
      filters += ` AND ac.enrichment_status = ?`;
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
      filters += ` AND (ac.column_name LIKE ? OR ac.business_name LIKE ? OR at.table_name LIKE ? OR ac.description LIKE ?)`;
    }

    // Get summary counts (always unfiltered by pagination, but respects tier/status/search)
    const [countRows] = await query(
      `SELECT
        COUNT(*) as total,
        SUM(CASE WHEN ac.confidence_score >= 90 THEN 1 ELSE 0 END) as auto_approve,
        SUM(CASE WHEN ac.confidence_score >= 60 AND ac.confidence_score < 90 THEN 1 ELSE 0 END) as review,
        SUM(CASE WHEN ac.confidence_score < 60 THEN 1 ELSE 0 END) as manual,
        SUM(CASE WHEN ac.enrichment_status = 'approved' THEN 1 ELSE 0 END) as approved,
        SUM(CASE WHEN ac.enrichment_status = 'rejected' THEN 1 ELSE 0 END) as rejected,
        SUM(CASE WHEN ac.enrichment_status = 'ai_enriched' THEN 1 ELSE 0 END) as ai_enriched,
        SUM(CASE WHEN ac.enrichment_status = 'draft' THEN 1 ELSE 0 END) as draft
      FROM app_columns ac
      JOIN app_tables at ON ac.table_id = at.id
      WHERE at.app_id = ?`,
      [appId]
    );

    // Get filtered count for pagination
    const [filteredCountRows] = await query(
      `SELECT COUNT(*) as filtered_total
       FROM app_columns ac JOIN app_tables at ON ac.table_id = at.id
       WHERE at.app_id = ?${filters}`,
      params
    );

    // Get paginated results
    const dataParams = [...params, limitNum, offset];
    const [rows] = await query(
      `SELECT
        ac.id, ac.column_name, ac.data_type, ac.is_pk, ac.is_fk, ac.fk_reference,
        ac.business_name, ac.description, ac.value_mapping,
        ac.enrichment_status, ac.confidence_score, ac.enriched_by, ac.enriched_at,
        ac.table_id, at.table_name, at.entity_name, at.entity_metadata
      FROM app_columns ac
      JOIN app_tables at ON ac.table_id = at.id
      WHERE at.app_id = ?${filters}
      ORDER BY ac.confidence_score ASC, at.table_name, ac.column_name
      LIMIT ? OFFSET ?`,
      dataParams
    );

    const columns = rows.map(row => {
      let parsedMapping = null;
      if (row.value_mapping) {
        try { parsedMapping = JSON.parse(row.value_mapping); } catch (e) { parsedMapping = row.value_mapping; }
      }
      return { ...row, value_mapping: parsedMapping };
    });

    const summary = countRows[0] || {};
    const filteredTotal = parseInt(filteredCountRows[0]?.filtered_total || 0);

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

    if (business_name !== undefined) { sets.push(`business_name = ?`); params.push(business_name); }
    if (description !== undefined) { sets.push(`description = ?`); params.push(description); }
    if (enrichment_status !== undefined) {
      sets.push(`enrichment_status = ?`); params.push(enrichment_status);
      sets.push(`enriched_by = ?`); params.push('human');
      sets.push(`enriched_at = NOW()`);
    }
    if (value_mapping !== undefined) {
      sets.push(`value_mapping = ?`);
      params.push(typeof value_mapping === 'string' ? value_mapping : JSON.stringify(value_mapping));
    }

    if (sets.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    // Verify column belongs to this app
    params.push(columnId);
    params.push(appId);
    const [updateResult] = await query(
      `UPDATE app_columns ac
       JOIN app_tables at ON ac.table_id = at.id
       SET ${sets.join(', ')}
       WHERE ac.id = ? AND at.app_id = ?`,
      params
    );

    if (updateResult.affectedRows === 0) {
      return res.status(404).json({ error: 'Column not found' });
    }

    const [updatedColRows] = await query('SELECT * FROM app_columns WHERE id = ?', [columnId]);
    res.json({ column: updatedColRows[0] });
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

    let approveResult;
    if (column_ids && column_ids.length > 0) {
      // Approve specific columns
      const placeholders = column_ids.map(() => '?').join(',');
      [approveResult] = await query(
        `UPDATE app_columns ac
         JOIN app_tables at ON ac.table_id = at.id
         SET ac.enrichment_status = 'approved', ac.enriched_by = 'human', ac.enriched_at = NOW()
         WHERE at.app_id = ?
           AND ac.id IN (${placeholders})
           AND ac.enrichment_status != 'approved'`,
        [appId, ...column_ids]
      );
    } else {
      // Approve all above confidence threshold
      [approveResult] = await query(
        `UPDATE app_columns ac
         JOIN app_tables at ON ac.table_id = at.id
         SET ac.enrichment_status = 'approved', ac.enriched_by = 'human', ac.enriched_at = NOW()
         WHERE at.app_id = ?
           AND ac.confidence_score >= ?
           AND ac.enrichment_status IN ('ai_enriched', 'needs_review')`,
        [appId, min_confidence]
      );
    }

    res.json({ approved_count: approveResult.affectedRows });
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

    if (entity_name !== undefined) { sets.push(`entity_name = ?`); params.push(entity_name); }
    if (description !== undefined) { sets.push(`description = ?`); params.push(description); }
    if (entity_metadata !== undefined) {
      sets.push(`entity_metadata = ?`);
      params.push(typeof entity_metadata === 'string' ? entity_metadata : JSON.stringify(entity_metadata));
    }
    if (enrichment_status !== undefined) {
      sets.push(`enrichment_status = ?`); params.push(enrichment_status);
      sets.push(`enriched_by = ?`); params.push('human');
      sets.push(`enriched_at = NOW()`);
    }

    if (sets.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    params.push(tableId);
    params.push(appId);
    const [tableUpdateResult] = await query(
      `UPDATE app_tables SET ${sets.join(', ')} WHERE id = ? AND app_id = ?`,
      params
    );

    if (tableUpdateResult.affectedRows === 0) {
      return res.status(404).json({ error: 'Table not found' });
    }

    const [updatedTableRows] = await query('SELECT * FROM app_tables WHERE id = ?', [tableId]);
    res.json({ table: updatedTableRows[0] });
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

    let tableApproveResult;
    if (table_ids && table_ids.length > 0) {
      const placeholders = table_ids.map(() => '?').join(',');
      [tableApproveResult] = await query(
        `UPDATE app_tables SET enrichment_status = 'approved', enriched_by = 'human', enriched_at = NOW()
         WHERE app_id = ? AND id IN (${placeholders}) AND enrichment_status != 'approved'`,
        [appId, ...table_ids]
      );
    } else {
      [tableApproveResult] = await query(
        `UPDATE app_tables SET enrichment_status = 'approved', enriched_by = 'human', enriched_at = NOW()
         WHERE app_id = ? AND enrichment_status IN ('draft', 'ai_enriched', 'needs_review')`,
        [appId]
      );
    }

    res.json({ approved_count: tableApproveResult.affectedRows });
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

    if (rel_type !== undefined) { sets.push(`rel_type = ?`); params.push(rel_type); }
    if (cardinality !== undefined) { sets.push(`cardinality = ?`); params.push(cardinality); }
    if (enrichment_status !== undefined) {
      sets.push(`enrichment_status = ?`); params.push(enrichment_status);
      sets.push(`enriched_by = ?`); params.push('human');
      sets.push(`enriched_at = NOW()`);
    }

    if (sets.length === 0) {
      return res.status(400).json({ error: 'No fields to update' });
    }

    params.push(relId);
    params.push(appId);
    const [relUpdateResult] = await query(
      `UPDATE app_relationships SET ${sets.join(', ')} WHERE id = ? AND app_id = ?`,
      params
    );

    if (relUpdateResult.affectedRows === 0) {
      return res.status(404).json({ error: 'Relationship not found' });
    }

    const [updatedRelRows] = await query('SELECT * FROM app_relationships WHERE id = ?', [relId]);
    res.json({ relationship: updatedRelRows[0] });
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
    const [fromTableRows] = await query('SELECT id FROM app_tables WHERE app_id = ? AND table_name = ?', [appId, from_table_name]);
    const [toTableRows] = await query('SELECT id FROM app_tables WHERE app_id = ? AND table_name = ?', [appId, to_table_name]);

    if (fromTableRows.length === 0) return res.status(404).json({ error: `Table '${from_table_name}' not found` });
    if (toTableRows.length === 0) return res.status(404).json({ error: `Table '${to_table_name}' not found` });

    // Check if relationship already exists
    const [existingRelRows] = await query(
      'SELECT id FROM app_relationships WHERE from_table_id = ? AND from_column = ? AND to_table_id = ? AND to_column = ?',
      [fromTableRows[0].id, from_column, toTableRows[0].id, to_column]
    );
    if (existingRelRows.length > 0) {
      return res.status(409).json({ error: 'Relationship already exists' });
    }

    const [relInsertResult] = await query(
      `INSERT INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality, enrichment_status, enriched_by, enriched_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, 'approved', 'human', NOW())`,
      [appId, fromTableRows[0].id, from_column, toTableRows[0].id, to_column, rel_type, cardinality || null]
    );

    const [newRelRows] = await query('SELECT * FROM app_relationships WHERE id = ?', [relInsertResult.insertId]);
    res.json({ relationship: newRelRows[0] });
  } catch (err) {
    console.error('Create relationship error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// DELETE /api/schema/:appId/relationships/:relId - delete a relationship
router.delete('/:appId/relationships/:relId', async (req, res) => {
  try {
    const { appId, relId } = req.params;
    const [delRelResult] = await query(
      'DELETE FROM app_relationships WHERE id = ? AND app_id = ?',
      [relId, appId]
    );

    if (delRelResult.affectedRows === 0) {
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

    let relApproveResult;
    if (rel_ids && rel_ids.length > 0) {
      const placeholders = rel_ids.map(() => '?').join(',');
      [relApproveResult] = await query(
        `UPDATE app_relationships SET enrichment_status = 'approved', enriched_by = 'human', enriched_at = NOW()
         WHERE app_id = ? AND id IN (${placeholders}) AND enrichment_status != 'approved'`,
        [appId, ...rel_ids]
      );
    } else {
      [relApproveResult] = await query(
        `UPDATE app_relationships SET enrichment_status = 'approved', enriched_by = 'human', enriched_at = NOW()
         WHERE app_id = ? AND enrichment_status IN ('ai_enriched', 'draft', 'needs_review')`,
        [appId]
      );
    }

    res.json({ approved_count: relApproveResult.affectedRows });
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
      // First find the table id
      const [tableIdRows] = await query(
        'SELECT id FROM app_tables WHERE app_id = ? AND table_name = ?',
        [appId, table.table_name]
      );
      if (tableIdRows.length === 0) continue;
      const tableId = tableIdRows[0].id;

      // Update table enrichment
      await query(
        `UPDATE app_tables SET
          description = COALESCE(?, description),
          entity_metadata = COALESCE(?, entity_metadata),
          enrichment_status = 'ai_enriched',
          enriched_by = 'ai',
          enriched_at = NOW()
        WHERE id = ?`,
        [table.description || null,
         table.entity_metadata ? JSON.stringify(table.entity_metadata) : null,
         tableId]
      );

      results.tables_updated++;

      // Also update entity_name if provided
      if (table.entity_name) {
        await query('UPDATE app_tables SET entity_name = ? WHERE id = ?', [table.entity_name, tableId]);
      }

      // Update column enrichments
      if (table.columns && Array.isArray(table.columns)) {
        for (const col of table.columns) {
          const status = (col.confidence_score || 0) >= 70 ? 'ai_enriched' : 'needs_review';
          await query(
            `UPDATE app_columns SET
              business_name = COALESCE(?, business_name),
              description = COALESCE(?, description),
              confidence_score = COALESCE(?, confidence_score),
              enrichment_status = ?,
              enriched_by = 'ai',
              enriched_at = NOW(),
              value_mapping = COALESCE(?, value_mapping)
            WHERE table_id = ? AND column_name = ?`,
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
      const [tableRows] = await query(
        'SELECT id FROM app_tables WHERE app_id = ? AND table_name = ?', [appId, table.table_name]);
      if (tableRows.length === 0) continue;
      const tableId = tableRows[0].id;

      // Update each column's value_mapping with the profiled value dictionary
      for (const [colName, valueDict] of Object.entries(table.value_dictionaries)) {
        if (Object.keys(valueDict).length > 0) {
          await query(
            `UPDATE app_columns SET value_mapping = ? WHERE table_id = ? AND column_name = ?`,
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
        `INSERT IGNORE INTO context_documents (app_id, filename, file_type, file_size, extracted_text, description, uploaded_at)
         VALUES (?, ?, ?, ?, ?, ?, NOW())`,
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
        `INSERT IGNORE INTO query_patterns (app_id, pattern_name, nl_template, sql_template, tables_used, confidence, status)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [appId, patternName, p.question, p.sql || p.sql_template || null,
         tablesUsed.length > 0 ? JSON.stringify(tablesUsed) : null,
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
      // Insert table (INSERT ... ON DUPLICATE KEY UPDATE for MySQL)
      await query(
        `INSERT INTO app_tables (app_id, table_name, entity_name, row_count, description, enrichment_status, confidence_score)
         VALUES (?, ?, ?, ?, ?, 'approved', 1.0)
         ON DUPLICATE KEY UPDATE entity_name = VALUES(entity_name), row_count = VALUES(row_count)`,
        [appId, table.table_name, table.entity_name || table.table_name, table.row_count || 0,
         table.description || `Table ${table.table_name}`]
      );
      const [importedTableRows] = await query(
        'SELECT id FROM app_tables WHERE app_id = ? AND table_name = ?',
        [appId, table.table_name]
      );
      const tableId = importedTableRows[0].id;
      results.tables_created++;

      // Insert columns
      if (table.columns && Array.isArray(table.columns)) {
        for (const col of table.columns) {
          await query(
            `INSERT IGNORE INTO app_columns (table_id, column_name, data_type, is_pk, is_fk, fk_reference, business_name, enrichment_status, confidence_score)
             VALUES (?, ?, ?, ?, ?, ?, ?, 'approved', 1.0)`,
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
        const [fromRelTableRows] = await query(
          'SELECT id FROM app_tables WHERE app_id = ? AND table_name = ?', [appId, rel.from_table]);
        const [toRelTableRows] = await query(
          'SELECT id FROM app_tables WHERE app_id = ? AND table_name = ?', [appId, rel.to_table]);

        if (fromRelTableRows.length > 0 && toRelTableRows.length > 0) {
          await query(
            `INSERT IGNORE INTO app_relationships (app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality, enrichment_status, confidence_score)
             VALUES (?, ?, ?, ?, ?, ?, ?, 'approved', 1.0)`,
            [appId, fromRelTableRows[0].id, rel.from_column, toRelTableRows[0].id, rel.to_column,
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
