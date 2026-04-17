/**
 * Workspace Routes — Maps to CDP Knowledge Bases (KBs)
 *
 * Workspaces group structured data (via app_id) and document collections
 * into a single access-controlled scope. Users are assigned to workspaces
 * via workspace_members (maps to CDP KB Assignment).
 *
 * GET    /api/workspaces           — List workspaces for current user
 * GET    /api/workspaces/:id       — Get workspace detail
 * POST   /api/workspaces           — Create workspace (admin)
 * PUT    /api/workspaces/:id       — Update workspace (admin)
 * DELETE /api/workspaces/:id       — Delete workspace (admin)
 * POST   /api/workspaces/:id/members — Add member to workspace
 * DELETE /api/workspaces/:id/members/:userId — Remove member
 * POST   /api/workspaces/auto-create — Create workspaces from existing apps (migration)
 */
const express = require('express');
const { query } = require('../db');

const router = express.Router();

// ─── List workspaces for current user ───
router.get('/', async (req, res) => {
  try {
    const userId = req.user?.id;
    if (!userId) return res.status(401).json({ error: 'Authentication required' });

    const [rows] = await query(`
      SELECT
        w.id, w.name, w.description, w.app_id, w.is_default, w.status, w.created_at,
        wm.role as member_role, wm.is_default as member_default,
        a.name as app_name, a.type as app_type, a.status as app_status,
        (SELECT COUNT(*) FROM doc_collections dc WHERE dc.workspace_id = w.id) as doc_collection_count,
        (SELECT COUNT(*) FROM doc_sources ds
         JOIN doc_collections dc2 ON ds.collection_id = dc2.id
         WHERE dc2.workspace_id = w.id AND ds.status = 'ready') as doc_count,
        (SELECT COUNT(*) FROM app_tables at2 WHERE at2.app_id = w.app_id) as table_count
      FROM workspaces w
      JOIN workspace_members wm ON w.id = wm.workspace_id
      LEFT JOIN applications a ON w.app_id = a.id
      WHERE wm.user_id = ?
        AND wm.enabled = TRUE
        AND (wm.start_date IS NULL OR wm.start_date <= CURRENT_DATE)
        AND (wm.end_date IS NULL OR wm.end_date >= CURRENT_DATE)
        AND w.status = 'active'
      ORDER BY w.is_default DESC, wm.is_default DESC, w.name ASC
    `, [userId]);

    res.json({ workspaces: rows });
  } catch (err) {
    console.error('List workspaces error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ─── Get workspace detail ───
router.get('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const userId = req.user?.id;

    // Verify membership
    const [memberRows] = await query(
      'SELECT role FROM workspace_members WHERE workspace_id = ? AND user_id = ? AND enabled = TRUE',
      [id, userId]
    );
    if (memberRows.length === 0) {
      return res.status(403).json({ error: 'Not a member of this workspace' });
    }

    const [wsRows] = await query(`
      SELECT w.*, a.name as app_name, a.type as app_type, a.status as app_status
      FROM workspaces w
      LEFT JOIN applications a ON w.app_id = a.id
      WHERE w.id = ?
    `, [id]);

    if (wsRows.length === 0) {
      return res.status(404).json({ error: 'Workspace not found' });
    }

    // Get collections in this workspace
    const [collectionsRows] = await query(
      `SELECT id, name, description, doc_count, chunk_count, status
       FROM doc_collections WHERE workspace_id = ? ORDER BY name`,
      [id]
    );

    // Get members
    const [membersRows] = await query(
      `SELECT wm.user_id, wm.role, wm.is_default, wm.enabled, u.name, u.email
       FROM workspace_members wm
       JOIN users u ON wm.user_id = u.id
       WHERE wm.workspace_id = ?
       ORDER BY u.name`,
      [id]
    );

    res.json({
      workspace: wsRows[0],
      collections: collectionsRows,
      members: membersRows,
      userRole: memberRows[0].role,
    });
  } catch (err) {
    console.error('Get workspace error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ─── Create workspace ───
router.post('/', async (req, res) => {
  try {
    const { name, description, appId } = req.body;
    const userId = req.user?.id;

    if (!name) return res.status(400).json({ error: 'Workspace name required' });

    const [result] = await query(
      `INSERT INTO workspaces (name, description, app_id, created_by)
       VALUES (?, ?, ?, ?)`,
      [name, description || null, appId || null, userId]
    );

    const workspaceId = result.insertId;

    // Auto-add creator as admin member
    await query(
      `INSERT INTO workspace_members (workspace_id, user_id, role, is_default)
       VALUES (?, ?, 'admin', TRUE)`,
      [workspaceId, userId]
    );

    const [wsRows] = await query('SELECT * FROM workspaces WHERE id = ?', [workspaceId]);
    res.status(201).json({ workspace: wsRows[0] });
  } catch (err) {
    console.error('Create workspace error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ─── Update workspace ───
router.put('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { name, description, appId, status, isDefault } = req.body;

    const updates = [];
    const params = [];

    if (name !== undefined) { updates.push(`name = ?`); params.push(name); }
    if (description !== undefined) { updates.push(`description = ?`); params.push(description); }
    if (appId !== undefined) { updates.push(`app_id = ?`); params.push(appId); }
    if (status !== undefined) { updates.push(`status = ?`); params.push(status); }
    if (isDefault !== undefined) { updates.push(`is_default = ?`); params.push(isDefault); }
    updates.push(`updated_at = NOW()`);
    params.push(id);

    const [result] = await query(
      `UPDATE workspaces SET ${updates.join(', ')} WHERE id = ?`,
      params
    );

    if (result.affectedRows === 0) return res.status(404).json({ error: 'Workspace not found' });

    const [wsRows] = await query('SELECT * FROM workspaces WHERE id = ?', [id]);
    res.json({ workspace: wsRows[0] });
  } catch (err) {
    console.error('Update workspace error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ─── Delete workspace ───
router.delete('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const [wsRows] = await query('SELECT * FROM workspaces WHERE id = ?', [id]);
    if (wsRows.length === 0) return res.status(404).json({ error: 'Workspace not found' });
    await query('DELETE FROM workspaces WHERE id = ?', [id]);
    res.json({ message: 'Workspace deleted', workspace: wsRows[0] });
  } catch (err) {
    console.error('Delete workspace error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ─── Add member to workspace (maps to CDP "Assign Ingest/Access user to KB") ───
router.post('/:id/members', async (req, res) => {
  try {
    const { id } = req.params;
    const { userId, role, isDefault, startDate, endDate } = req.body;

    if (!userId) return res.status(400).json({ error: 'userId required' });

    await query(
      `INSERT INTO workspace_members (workspace_id, user_id, role, is_default, start_date, end_date)
       VALUES (?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         role = VALUES(role), is_default = VALUES(is_default),
         start_date = VALUES(start_date), end_date = VALUES(end_date), enabled = TRUE`,
      [id, userId, role || 'reader', isDefault || false, startDate || null, endDate || null]
    );

    const [memberRows] = await query(
      'SELECT * FROM workspace_members WHERE workspace_id = ? AND user_id = ?',
      [id, userId]
    );
    res.status(201).json({ member: memberRows[0] });
  } catch (err) {
    console.error('Add member error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ─── Remove member from workspace ───
router.delete('/:id/members/:userId', async (req, res) => {
  try {
    const { id, userId } = req.params;
    await query('DELETE FROM workspace_members WHERE workspace_id = ? AND user_id = ?', [id, userId]);
    res.json({ message: 'Member removed' });
  } catch (err) {
    console.error('Remove member error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ─── Auto-create workspaces from existing applications (migration helper) ───
// This creates a workspace for each existing application and assigns all users
router.post('/auto-create', async (req, res) => {
  try {
    const [apps] = await query('SELECT id, name, type, description FROM applications');
    const [users] = await query('SELECT id FROM users');
    const created = [];

    for (const app of apps) {
      // Check if workspace already exists for this app
      const [existing] = await query('SELECT id FROM workspaces WHERE app_id = ?', [app.id]);
      if (existing.length > 0) {
        created.push({ appId: app.id, workspaceId: existing[0].id, status: 'already_exists' });
        continue;
      }

      // Create workspace (first one is default)
      const isFirst = created.length === 0;
      const [wsResult] = await query(
        `INSERT INTO workspaces (name, description, app_id, is_default)
         VALUES (?, ?, ?, ?)`,
        [app.name, app.description || `Workspace for ${app.name} (${app.type})`, app.id, isFirst]
      );
      const wsId = wsResult.insertId;

      // Assign all users to this workspace
      for (const user of users) {
        try {
          await query(
            `INSERT INTO workspace_members (workspace_id, user_id, role, is_default)
             VALUES (?, ?, 'reader', TRUE)
             ON DUPLICATE KEY UPDATE enabled = TRUE`,
            [wsId, user.id]
          );
        } catch (e) { /* skip duplicates */ }
      }

      // Link existing doc_collections for this app to the workspace
      await query(
        'UPDATE doc_collections SET workspace_id = ? WHERE app_id = ? AND workspace_id IS NULL',
        [wsId, app.id]
      );

      // Link existing doc_chunks for this app to the workspace
      await query(
        'UPDATE doc_chunks SET workspace_id = ? WHERE app_id = ? AND workspace_id IS NULL',
        [wsId, app.id]
      );

      created.push({ appId: app.id, workspaceId: wsId, name: app.name, status: 'created' });
    }

    res.json({ message: `Processed ${created.length} applications`, workspaces: created });
  } catch (err) {
    console.error('Auto-create workspaces error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
