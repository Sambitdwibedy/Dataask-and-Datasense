const express = require('express');
const jwt = require('jsonwebtoken');
const bcryptjs = require('bcryptjs');
const { query } = require('../db');
const { requireAuth } = require('../middleware/auth');

const router = express.Router();

// POST /api/auth/login
router.post('/login', async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password required' });
    }

    const [rows] = await query('SELECT id, email, password_hash, name, role FROM users WHERE email = ?', [email]);

    if (rows.length === 0) {
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    const user = rows[0];
    const passwordMatch = await bcryptjs.compare(password, user.password_hash);

    if (!passwordMatch) {
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    const token = jwt.sign(
      {
        id: user.id,
        email: user.email,
        name: user.name,
        role: user.role,
      },
      process.env.JWT_SECRET,
      { expiresIn: process.env.JWT_EXPIRY || '24h' }
    );

    // Load user's workspace assignments (maps to CDP KB assignments)
    let workspaces = [];
    let defaultWorkspace = null;
    try {
      const [wsRows] = await query(`
        SELECT w.id, w.name, w.app_id, w.description, w.is_default,
               wm.role as member_role, wm.is_default as member_default,
               a.name as app_name, a.type as app_type
        FROM workspaces w
        JOIN workspace_members wm ON w.id = wm.workspace_id
        LEFT JOIN applications a ON w.app_id = a.id
        WHERE wm.user_id = ?
          AND wm.enabled = TRUE
          AND w.status = 'active'
          AND (wm.start_date IS NULL OR wm.start_date <= CURRENT_DATE)
          AND (wm.end_date IS NULL OR wm.end_date >= CURRENT_DATE)
        ORDER BY w.is_default DESC, wm.is_default DESC, w.name ASC
      `, [user.id]);
      workspaces = wsRows;
      defaultWorkspace = workspaces.find(ws => ws.is_default) || workspaces[0] || null;
    } catch (wsErr) {
      // Workspaces table may not exist yet — graceful fallback
      console.warn('Workspace lookup skipped (table may not exist yet):', wsErr.message);
    }

    res.json({
      token,
      user: {
        id: user.id,
        email: user.email,
        name: user.name,
        role: user.role,
      },
      workspaces,
      defaultWorkspace,
    });
  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// POST /api/auth/register
router.post('/register', async (req, res) => {
  try {
    const { email, password, name } = req.body;

    if (!email || !password || !name) {
      return res.status(400).json({ error: 'Email, password, and name required' });
    }

    // Check if user already exists
    const [existingRows] = await query('SELECT id FROM users WHERE email = ?', [email]);
    if (existingRows.length > 0) {
      return res.status(409).json({ error: 'User already exists' });
    }

    const passwordHash = await bcryptjs.hash(password, 10);

    const [result] = await query(
      'INSERT INTO users (email, password_hash, name, role, created_at) VALUES (?, ?, ?, ?, NOW())',
      [email, passwordHash, name, 'user']
    );

    const [newUserRows] = await query('SELECT id, email, name, role FROM users WHERE id = ?', [result.insertId]);
    const user = newUserRows[0];

    const token = jwt.sign(
      {
        id: user.id,
        email: user.email,
        name: user.name,
        role: user.role,
      },
      process.env.JWT_SECRET,
      { expiresIn: process.env.JWT_EXPIRY || '24h' }
    );

    res.status(201).json({
      token,
      user: {
        id: user.id,
        email: user.email,
        name: user.name,
        role: user.role,
      },
    });
  } catch (err) {
    console.error('Register error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/auth/me
router.get('/me', requireAuth, async (req, res) => {
  try {
    const [rows] = await query('SELECT id, email, name, role FROM users WHERE id = ?', [req.user.id]);

    if (rows.length === 0) {
      return res.status(404).json({ error: 'User not found' });
    }

    // Load workspace assignments
    let workspaces = [];
    let defaultWorkspace = null;
    try {
      const [wsRows] = await query(`
        SELECT w.id, w.name, w.app_id, w.description, w.is_default,
               wm.role as member_role, wm.is_default as member_default,
               a.name as app_name, a.type as app_type
        FROM workspaces w
        JOIN workspace_members wm ON w.id = wm.workspace_id
        LEFT JOIN applications a ON w.app_id = a.id
        WHERE wm.user_id = ?
          AND wm.enabled = TRUE
          AND w.status = 'active'
          AND (wm.start_date IS NULL OR wm.start_date <= CURRENT_DATE)
          AND (wm.end_date IS NULL OR wm.end_date >= CURRENT_DATE)
        ORDER BY w.is_default DESC, wm.is_default DESC, w.name ASC
      `, [req.user.id]);
      workspaces = wsRows;
      defaultWorkspace = workspaces.find(ws => ws.is_default) || workspaces[0] || null;
    } catch (wsErr) {
      console.warn('Workspace lookup skipped:', wsErr.message);
    }

    res.json({
      user: rows[0],
      workspaces,
      defaultWorkspace,
    });
  } catch (err) {
    console.error('Get user error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
