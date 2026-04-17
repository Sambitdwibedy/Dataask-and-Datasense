// Database connection — shared with BOKG Builder (same DATABASE_URL)
const { Pool } = require('pg');
require('dotenv').config();

// Determine SSL config based on the DATABASE_URL host.
// Vercel Postgres, Neon, Supabase, Railway all require SSL but each has
// slightly different TLS requirements. Setting rejectUnauthorized: false is
// safe for managed cloud Postgres providers (certs are managed by the provider).
function sslConfig(url) {
  if (!url) return false;
  // Local/Docker connections — no SSL needed
  if (url.includes('localhost') || url.includes('127.0.0.1')) return false;
  // All remote managed providers (Railway, Neon, Supabase, Vercel Postgres, etc.)
  return { rejectUnauthorized: false };
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  ssl: sslConfig(process.env.DATABASE_URL),
});

const query = (text, params) => pool.query(text, params);

module.exports = { pool, query };
