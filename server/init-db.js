/**
 * Database Initialization Script for Data Ask
 *
 * Run manually: node init-db.js
 * Creates the Data Ask tables (additive to BOKG Builder schema).
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { pool, query } = require('./db');

async function initDb() {
  console.log('Initializing Data Ask database tables...');
  console.log(`Database: ${process.env.DATABASE_URL?.replace(/:[^@]+@/, ':***@')}`);

  try {
    // Read schema
    const schemaPath = path.join(__dirname, 'schema.sql');
    const schemaSql = fs.readFileSync(schemaPath, 'utf-8');

    // Execute full schema (handles IF NOT EXISTS gracefully)
    await query(schemaSql);
    console.log('✓ Schema applied successfully');

    // Verify tables
    const tables = ['doc_collections', 'doc_sources', 'doc_chunks', 'ida_conversations'];
    for (const table of tables) {
      const result = await query(`SELECT COUNT(*) FROM ${table}`);
      console.log(`  ${table}: ${result.rows[0].count} rows`);
    }

    // Check vector extension
    const extResult = await query("SELECT extversion FROM pg_extension WHERE extname = 'vector'");
    if (extResult.rows.length > 0) {
      console.log(`✓ pgvector extension: v${extResult.rows[0].extversion}`);
    } else {
      console.warn('⚠ pgvector extension not found — vector search will not work');
    }

    console.log('\nDone! Data Ask tables are ready.');
  } catch (err) {
    console.error('Init failed:', err.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

initDb();
