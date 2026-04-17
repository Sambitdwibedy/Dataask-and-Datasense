/**
 * Database Initialization Script for Data Ask (MySQL)
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
  console.log(`Database: ${process.env.DB_HOST}/${process.env.DB_NAME}`);

  try {
    // Read schema and execute statements one by one (mysql2 does not support multi-statement by default)
    const schemaPath = path.join(__dirname, 'schema.sql');
    const schemaSql = fs.readFileSync(schemaPath, 'utf-8');

    // Split on semicolons and execute each statement
    const statements = schemaSql
      .split(';')
      .map(s => s.trim())
      .filter(s => s.length > 0 && !s.startsWith('--'));

    for (const stmt of statements) {
      try {
        await query(stmt);
      } catch (err) {
        // Warn on non-critical errors (e.g. column already exists)
        if (!err.message.includes('Duplicate column') && !err.message.includes('already exists')) {
          console.warn(`  ⚠ Statement warning: ${err.message.substring(0, 120)}`);
        }
      }
    }
    console.log('✓ Schema applied successfully');

    // Verify tables
    const tables = ['doc_collections', 'doc_sources', 'doc_chunks', 'ida_conversations'];
    for (const table of tables) {
      const [rows] = await query(`SELECT COUNT(*) as cnt FROM ${table}`);
      console.log(`  ${table}: ${rows[0].cnt} rows`);
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
