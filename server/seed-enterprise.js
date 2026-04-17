#!/usr/bin/env node

/**
 * BOKG Builder - Enterprise App Seed Script
 * Creates Oracle EBS (OEBS) and SAP ECC application entries.
 *
 * These apps use SQLite test databases with synthetic data.
 * Run the pipeline after seeding to load data, profile, and enrich.
 *
 * Usage: node seed-enterprise.js
 */

const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function createApp(name, type, description, config) {
  const result = await pool.query(
    `INSERT INTO applications (name, type, description, status, config)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (name) DO UPDATE SET
       type = $2, description = $3, config = $5, updated_at = NOW()
     RETURNING id`,
    [name, type, description, 'ingesting', JSON.stringify(config)]
  );
  return result.rows[0].id;
}

async function main() {
  console.log('=== BOKG Builder — Enterprise App Seed ===\n');

  // Oracle E-Business Suite
  const oebsId = await createApp(
    'Oracle E-Business Suite R12',
    'Oracle EBS',
    'Oracle E-Business Suite R12 — 11 modules (AP, AR, GL, PO, OM, INV, HR, PAY, BEN, CE, FND) with 123 tables, 1,924 columns, and 58,249 rows of synthetic demo data covering Procure-to-Pay, Order-to-Cash, and Record-to-Report business processes. Includes FND_ AOL metadata tables (FND_TABLES, FND_COLUMNS, FND_PRIMARY_KEYS, FND_FOREIGN_KEYS) for BOKG enrichment.',
    {
      sourceType: 'oracle_ebs',
      version: 'R12.2',
      loadedAt: new Date().toISOString(),
      modules: ['AP', 'AR', 'GL', 'PO', 'OM', 'INV', 'HR', 'PAY', 'BEN', 'CE', 'FND'],
      tableCount: 123,
      columnCount: 1924,
      pipeline: {
        parallel_concurrency: 4,
        batch_column_threshold: 8,
        batch_max_columns: 30,
        sample_row_count: 10,
      },
      query_engine: {
        model: 'claude-sonnet-4-20250514',
        show_token_cost: true,
        show_sql_details: true,
        schema_link_threshold: 15,
        column_link_threshold: 20,
        auto_seed_qpd: false,  // Import pre-built QPD from prototype instead
        max_seed_questions_per_entity: 3,
      }
    }
  );
  console.log(`✓ Oracle EBS R12 created (id: ${oebsId})`);

  // SAP ECC
  const sapId = await createApp(
    'SAP ECC 6.0',
    'SAP ECC',
    'SAP ECC 6.0 — 10 modules (FI-GL, FI-AP, FI-AR, CO, SD, MM, HR, PAY, BEN, PM) with 124 tables and 3,747 rows of synthetic demo data covering core Finance, Sales & Distribution, Materials Management, and Human Resources.',
    {
      sourceType: 'sap_ecc',
      version: '6.0 EhP8',
      loadedAt: new Date().toISOString(),
      modules: ['FI_GL', 'FI_AP', 'FI_AR', 'CO', 'SD', 'MM', 'HR', 'PAY', 'BEN', 'PM'],
      tableCount: 124,
      columnCount: 699,
      pipeline: {
        parallel_concurrency: 4,
        batch_column_threshold: 8,
        batch_max_columns: 30,
        sample_row_count: 10,
      },
      query_engine: {
        model: 'claude-sonnet-4-20250514',
        show_token_cost: true,
        show_sql_details: true,
        schema_link_threshold: 15,
        column_link_threshold: 20,
        auto_seed_qpd: false,
        max_seed_questions_per_entity: 3,
      }
    }
  );
  console.log(`✓ SAP ECC 6.0 created (id: ${sapId})`);

  console.log('\nDone. Run the pipeline for each app to load data and enrich.');
  await pool.end();
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
