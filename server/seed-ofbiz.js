#!/usr/bin/env node

/**
 * BOKG Builder - OFBiz Seed Script
 * Loads Apache OFBiz 18.12 schema into PostgreSQL
 *
 * Usage: node seed-ofbiz.js [--reset]
 * Options:
 *   --reset   Drop all existing data before seeding (careful!)
 */

const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// Import constants
const {
  ENRICHMENT_STATUSES,
  PIPELINE_STAGES,
  OFBIZ_FIELD_PATTERNS
} = require('../shared/constants');

// Global statistics
const stats = {
  users: 0,
  applications: 0,
  modules: 0,
  tables: 0,
  columns: 0,
  relationships: 0,
  pipelineRuns: 0,
  queryPatterns: 0,
  testQueries: 0
};

// CamelCase to Title Case converter
function camelCaseToTitleCase(str) {
  if (!str) return '';

  // Insert space before uppercase letters (except at the start)
  return str
    .replace(/([A-Z])/g, ' $1')
    .replace(/^./, (char) => char.toUpperCase())
    .trim();
}

// Generate business name for a column based on name, table context, and data type
function generateBusinessName(columnName, tableName, dataType, ofbizType = '') {
  // Check for OFBiz-specific patterns
  for (const [pattern, config] of Object.entries(OFBIZ_FIELD_PATTERNS)) {
    if (columnName.endsWith(pattern)) {
      if (config.display) {
        return config.display;
      }
      const baseName = columnName.substring(0, columnName.length - pattern.length);
      return camelCaseToTitleCase(baseName) + config.suffix;
    }
  }

  // Handle generic patterns
  if (columnName.includes('Date') || columnName.includes('Time')) {
    return camelCaseToTitleCase(columnName);
  }

  if (dataType && (dataType.includes('NUMERIC') || dataType.includes('DECIMAL') ||
                   dataType.includes('FLOAT') || dataType.includes('DOUBLE'))) {
    return camelCaseToTitleCase(columnName) + ' Amount';
  }

  if (dataType && dataType.includes('BOOLEAN')) {
    return camelCaseToTitleCase(columnName) + ' Flag';
  }

  // Default: just convert camelCase to Title Case
  return camelCaseToTitleCase(columnName);
}

// Determine confidence score based on how "obvious" the column name is
function calculateConfidenceScore(columnName, dataType = '') {
  // High confidence patterns
  if (/^(id|Id|ID)$/.test(columnName)) return 95;
  if (columnName.endsWith('Id')) return 92;
  if (columnName.endsWith('TypeId')) return 90;
  if (columnName.endsWith('StatusId')) return 88;
  if (columnName.endsWith('SeqId')) return 90;
  if (columnName === 'createdStamp' || columnName === 'lastUpdatedStamp') return 95;
  if (columnName === 'fromDate' || columnName === 'thruDate') return 93;

  // Medium confidence
  if (columnName.includes('Date') || columnName.includes('Time')) return 80;
  if (columnName.includes('Amount') || columnName.includes('Quantity')) return 85;
  if (columnName.includes('Code') || columnName.includes('Name')) return 75;

  // Lower confidence for single-letter or cryptic abbreviations
  if (columnName.length <= 2) return 40;
  if (/^[A-Z]{2,}$/.test(columnName)) return 45; // All uppercase abbreviations

  // Default medium-low confidence
  return 60;
}

// Determine enrichment status distribution
function getEnrichmentStatus() {
  const rand = Math.random() * 100;
  if (rand < 60) return ENRICHMENT_STATUSES.AI_ENRICHED;
  if (rand < 85) return ENRICHMENT_STATUSES.NEEDS_REVIEW;
  if (rand < 95) return ENRICHMENT_STATUSES.APPROVED;
  return ENRICHMENT_STATUSES.DRAFT;
}

async function createDefaultUser() {
  console.log('Creating default admin user...');

  const bcryptjs = require('bcryptjs');
  const passwordHash = bcryptjs.hashSync('demo2026', 10);

  const query = `
    INSERT INTO users (email, password_hash, name, role)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (email) DO NOTHING
    RETURNING id
  `;

  const result = await pool.query(query, [
    'mark@solix.com',
    passwordHash,
    'Mark Lee',
    'admin'
  ]);

  if (result.rows.length > 0) {
    stats.users = 1;
    console.log('✓ Default admin user created');
    return result.rows[0].id;
  } else {
    console.log('✓ Admin user already exists');
    const userQuery = 'SELECT id FROM users WHERE email = $1';
    const userResult = await pool.query(userQuery, ['mark@solix.com']);
    return userResult.rows[0].id;
  }
}

async function loadOFBizEntities() {
  console.log('Loading OFBiz entities...');

  const filePath = path.join(
    __dirname,
    '../../OFBiz Schema/ofbiz_entities.json'
  );

  const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  console.log(`✓ Loaded ${data.length} OFBiz entities`);

  return data;
}

async function createApplication(userId) {
  console.log('Creating Apache OFBiz 18.12 application...');

  const query = `
    INSERT INTO applications (name, type, description, status, config)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
    RETURNING id
  `;

  const result = await pool.query(query, [
    'Apache OFBiz 18.12',
    'ofbiz',
    'Apache OFBiz 18.12 Enterprise Resource Planning System - Complete schema with 804 entities across 12 modules',
    'ingesting',
    JSON.stringify({
      version: '18.12',
      sourceType: 'ofbiz',
      loadedAt: new Date().toISOString(),
      entityCount: 804,
      columnCount: 5208
    })
  ]);

  stats.applications = 1;
  console.log('✓ Application created');
  return result.rows[0].id;
}

async function createModules(appId, entities) {
  console.log('Creating modules...');

  // Extract unique modules from entities
  const uniqueModules = new Map();
  entities.forEach(entity => {
    const module = entity.module || 'unknown';
    if (!uniqueModules.has(module)) {
      uniqueModules.set(module, {
        code: module,
        name: module
          .split('_')
          .map(w => w.charAt(0).toUpperCase() + w.slice(1))
          .join(' '),
        description: `${module.charAt(0).toUpperCase() + module.slice(1)} module entities`
      });
    }
  });

  const moduleMap = new Map();

  for (const [code, moduleData] of uniqueModules) {
    const query = `
      INSERT INTO app_modules (app_id, code, name, description)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (app_id, code) DO UPDATE SET name = $3, description = $4
      RETURNING id
    `;

    const result = await pool.query(query, [
      appId,
      moduleData.code,
      moduleData.name,
      moduleData.description
    ]);

    moduleMap.set(code, result.rows[0].id);
    stats.modules++;
  }

  console.log(`✓ Created ${stats.modules} modules`);
  return moduleMap;
}

async function createTables(appId, entities, moduleMap) {
  console.log('Creating tables...');

  const tableMap = new Map();

  for (const entity of entities) {
    const moduleId = moduleMap.get(entity.module || 'unknown');

    const query = `
      INSERT INTO app_tables (app_id, module_id, table_name, entity_name, description)
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (app_id, table_name) DO UPDATE SET entity_name = $4, description = $5
      RETURNING id
    `;

    const result = await pool.query(query, [
      appId,
      moduleId,
      entity.table_name,
      entity.entity_name,
      entity.title || camelCaseToTitleCase(entity.entity_name)
    ]);

    tableMap.set(entity.entity_name, {
      id: result.rows[0].id,
      primaryKeys: entity.primary_keys || [],
      fields: entity.fields || []
    });

    stats.tables++;
  }

  console.log(`✓ Created ${stats.tables} tables`);
  return tableMap;
}

async function createColumns(tableMap) {
  console.log('Creating columns...');

  const columnMap = new Map();
  let batchCount = 0;
  const batchSize = 500;
  let batch = [];

  for (const [entityName, tableData] of tableMap) {
    const { id: tableId, primaryKeys, fields } = tableData;

    for (const field of fields) {
      const isPK = primaryKeys && primaryKeys.includes(field.name);
      const businessName = generateBusinessName(
        field.name,
        entityName,
        field.pg_type,
        field.ofbiz_type
      );
      const confidence = calculateConfidenceScore(field.name, field.pg_type);
      const status = getEnrichmentStatus();

      batch.push({
        tableId,
        columnName: field.name,
        dataType: field.pg_type,
        isPK,
        businessName,
        description: `${businessName} field in ${entityName}`,
        enrichmentStatus: status,
        confidenceScore: confidence,
        enrichedBy: 'ai',
        enrichedAt: new Date()
      });

      stats.columns++;

      // Execute batch when limit reached
      if (batch.length >= batchSize) {
        await insertColumnBatch(batch);
        batch = [];
        batchCount++;
      }
    }
  }

  // Insert remaining columns
  if (batch.length > 0) {
    await insertColumnBatch(batch);
    batchCount++;
  }

  console.log(`✓ Created ${stats.columns} columns in ${batchCount} batches`);
}

async function insertColumnBatch(batch) {
  const query = `
    INSERT INTO app_columns (
      table_id, column_name, data_type, is_pk,
      business_name, description, enrichment_status,
      confidence_score, enriched_by, enriched_at
    ) VALUES
    ${batch.map((_, i) => {
      const offset = i * 10 + 1;
      return `($${offset}, $${offset+1}, $${offset+2}, $${offset+3}, $${offset+4}, $${offset+5}, $${offset+6}, $${offset+7}, $${offset+8}, $${offset+9})`;
    }).join(', ')}
    ON CONFLICT (table_id, column_name) DO UPDATE SET
      business_name = EXCLUDED.business_name,
      enrichment_status = EXCLUDED.enrichment_status,
      confidence_score = EXCLUDED.confidence_score
  `;

  const values = [];
  batch.forEach(col => {
    values.push(
      col.tableId,
      col.columnName,
      col.dataType,
      col.isPK,
      col.businessName,
      col.description,
      col.enrichmentStatus,
      col.confidenceScore,
      col.enrichedBy,
      col.enrichedAt
    );
  });

  await pool.query(query, values);
}

async function createRelationships(entities, tableMap) {
  console.log('Creating relationships...');

  let relationshipCount = 0;
  const appId = (await pool.query('SELECT id FROM applications WHERE name = $1',
    ['Apache OFBiz 18.12'])).rows[0].id;

  for (const entity of entities) {
    const tableData = tableMap.get(entity.entity_name);
    if (!tableData) continue;

    const relations = entity.relations || [];

    for (const rel of relations) {
      const relTableData = tableMap.get(rel.rel_entity);
      if (!relTableData) continue;

      // Extract key mappings
      const keyMaps = rel.key_maps || [];
      for (const keyMap of keyMaps) {
        const query = `
          INSERT INTO app_relationships (
            app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality
          ) VALUES ($1, $2, $3, $4, $5, $6, $7)
          ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING
        `;

        const cardinality = rel.type === 'one' ? 'one_to_one' : 'one_to_many';

        try {
          await pool.query(query, [
            appId,
            tableData.id,
            keyMap.field_name,
            relTableData.id,
            keyMap.rel_field_name,
            'fk',
            cardinality
          ]);
          relationshipCount++;
          stats.relationships++;
        } catch (err) {
          // Silently skip duplicate relationship errors
        }
      }
    }
  }

  console.log(`✓ Created ${stats.relationships} relationships`);
}

async function createPipelineHistory(userId, appId) {
  console.log('Creating pipeline run history...');

  const now = new Date();

  // Three runs: failed, in-progress, completed
  const runs = [
    {
      status: 'failed',
      startedAt: new Date(now - 7 * 24 * 60 * 60 * 1000), // 7 days ago
      completedAt: new Date(now - 7 * 24 * 60 * 60 * 1000 + 30 * 60 * 1000),
      stages: ['ingest', 'profile']
    },
    {
      status: 'running',
      startedAt: new Date(now - 2 * 60 * 60 * 1000), // 2 hours ago
      completedAt: null,
      stages: ['ingest', 'profile', 'infer', 'enrich']
    },
    {
      status: 'completed',
      startedAt: new Date(now - 24 * 60 * 60 * 1000), // 1 day ago
      completedAt: new Date(now - 24 * 60 * 60 * 1000 + 90 * 60 * 1000),
      stages: ['ingest', 'profile', 'infer', 'enrich']
    }
  ];

  for (const run of runs) {
    const query = `
      INSERT INTO pipeline_runs (app_id, triggered_by, status, started_at, completed_at, stages)
      VALUES ($1, $2, $3, $4, $5, $6)
    `;

    await pool.query(query, [
      appId,
      userId,
      run.status,
      run.startedAt,
      run.completedAt,
      JSON.stringify(run.stages)
    ]);

    stats.pipelineRuns++;
  }

  console.log(`✓ Created ${stats.pipelineRuns} pipeline runs`);
}

async function createQueryPatterns(appId) {
  console.log('Creating query patterns...');

  const patterns = [
    {
      name: 'List all orders for a customer',
      nlTemplate: 'Show me orders for customer {customerId}',
      sqlTemplate: 'SELECT * FROM orders WHERE party_id = $1',
      tablesUsed: ['orders', 'parties']
    },
    {
      name: 'Outstanding invoices by age',
      nlTemplate: 'Show invoices older than {days} days',
      sqlTemplate: 'SELECT * FROM invoices WHERE created_date < NOW() - INTERVAL $1 DAY AND status != "paid"',
      tablesUsed: ['invoices']
    },
    {
      name: 'Product inventory levels',
      nlTemplate: 'Get current inventory for product {productId}',
      sqlTemplate: 'SELECT quantity_on_hand, reserved_quantity FROM inventory WHERE product_id = $1',
      tablesUsed: ['inventory', 'products']
    },
    {
      name: 'Sales by product category',
      nlTemplate: 'Sales totals grouped by product category',
      sqlTemplate: 'SELECT pc.category_name, SUM(oi.quantity * oi.unit_price) FROM order_items oi JOIN products p ON oi.product_id = p.id JOIN product_categories pc ON p.category_id = pc.id GROUP BY pc.category_name',
      tablesUsed: ['order_items', 'products', 'product_categories']
    },
    {
      name: 'Customer purchase frequency',
      nlTemplate: 'How many orders has customer {customerId} placed?',
      sqlTemplate: 'SELECT COUNT(*) FROM orders WHERE party_id = $1',
      tablesUsed: ['orders']
    },
    {
      name: 'Employee hierarchy',
      nlTemplate: 'Show reporting structure for employee {employeeId}',
      sqlTemplate: 'SELECT * FROM employees WHERE reports_to = $1',
      tablesUsed: ['employees']
    },
    {
      name: 'Vendor performance metrics',
      nlTemplate: 'Performance metrics for vendor {vendorId}',
      sqlTemplate: 'SELECT v.name, COUNT(po.id) as total_orders, AVG(EXTRACT(DAY FROM po.delivered_date - po.created_date)) as avg_delivery_days FROM vendors v LEFT JOIN purchase_orders po ON v.id = po.vendor_id WHERE v.id = $1 GROUP BY v.id, v.name',
      tablesUsed: ['vendors', 'purchase_orders']
    },
    {
      name: 'Budget variance analysis',
      nlTemplate: 'Budget variance for cost center {costCenterId}',
      sqlTemplate: 'SELECT budget_amount, actual_amount, (budget_amount - actual_amount) as variance FROM budgets WHERE cost_center_id = $1',
      tablesUsed: ['budgets']
    },
    {
      name: 'Open purchase orders',
      nlTemplate: 'All open purchase orders',
      sqlTemplate: 'SELECT * FROM purchase_orders WHERE status IN ("open", "pending") ORDER BY created_date DESC',
      tablesUsed: ['purchase_orders']
    },
    {
      name: 'Customer payment history',
      nlTemplate: 'Payment history for customer {customerId}',
      sqlTemplate: 'SELECT p.payment_date, p.amount, p.method FROM payments p JOIN orders o ON p.order_id = o.id WHERE o.party_id = $1 ORDER BY p.payment_date DESC',
      tablesUsed: ['payments', 'orders']
    }
  ];

  for (const pattern of patterns) {
    const query = `
      INSERT INTO query_patterns (app_id, pattern_name, nl_template, sql_template, tables_used, status, usage_count, confidence)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      ON CONFLICT (app_id, pattern_name) DO NOTHING
    `;

    await pool.query(query, [
      appId,
      pattern.name,
      pattern.nlTemplate,
      pattern.sqlTemplate,
      pattern.tablesUsed,
      'discovered',
      Math.floor(Math.random() * 20),
      Math.round((Math.random() * 30 + 70) * 100) / 100 // 70-100
    ]);

    stats.queryPatterns++;
  }

  console.log(`✓ Created ${stats.queryPatterns} query patterns`);
}

async function createTestQueries(userId, appId) {
  console.log('Creating test queries...');

  const queries = [
    {
      nlQuery: 'What are the top 10 customers by order value?',
      generatedSql: 'SELECT p.party_id, COUNT(*) as order_count, SUM(oi.quantity * oi.unit_price) as total_value FROM orders o JOIN order_items oi ON o.id = oi.order_id JOIN parties p ON o.party_id = p.id GROUP BY p.party_id ORDER BY total_value DESC LIMIT 10',
      feedback: 'thumbs_up',
      confidence: 0.92
    },
    {
      nlQuery: 'Show me all products that are currently out of stock',
      generatedSql: 'SELECT * FROM products WHERE quantity_on_hand = 0',
      feedback: 'thumbs_up',
      confidence: 0.88
    },
    {
      nlQuery: 'List invoices due in the next 30 days',
      generatedSql: 'SELECT * FROM invoices WHERE due_date BETWEEN NOW() AND NOW() + INTERVAL 30 DAY AND status != "paid"',
      feedback: null,
      confidence: 0.85
    },
    {
      nlQuery: 'What is the total revenue for Q1 2024?',
      generatedSql: 'SELECT SUM(oi.quantity * oi.unit_price) FROM order_items oi JOIN orders o ON oi.order_id = o.id WHERE EXTRACT(YEAR FROM o.order_date) = 2024 AND EXTRACT(QUARTER FROM o.order_date) = 1',
      feedback: 'thumbs_up',
      confidence: 0.91
    },
    {
      nlQuery: 'Show employees and their managers',
      generatedSql: 'SELECT e.name as employee_name, m.name as manager_name FROM employees e LEFT JOIN employees m ON e.reports_to = m.id',
      feedback: 'thumbs_down',
      confidence: 0.78
    }
  ];

  for (const testQuery of queries) {
    const query = `
      INSERT INTO test_queries (app_id, user_id, nl_query, generated_sql, feedback, confidence, created_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
    `;

    await pool.query(query, [
      appId,
      userId,
      testQuery.nlQuery,
      testQuery.generatedSql,
      testQuery.feedback,
      testQuery.confidence,
      new Date(Date.now() - Math.random() * 7 * 24 * 60 * 60 * 1000) // Random within last 7 days
    ]);

    stats.testQueries++;
  }

  console.log(`✓ Created ${stats.testQueries} test queries`);
}

async function resetDatabase() {
  console.log('Resetting database...');

  const tables = [
    'curation_log',
    'test_queries',
    'query_patterns',
    'pipeline_runs',
    'app_relationships',
    'app_columns',
    'app_tables',
    'app_modules',
    'applications',
    'users'
  ];

  for (const table of tables) {
    try {
      await pool.query(`TRUNCATE TABLE ${table} CASCADE`);
    } catch (err) {
      // Table might not exist, that's okay
    }
  }

  console.log('✓ Database reset');
}

async function main() {
  try {
    console.log('\n========================================');
    console.log('BOKG Builder - OFBiz Schema Seed');
    console.log('========================================\n');

    // Check for --reset flag
    const shouldReset = process.argv.includes('--reset');
    if (shouldReset) {
      await resetDatabase();
    }

    // Run schema creation
    console.log('Creating schema...');
    const schemaSQL = fs.readFileSync(
      path.join(__dirname, 'schema.sql'),
      'utf8'
    );
    await pool.query(schemaSQL);
    console.log('✓ Schema ready');

    // Create default user
    const userId = await createDefaultUser();

    // Load OFBiz entities
    const entities = await loadOFBizEntities();

    // Create application
    const appId = await createApplication(userId);

    // Create modules
    const moduleMap = await createModules(appId, entities);

    // Create tables
    const tableMap = await createTables(appId, entities, moduleMap);

    // Create columns
    await createColumns(tableMap);

    // Create relationships
    await createRelationships(entities, tableMap);

    // Create pipeline history
    await createPipelineHistory(userId, appId);

    // Create query patterns
    await createQueryPatterns(appId);

    // Create test queries
    await createTestQueries(userId, appId);

    console.log('\n========================================');
    console.log('Seed Complete!');
    console.log('========================================');
    console.log(`Users:            ${stats.users}`);
    console.log(`Applications:     ${stats.applications}`);
    console.log(`Modules:          ${stats.modules}`);
    console.log(`Tables:           ${stats.tables}`);
    console.log(`Columns:          ${stats.columns}`);
    console.log(`Relationships:    ${stats.relationships}`);
    console.log(`Pipeline Runs:    ${stats.pipelineRuns}`);
    console.log(`Query Patterns:   ${stats.queryPatterns}`);
    console.log(`Test Queries:     ${stats.testQueries}`);
    console.log('========================================\n');

    process.exit(0);
  } catch (err) {
    console.error('Error during seeding:', err);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
