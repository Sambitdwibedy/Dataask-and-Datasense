#!/usr/bin/env node

/**
 * BOKG Builder - BIRD Benchmark Database Seed Script
 * Loads BIRD benchmark databases into PostgreSQL
 *
 * This script creates two sample databases from the BIRD benchmark:
 * 1. California Schools Database
 * 2. Financial Database
 *
 * Usage: node seed-bird.js
 */

const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

const {
  ENRICHMENT_STATUSES
} = require('../shared/constants');

const stats = {
  applications: 0,
  modules: 0,
  tables: 0,
  columns: 0,
  relationships: 0
};

function camelCaseToTitleCase(str) {
  if (!str) return '';
  return str
    .replace(/([A-Z])/g, ' $1')
    .replace(/^./, (char) => char.toUpperCase())
    .replace(/_/g, ' ')
    .trim();
}

function calculateConfidenceScore(columnName, dataType = '') {
  if (columnName.includes('_id')) return 92;
  if (columnName === 'id') return 95;
  if (columnName.includes('date') || columnName.includes('Date')) return 90;
  if (columnName.includes('amount') || columnName.includes('Amount')) return 85;
  if (columnName.includes('count') || columnName.includes('Count')) return 80;
  return 75;
}

function generateBusinessName(columnName, tableName, dataType) {
  const mappings = {
    // California Schools
    cdsCode: 'School Code',
    ncesDist: 'NCES District ID',
    ncesSchool: 'NCES School ID',
    statusType: 'Status Type',
    county: 'County',
    district: 'District Name',
    school: 'School Name',
    street: 'Street Address',
    streetAbr: 'Street Address (Abbreviated)',
    city: 'City',
    zip: 'Zip Code',
    state: 'State',
    mailStreet: 'Mailing Street',
    mailStrAbr: 'Mailing Street (Abbreviated)',
    mailCity: 'Mailing City',
    mailZip: 'Mailing Zip',
    mailState: 'Mailing State',
    phone: 'Phone Number',
    ext: 'Extension',
    website: 'Website',
    openDate: 'Open Date',
    closedDate: 'Closed Date',
    charter: 'Charter Flag',
    testYear: 'Test Year',
    satScore: 'SAT Score',
    frpmEligible: 'FRPM Eligible Count',
    frpmParticipate: 'FRPM Participant Count',

    // Financial Database (actual SQLite column names are snake_case)
    account_id: 'Account ID',
    district_id: 'District ID',
    frequency: 'Frequency',
    date: 'Date',
    client_id: 'Client ID',
    loan_id: 'Loan ID',
    card_id: 'Card ID',
    trans_id: 'Transaction ID',
    disp_id: 'Disposition ID',
    order_id: 'Order ID',
    amount: 'Amount',
    balance: 'Balance',
    status: 'Status',
    type: 'Type',
    duration: 'Duration',
    payments: 'Monthly Payment',
    gender: 'Gender',
    birth_date: 'Birth Date',
    k_symbol: 'Transaction Symbol',
    bank: 'Partner Bank',
    account: 'Partner Account',
    bank_to: 'Recipient Bank',
    account_to: 'Recipient Account',
    issued: 'Issue Date',
    operation: 'Operation Type',
    birthNumber: 'Birth Number',
    district: 'District',
    birthDate: 'Birth Date',
    createdAt: 'Created At'
  };

  if (mappings[columnName]) {
    return mappings[columnName];
  }

  return camelCaseToTitleCase(columnName);
}

async function createApplication(name, type, description) {
  const query = `
    INSERT INTO applications (name, type, description, status, config)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (name) DO UPDATE SET updated_at = NOW()
    RETURNING id
  `;

  const result = await pool.query(query, [
    name,
    type,
    description,
    'profiling',
    JSON.stringify({
      source: 'BIRD',
      loadedAt: new Date().toISOString()
    })
  ]);

  stats.applications++;
  return result.rows[0].id;
}

// Creates a domain (business grouping of objects). DB table: app_modules
async function createDomain(appId, code, name, description) {
  const query = `
    INSERT INTO app_modules (app_id, code, name, description)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (app_id, code) DO UPDATE SET name = $3
    RETURNING id
  `;

  const result = await pool.query(query, [appId, code, name, description]);
  stats.modules++;
  return result.rows[0].id;
}

async function createTable(appId, moduleId, tableName, description) {
  const query = `
    INSERT INTO app_tables (app_id, module_id, table_name, entity_name, description)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (app_id, table_name) DO UPDATE SET description = $5
    RETURNING id
  `;

  const result = await pool.query(query, [
    appId,
    moduleId,
    tableName,
    camelCaseToTitleCase(tableName),
    description
  ]);

  stats.tables++;
  return result.rows[0].id;
}

async function createColumn(tableId, columnName, dataType, description, isPK = false) {
  const businessName = generateBusinessName(columnName, '', dataType);
  const confidence = calculateConfidenceScore(columnName, dataType);

  const query = `
    INSERT INTO app_columns (
      table_id, column_name, data_type, is_pk,
      business_name, description, enrichment_status,
      confidence_score, enriched_by, enriched_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT (table_id, column_name) DO NOTHING
  `;

  await pool.query(query, [
    tableId,
    columnName,
    dataType,
    isPK,
    businessName,
    description,
    'approved',
    confidence,
    'ai+human',
    new Date()
  ]);

  stats.columns++;
}

async function createRelationship(appId, fromTableId, fromColumn, toTableId, toColumn) {
  const query = `
    INSERT INTO app_relationships (
      app_id, from_table_id, from_column, to_table_id, to_column, rel_type, cardinality
    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (from_table_id, from_column, to_table_id, to_column) DO NOTHING
  `;

  try {
    await pool.query(query, [
      appId,
      fromTableId,
      fromColumn,
      toTableId,
      toColumn,
      'inferred',
      'one_to_many'
    ]);
    stats.relationships++;
  } catch (err) {
    // Silently skip
  }
}

async function seedCaliforniaSchools() {
  console.log('\nSeeding California Schools Database...');

  const appId = await createApplication(
    'California Schools Database',
    'postgresql',
    'California public school data including school information, SAT scores, and free/reduced price meal program participation. Dataset from BIRD benchmark with ~1,000 schools across 58 counties.'
  );

  const mainModuleId = await createDomain(appId, 'main', 'School Data', 'Primary school and assessment data');

  // Create tables
  const schoolsTableId = await createTable(
    appId,
    mainModuleId,
    'schools',
    'California public schools with contact and operational information'
  );

  const satscoresTableId = await createTable(
    appId,
    mainModuleId,
    'satscores',
    'SAT test scores by school and year'
  );

  const frpmTableId = await createTable(
    appId,
    mainModuleId,
    'frpm',
    'Free and reduced-price meals program enrollment'
  );

  // Schools table columns
  const schoolColumns = [
    { name: 'cdsCode', type: 'VARCHAR(20)', desc: 'California Department of Education school code', pk: true },
    { name: 'ncesDist', type: 'VARCHAR(20)', desc: 'National Center for Educational Statistics district ID' },
    { name: 'ncesSchool', type: 'VARCHAR(20)', desc: 'National Center for Educational Statistics school ID' },
    { name: 'statusType', type: 'VARCHAR(50)', desc: 'Operational status of school' },
    { name: 'county', type: 'VARCHAR(100)', desc: 'County name' },
    { name: 'district', type: 'VARCHAR(255)', desc: 'School district name' },
    { name: 'school', type: 'VARCHAR(255)', desc: 'School name' },
    { name: 'street', type: 'VARCHAR(255)', desc: 'Physical street address' },
    { name: 'city', type: 'VARCHAR(100)', desc: 'City of school location' },
    { name: 'zip', type: 'VARCHAR(10)', desc: 'Zip code' },
    { name: 'state', type: 'VARCHAR(2)', desc: 'State (always CA)' },
    { name: 'phone', type: 'VARCHAR(20)', desc: 'School phone number' },
    { name: 'website', type: 'VARCHAR(255)', desc: 'School website URL' },
    { name: 'openDate', type: 'DATE', desc: 'Date school opened' },
    { name: 'closedDate', type: 'DATE', desc: 'Date school closed (if applicable)' },
    { name: 'charter', type: 'BOOLEAN', desc: 'Whether school is a charter school' }
  ];

  for (const col of schoolColumns) {
    await createColumn(schoolsTableId, col.name, col.type, col.desc, col.pk);
  }

  // SAT Scores table columns
  const satColumns = [
    { name: 'testYear', type: 'INTEGER', desc: 'Academic test year', pk: true },
    { name: 'cdsCode', type: 'VARCHAR(20)', desc: 'School code reference', pk: true },
    { name: 'satScore', type: 'NUMERIC(5,2)', desc: 'Average SAT score' },
    { name: 'numTestTakers', type: 'INTEGER', desc: 'Number of students taking test' }
  ];

  for (const col of satColumns) {
    await createColumn(satscoresTableId, col.name, col.type, col.desc, col.pk);
  }

  // FRPM table columns
  const frpmColumns = [
    { name: 'testYear', type: 'INTEGER', desc: 'Academic test year', pk: true },
    { name: 'cdsCode', type: 'VARCHAR(20)', desc: 'School code reference', pk: true },
    { name: 'frpmEligible', type: 'INTEGER', desc: 'Number of students eligible for FRPM' },
    { name: 'frpmParticipate', type: 'INTEGER', desc: 'Number of students participating in FRPM' },
    { name: 'frpmPercentage', type: 'NUMERIC(5,2)', desc: 'Percentage of students participating' }
  ];

  for (const col of frpmColumns) {
    await createColumn(frpmTableId, col.name, col.type, col.desc, col.pk);
  }

  // Create relationships
  await createRelationship(appId, satscoresTableId, 'cdsCode', schoolsTableId, 'cdsCode');
  await createRelationship(appId, frpmTableId, 'cdsCode', schoolsTableId, 'cdsCode');

  console.log('✓ California Schools Database seeded');
}

async function seedFinancialDatabase() {
  console.log('\nSeeding Financial Database...');

  const appId = await createApplication(
    'Financial Database',
    'postgresql',
    'Czech bank financial data including accounts, clients, transactions, loans, and cards. Dataset from BIRD benchmark with transactions from 1993-1998.'
  );

  const mainModuleId = await createDomain(appId, 'main', 'Banking Data', 'Core banking and financial transaction data');

  // Create tables
  const accountTableId = await createTable(appId, mainModuleId, 'account', 'Bank accounts');
  const clientTableId = await createTable(appId, mainModuleId, 'client', 'Bank clients/customers');
  const transTableId = await createTable(appId, mainModuleId, 'trans', 'Financial transactions');
  const cardTableId = await createTable(appId, mainModuleId, 'card', 'Client debit and credit cards');
  const loanTableId = await createTable(appId, mainModuleId, 'loan', 'Customer loans');
  const dispTableId = await createTable(appId, mainModuleId, 'disp', 'Client account dispositions');
  const orderTableId = await createTable(appId, mainModuleId, 'order', 'Standing orders');
  const districtTableId = await createTable(appId, mainModuleId, 'district', 'Geographic district information');

  // Account table — column names match actual SQLite schema
  const accountColumns = [
    { name: 'account_id', type: 'BIGINT', desc: 'Unique account identifier', pk: true },
    { name: 'district_id', type: 'BIGINT', desc: 'Branch district code' },
    { name: 'frequency', type: 'TEXT', desc: 'Statement frequency (POPLATEK MESICNE=monthly, POPLATEK TYDNE=weekly, POPLATEK PO OBRATU=after transaction)' },
    { name: 'date', type: 'TEXT', desc: 'Account creation date (stored as text, YYYY-MM-DD format, spans 1993-1997)' }
  ];

  for (const col of accountColumns) {
    await createColumn(accountTableId, col.name, col.type, col.desc, col.pk);
  }

  // Client table
  const clientColumns = [
    { name: 'client_id', type: 'BIGINT', desc: 'Unique client identifier', pk: true },
    { name: 'gender', type: 'TEXT', desc: 'Gender (F=Female, M=Male)' },
    { name: 'birth_date', type: 'TEXT', desc: 'Client date of birth (stored as text)' },
    { name: 'district_id', type: 'BIGINT', desc: 'Client district of residence' }
  ];

  for (const col of clientColumns) {
    await createColumn(clientTableId, col.name, col.type, col.desc, col.pk);
  }

  // Transaction table
  const transColumns = [
    { name: 'trans_id', type: 'BIGINT', desc: 'Unique transaction identifier', pk: true },
    { name: 'account_id', type: 'BIGINT', desc: 'Account reference' },
    { name: 'date', type: 'TEXT', desc: 'Transaction date (stored as text)' },
    { name: 'type', type: 'TEXT', desc: 'Transaction type (PRIJEM=credit, VYDAJ=debit)' },
    { name: 'operation', type: 'TEXT', desc: 'Operation type (VYBER KARTOU=card withdrawal, VKLAD=deposit, PREVOD Z UCTU=transfer from, VYBER=cash withdrawal, PREVOD NA UCET=transfer to)' },
    { name: 'amount', type: 'DOUBLE PRECISION', desc: 'Transaction amount' },
    { name: 'balance', type: 'DOUBLE PRECISION', desc: 'Account balance after transaction' },
    { name: 'k_symbol', type: 'TEXT', desc: 'Transaction characteristic symbol (POJISTNE=insurance, SLUZBY=payment for statement, UROK=interest credited, SANKC. UROK=sanction interest, SIPO=household, DUCHOD=pension, UVER=loan payment)' },
    { name: 'bank', type: 'TEXT', desc: 'Partner bank code (for inter-bank transactions)' },
    { name: 'account', type: 'TEXT', desc: 'Partner account number (for inter-bank transactions)' }
  ];

  for (const col of transColumns) {
    await createColumn(transTableId, col.name, col.type, col.desc, col.pk);
  }

  // Card table
  const cardColumns = [
    { name: 'card_id', type: 'BIGINT', desc: 'Unique card identifier', pk: true },
    { name: 'disp_id', type: 'BIGINT', desc: 'Disposition reference' },
    { name: 'type', type: 'TEXT', desc: 'Card type (gold, classic, junior)' },
    { name: 'issued', type: 'TEXT', desc: 'Card issue date (stored as text)' }
  ];

  for (const col of cardColumns) {
    await createColumn(cardTableId, col.name, col.type, col.desc, col.pk);
  }

  // Loan table
  const loanColumns = [
    { name: 'loan_id', type: 'BIGINT', desc: 'Unique loan identifier', pk: true },
    { name: 'account_id', type: 'BIGINT', desc: 'Account reference' },
    { name: 'date', type: 'TEXT', desc: 'Loan initiation date (stored as text)' },
    { name: 'amount', type: 'BIGINT', desc: 'Loan amount' },
    { name: 'duration', type: 'BIGINT', desc: 'Loan duration in months' },
    { name: 'payments', type: 'DOUBLE PRECISION', desc: 'Monthly payment amount' },
    { name: 'status', type: 'TEXT', desc: 'Loan status (A=contract finished, no problems; B=contract finished, loan not paid; C=running contract, OK; D=running contract, client in debt)' }
  ];

  for (const col of loanColumns) {
    await createColumn(loanTableId, col.name, col.type, col.desc, col.pk);
  }

  // Disposition table
  const dispColumns = [
    { name: 'disp_id', type: 'BIGINT', desc: 'Unique disposition identifier', pk: true },
    { name: 'client_id', type: 'BIGINT', desc: 'Client reference' },
    { name: 'account_id', type: 'BIGINT', desc: 'Account reference' },
    { name: 'type', type: 'TEXT', desc: 'Account relationship type (OWNER, DISPONENT)' }
  ];

  for (const col of dispColumns) {
    await createColumn(dispTableId, col.name, col.type, col.desc, col.pk);
  }

  // Order table
  const orderColumns = [
    { name: 'order_id', type: 'BIGINT', desc: 'Unique order identifier', pk: true },
    { name: 'account_id', type: 'BIGINT', desc: 'Account reference' },
    { name: 'bank_to', type: 'TEXT', desc: 'Recipient bank code' },
    { name: 'account_to', type: 'TEXT', desc: 'Recipient account number' },
    { name: 'amount', type: 'DOUBLE PRECISION', desc: 'Transfer amount' },
    { name: 'k_symbol', type: 'TEXT', desc: 'Transaction characteristic symbol (POJISTNE=insurance, SIPO=household, LEASING=leasing, UVER=loan payment)' }
  ];

  for (const col of orderColumns) {
    await createColumn(orderTableId, col.name, col.type, col.desc, col.pk);
  }

  // District table — columns A2-A16 are cryptic names from the BIRD benchmark
  const districtColumns = [
    { name: 'district_id', type: 'BIGINT', desc: 'Unique district identifier', pk: true },
    { name: 'A2', type: 'TEXT', desc: 'District name' },
    { name: 'A3', type: 'TEXT', desc: 'Region name' },
    { name: 'A4', type: 'BIGINT', desc: 'Number of inhabitants' },
    { name: 'A5', type: 'BIGINT', desc: 'Number of municipalities with <500 inhabitants' },
    { name: 'A6', type: 'BIGINT', desc: 'Number of municipalities with 500-1999 inhabitants' },
    { name: 'A7', type: 'BIGINT', desc: 'Number of municipalities with 2000-9999 inhabitants' },
    { name: 'A8', type: 'BIGINT', desc: 'Number of municipalities with >=10000 inhabitants' },
    { name: 'A9', type: 'BIGINT', desc: 'Number of cities' },
    { name: 'A10', type: 'DOUBLE PRECISION', desc: 'Ratio of urban inhabitants' },
    { name: 'A11', type: 'BIGINT', desc: 'Average salary' },
    { name: 'A12', type: 'DOUBLE PRECISION', desc: 'Unemployment rate 1995' },
    { name: 'A13', type: 'DOUBLE PRECISION', desc: 'Unemployment rate 1996' },
    { name: 'A14', type: 'BIGINT', desc: 'Number of entrepreneurs per 1000 inhabitants' },
    { name: 'A15', type: 'BIGINT', desc: 'Number of committed crimes 1995' },
    { name: 'A16', type: 'BIGINT', desc: 'Number of committed crimes 1996' }
  ];

  for (const col of districtColumns) {
    await createColumn(districtTableId, col.name, col.type, col.desc, col.pk);
  }

  // Create relationships — using actual column names from SQLite
  await createRelationship(appId, accountTableId, 'district_id', districtTableId, 'district_id');
  await createRelationship(appId, clientTableId, 'district_id', districtTableId, 'district_id');
  await createRelationship(appId, transTableId, 'account_id', accountTableId, 'account_id');
  await createRelationship(appId, cardTableId, 'disp_id', dispTableId, 'disp_id');
  await createRelationship(appId, loanTableId, 'account_id', accountTableId, 'account_id');
  await createRelationship(appId, dispTableId, 'client_id', clientTableId, 'client_id');
  await createRelationship(appId, dispTableId, 'account_id', accountTableId, 'account_id');
  await createRelationship(appId, orderTableId, 'account_id', accountTableId, 'account_id');

  console.log('✓ Financial Database seeded');
}

async function main() {
  try {
    console.log('\n========================================');
    console.log('BOKG Builder - BIRD Benchmark Seed');
    console.log('========================================');

    // Note: Assumes schema.sql has already been run
    // If running standalone, uncomment below:
    // const fs = require('fs');
    // const schemaSQL = fs.readFileSync('schema.sql', 'utf8');
    // await pool.query(schemaSQL);

    await seedCaliforniaSchools();
    await seedFinancialDatabase();

    console.log('\n========================================');
    console.log('BIRD Seed Complete!');
    console.log('========================================');
    console.log(`Applications:     ${stats.applications}`);
    console.log(`Domains:          ${stats.modules}`);
    console.log(`Tables:           ${stats.tables}`);
    console.log(`Columns:          ${stats.columns}`);
    console.log(`Relationships:    ${stats.relationships}`);
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
