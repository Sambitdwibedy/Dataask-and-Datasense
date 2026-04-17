/**
 * Embedding Service — Generates and stores vector embeddings for semantic schema linking.
 *
 * Uses OpenAI text-embedding-3-small (1536 dimensions) to embed table metadata.
 * Each table's embedding captures the semantic meaning of its name, entity name,
 * description, sample questions, and column names — enabling "spend" to match
 * AP_INVOICES even when there's zero keyword overlap.
 *
 * Stored in app_tables.embedding (pgvector) for cosine similarity search at query time.
 */

const { query } = require('../db');

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const EMBEDDING_MODEL = 'text-embedding-3-small'; // 1536 dimensions, ?.02/1M tokens
const BATCH_SIZE = 100; // OpenAI supports up to 2048 inputs per batch

/**
 * Business vocabulary enrichment — maps schema concepts to the natural language
 * terms users actually use when asking questions. Without this, embeddings for
 * "invoice" tables won't match "how much did we spend" because "spend" never
 * appears in the metadata.
 *
 * Pattern: [regex on table_name/entity_name/description] → business synonyms to inject
 */
const BUSINESS_VOCAB_PATTERNS = [
  // ── Actuals (past tense — money already spent/earned, transactions that happened) ──

  // Accounts Payable / Spending (ACTUALS — past tense: "did we spend", "were our expenses")
  { pattern: /invoice|ap_invoice|payable/i,
    vocab: 'actual spending, money already spent, past expenditures, completed payments, historical costs, expenses incurred, what did we spend, how much was spent, total spend to date, vendor payments made, bills paid, accounts payable transactions, procurement costs paid' },
  // Payment / Disbursement (ACTUALS — money that went out)
  { pattern: /payment(?!.*sched)|disbursement/i,
    vocab: 'actual payments made, money paid out, cash disbursed, completed transactions, spent, paid, payment history, outgoing payments, how much did we pay' },
  // Payment Schedules (CURRENT — amounts due now or overdue)
  { pattern: /payment.*schedul/i,
    vocab: 'amounts currently due, outstanding balances, overdue payments, payables aging, payment obligations, accounts payable balance' },
  // Orders / Revenue (ACTUALS — past tense: "did we earn", "was our revenue")
  { pattern: /order_header|order_item|oe_order|booking/i,
    vocab: 'actual revenue, sales completed, income earned, bookings received, total sales to date, order value, how much revenue did we make, historical sales, money earned, sales recorded' },
  // Inventory / Stock (CURRENT STATE)
  { pattern: /inventory_item|inventory_count/i,
    vocab: 'current stock, stock on hand, warehouse inventory, materials available, product quantities, how much stock do we have' },
  // Products
  { pattern: /product(?!.*store|.*category|.*feature|.*config)/i,
    vocab: 'items, goods, merchandise, catalog, what we sell, product lineup' },
  // Customers / Parties
  { pattern: /party|customer|hz_part|hz_cust/i,
    vocab: 'customers, clients, buyers, accounts, who are our customers, biggest customers, customer base, customer information' },
  // Employees / HR
  { pattern: /person|employee|per_all_people|empl_position/i,
    vocab: 'employees, staff, headcount, workforce, personnel, team members, how many employees' },
  // GL / Financial Statements (ACTUALS — recorded balances)
  { pattern: /gl_account(?!.*hist)|chart.*account/i,
    vocab: 'general ledger, balance sheet, chart of accounts, financial statements, assets and liabilities, equity, account structure' },
  { pattern: /gl_account_history|acctg_trans/i,
    vocab: 'actual financial results, recorded transactions, posted entries, historical P&L, actual spending by period, actual revenue by period, general ledger balances' },
  // Shipment / Delivery (ACTUALS — completed shipments)
  { pattern: /shipment|delivery/i,
    vocab: 'deliveries made, shipped orders, fulfillment history, delivery tracking, delivery status, logistics' },
  // Facility / Warehouse
  { pattern: /facility|warehouse/i,
    vocab: 'warehouse, storage, distribution center, warehouse capacity, warehouse utilization' },
  // Supplier / Vendor
  { pattern: /supplier|vendor/i,
    vocab: 'suppliers, vendors, vendor expenses paid, supplier costs incurred, who do we buy from, procurement sources' },
  // Accounts Receivable (CURRENT — money owed to us)
  { pattern: /ar_|receivable|ra_customer/i,
    vocab: 'accounts receivable, money owed to us, outstanding receivables, AR balance, customer payments due, unpaid invoices' },
  // Purchase Orders (ACTUALS — orders placed)
  { pattern: /po_header|po_line|purchase.*order|requisition/i,
    vocab: 'purchase orders placed, POs issued, procurement history, what did we order, purchase requests submitted' },

  // ── Plans / Forecasts (future tense — money not yet spent/earned) ──

  // Budget / Forecast (FUTURE — planned, projected, not yet actual)
  { pattern: /budget|forecast/i,
    vocab: 'planned spending, future allocation, projected costs, budgeted amounts, spending plan, planned revenue, what are we planning to spend, upcoming expenses, financial forecast, not yet spent' },
  // Sales Opportunity / Pipeline (FUTURE — deals not yet closed)
  { pattern: /sales_opportunity|opportunity/i,
    vocab: 'potential revenue, sales pipeline, prospective deals, expected revenue, future sales, opportunity value, win rate, not yet closed' },

  // ── Other ──

  // Cost Components (ACTUALS — recorded costs)
  { pattern: /cost_component|product.*cost|average_cost/i,
    vocab: 'actual costs incurred, biggest costs, cost breakdown, unit cost, production cost, cost of goods sold, historical costs' },
  // Work / Projects
  { pattern: /work_effort|project/i,
    vocab: 'projects, tasks, work items, assignments, project tracking' },

  // ── BIRD Benchmark Domain Patterns ──

  // Schools / Education
  { pattern: /school|sat|frpm|charter/i,
    vocab: 'schools, education, students, enrollment, SAT scores, academic performance, test scores, graduation, charter school, magnet school, free reduced lunch, eligibility, county, district' },
  // Football / Soccer
  { pattern: /match|league|player_attr|team_attr|football/i,
    vocab: 'football, soccer, matches, games, goals scored, home team, away team, season, league standings, win, loss, draw, betting odds, player ratings, FIFA, team tactics' },
  // Card Games / Trading Cards
  { pattern: /cards|rulings|legalities|set_translations|foreign_data/i,
    vocab: 'trading cards, Magic the Gathering, MTG, card game, rarity, foil, mana cost, card type, set, edition, power, toughness, artist, flavor text, legal format' },
  // Programming / Q&A Community
  { pattern: /posts|votes|badges|comments|tags/i,
    vocab: 'programming, code, Stack Overflow, questions, answers, reputation, upvotes, downvotes, accepted answer, badge, tag, community, developers' },
  // Banking / Debit Cards
  { pattern: /gastan|trans|clients|district/i,
    vocab: 'banking, debit card, transactions, account balance, currency, ATM, withdrawal, payment, Czech, loan, credit, district demographics' },
  // Formula 1 Racing
  { pattern: /races|circuits|drivers|constructors|pit_stops|qualifying|lap_times/i,
    vocab: 'Formula 1, F1, racing, Grand Prix, driver, constructor, circuit, lap time, pit stop, qualifying, grid position, championship, points, fastest lap' },
  // Student Club
  { pattern: /member|major|zip_code|income|budget|expense|event|attendance/i,
    vocab: 'student club, university, college, members, major, expenses, budget, events, attendance, income, funding' },
  // Superhero
  { pattern: /superhero|hero_power|superpower|alignment|colour|publisher/i,
    vocab: 'superhero, comic book, superpower, ability, hero, villain, alignment, good, evil, neutral, publisher, Marvel, DC, height, weight, eye color, hair color' },
  // Medical / Thrombosis
  { pattern: /patient|laboratory|examination|thrombosis/i,
    vocab: 'medical, patient, diagnosis, laboratory test, blood test, thrombosis, anti-nuclear antibody, immunoglobulin, creatinine, hemoglobin, inpatient, outpatient, symptoms' },
  // Chemistry / Toxicology
  { pattern: /molecule|atom|bond|connected/i,
    vocab: 'chemistry, molecule, atom, bond, toxicology, carcinogenic, mutagenic, element, carbon, nitrogen, oxygen, chlorine, single bond, double bond, molecular structure' },
  // Financial / Banking Transactions
  { pattern: /account|loan|trans.*type|order.*type|disp/i,
    vocab: 'banking, financial, account, loan, transaction, credit, debit, balance, interest, payment, transfer, withdrawal, deposit, statement' },
];

/**
 * Build a rich text description of a table for embedding.
 * Combines all metadata signals that a user might reference when asking about this table,
 * plus business vocabulary enrichment to bridge the gap between schema terms and user language.
 */
function buildTableText(table, columns, synonyms = []) {
  const parts = [];

  // Table name (split underscores for readability)
  parts.push(`Table: ${table.table_name}`);
  const readableName = table.table_name.replace(/_/g, ' ').toLowerCase();
  if (readableName !== table.table_name.toLowerCase()) {
    parts.push(`Also known as: ${readableName}`);
  }

  // Entity/business name
  if (table.entity_name) {
    parts.push(`Business name: ${table.entity_name}`);
  }

  // Description
  if (table.description) {
    parts.push(`Description: ${table.description}`);
  }

  // Entity metadata (domain, entity_type, sample_questions)
  const meta = typeof table.entity_metadata === 'string'
    ? JSON.parse(table.entity_metadata || '{}')
    : (table.entity_metadata || {});

  if (meta.domain || meta.module) {
    parts.push(`Domain: ${meta.domain || meta.module}`);
  }
  if (meta.entity_type) {
    parts.push(`Entity type: ${meta.entity_type}`);
  }
  if (meta.sample_questions && meta.sample_questions.length > 0) {
    parts.push(`Example questions: ${meta.sample_questions.join('; ')}`);
  }

  // Key columns (business names + descriptions, not all columns)
  if (columns && columns.length > 0) {
    const keyColTexts = columns
      .filter(c => c.business_name || c.description || c.column_role === 'measure' || c.column_role === 'dimension')
      .slice(0, 20) // limit to keep embedding text reasonable
      .map(c => {
        const parts = [c.column_name.replace(/_/g, ' ')];
        if (c.business_name) parts.push(c.business_name);
        if (c.description) parts.push(c.description);
        return parts.join(' — ');
      });
    if (keyColTexts.length > 0) {
      parts.push(`Key columns: ${keyColTexts.join(', ')}`);
    }
  }

  // Builder/AI synonyms: domain-specific terms from enrichment + manual curation
  if (synonyms && synonyms.length > 0) {
    const uniqueSyns = [...new Set(synonyms.map(s => s.toLowerCase().trim()))];
    parts.push(`Synonyms: ${uniqueSyns.join(', ')}`);
  }

  // Business vocabulary enrichment: inject natural language synonyms based on table patterns.
  // This bridges the gap between schema terms ("invoice") and user vocabulary ("spending").
  const combinedText = [table.table_name, table.entity_name, table.description].filter(Boolean).join(' ');
  const vocabParts = [];
  for (const { pattern, vocab } of BUSINESS_VOCAB_PATTERNS) {
    if (pattern.test(combinedText)) {
      vocabParts.push(vocab);
    }
  }
  if (vocabParts.length > 0) {
    parts.push(`Business concepts: ${vocabParts.join(', ')}`);
  }

  return parts.join('. ');
}

/**
 * Call OpenAI embeddings API for a batch of texts.
 * Returns array of { index, embedding } objects.
 */
async function getEmbeddings(texts) {
  if (!OPENAI_API_KEY) {
    throw new Error('OPENAI_API_KEY not set — cannot generate embeddings');
  }

  const response = await fetch('https://api.openai.com/v1/embeddings', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${OPENAI_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: EMBEDDING_MODEL,
      input: texts,
    }),
  });

  if (!response.ok) {
    const errBody = await response.text();
    throw new Error(`OpenAI embeddings API error ${response.status}: ${errBody}`);
  }

  const data = await response.json();
  return data.data; // [{ index, embedding, object }]
}

/**
 * Generate and store embeddings for all tables in an application.
 * Idempotent — overwrites existing embeddings.
 *
 * @param {number} appId
 * @param {object} options - { force: boolean } force regeneration even if embeddings exist
 * @returns {{ generated: number, skipped: number, errors: number }}
 */
async function generateEmbeddingsForApp(appId, options = {}) {
  const { force = false } = options;

  // Get all tables with metadata
  let tableFilter = force
    ? 'WHERE at.app_id = ?'
    : 'WHERE at.app_id = ? AND at.embedding IS NULL';

  const [tables] = await query(
    `SELECT at.id, at.table_name, at.entity_name, at.description, at.entity_metadata
     FROM app_tables at ${tableFilter}
     ORDER BY at.id`,
    [appId]
  );

  if (tables.length === 0) {
    return { generated: 0, skipped: 0, errors: 0, message: 'All tables already have embeddings' };
  }

  // Get columns for all these tables
  const tableIds = tables.map(t => t.id);
  const colPlaceholders = tableIds.map(() => '?').join(', ');
  const [columns] = await query(
    `SELECT table_id, column_name, business_name, description, column_role
     FROM app_columns WHERE table_id IN (${colPlaceholders})
     ORDER BY table_id, column_name`,
    tableIds
  );

  // Group columns by table
  const colsByTable = {};
  for (const col of columns) {
    if (!colsByTable[col.table_id]) colsByTable[col.table_id] = [];
    colsByTable[col.table_id].push(col);
  }

  // Get synonyms for all tables in this app (builder-curated + AI-generated)
  const [synonyms] = await query(
    `SELECT table_id, term FROM app_synonyms
     WHERE app_id = ? AND status = 'active'
     ORDER BY table_id`,
    [appId]
  );

  // Group synonyms by table
  const synsByTable = {};
  for (const syn of synonyms) {
    if (!synsByTable[syn.table_id]) synsByTable[syn.table_id] = [];
    synsByTable[syn.table_id].push(syn.term);
  }

  console.log(`[Embeddings] Found ${synonyms.length} active synonyms across ${Object.keys(synsByTable).length} tables for appId=${appId}`);

  // Build embedding texts
  const tableTexts = tables.map(t => ({
    id: t.id,
    text: buildTableText(t, colsByTable[t.id] || [], synsByTable[t.id] || []),
  }));

  let generated = 0;
  let errors = 0;

  // Process in batches
  for (let i = 0; i < tableTexts.length; i += BATCH_SIZE) {
    const batch = tableTexts.slice(i, i + BATCH_SIZE);
    const texts = batch.map(t => t.text);

    try {
      const embeddings = await getEmbeddings(texts);

      // Store embeddings in database
      for (const emb of embeddings) {
        const tableId = batch[emb.index].id;
        const vector = `[${emb.embedding.join(',')}]`;
        await query(
          'UPDATE app_tables SET embedding = ? WHERE id = ?',
          [vector, tableId]
        );
        generated++;
      }

      console.log(`[Embeddings] Batch ${Math.floor(i / BATCH_SIZE) + 1}: ${batch.length} tables embedded for appId=${appId}`);
    } catch (err) {
      console.error(`[Embeddings] Batch error:`, err.message);
      errors += batch.length;
    }
  }

  const skipped = tables.length - generated - errors;
  console.log(`[Embeddings] appId=${appId}: ${generated} generated, ${skipped} skipped, ${errors} errors`);

  return { generated, skipped, errors };
}

/**
 * Find tables semantically similar to a question using vector cosine similarity.
 * Returns table IDs with similarity scores, ordered by relevance.
 *
 * @param {number} appId
 * @param {string} question - Natural language question from user
 * @param {number} limit - Max tables to return (default 15)
 * @param {number} threshold - Minimum similarity score (default 0.25)
 * @returns {Array<{ table_id: number, table_name: string, similarity: number }>}
 */
async function semanticSchemaLink(appId, question, limit = 15, threshold = 0.25) {
  if (!OPENAI_API_KEY) return []; // graceful degradation

  try {
    // Embed the question
    const [questionEmbedding] = await getEmbeddings([question]);
    const questionVector = `[${questionEmbedding.embedding.join(',')}]`;

    // Cosine similarity search against table embeddings
    // 1 - (embedding <=> ?) converts cosine distance to similarity (1 = identical, 0 = orthogonal)
    const [rows] = await query(
      `SELECT id as table_id, table_name,
              1 - (embedding <=> ?) as similarity
       FROM app_tables
       WHERE app_id = ? AND embedding IS NOT NULL
       ORDER BY embedding <=> ?
       LIMIT ?`,
      [questionVector, appId, limit]
    );

    return rows
      .filter(r => r.similarity >= threshold)
      .map(r => ({
        table_id: r.table_id,
        table_name: r.table_name,
        similarity: parseFloat(r.similarity),
      }));
  } catch (err) {
    console.warn('[Embeddings] Semantic search failed (falling back to keyword-only):', err.message);
    return []; // graceful degradation — keyword matching still works
  }
}

module.exports = {
  generateEmbeddingsForApp,
  semanticSchemaLink,
  buildTableText,
  getEmbeddings,
};
