/**
 * Synonym & Ontology Management Routes
 *
 * Endpoints:
 *   GET    /api/synonyms/:appId          — List all synonyms for app (filters: status, source)
 *   POST   /api/synonyms/:appId          — Create synonym
 *   PUT    /api/synonyms/:appId/:id      — Update synonym (term, status)
 *   DELETE /api/synonyms/:appId/:id      — Delete synonym
 *   POST   /api/synonyms/:appId/generate — AI-generate synonyms for unmapped columns
 *   POST   /api/synonyms/:appId/apply-global — Apply global ontology + domain packs
 */

const express = require('express');
const router = express.Router();
const { query } = require('../db');

// ─── GET /api/synonyms/:appId/tables — Tables for the Add Synonym form ───
router.get('/:appId/tables', async (req, res) => {
  try {
    const { appId } = req.params;
    const result = await query(
      `SELECT id, table_name, entity_name FROM app_tables WHERE app_id = $1 ORDER BY COALESCE(entity_name, table_name)`,
      [appId]
    );
    res.json({ tables: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /api/synonyms/:appId/tables/:tableId/columns — Columns for Add Synonym form ───
router.get('/:appId/tables/:tableId/columns', async (req, res) => {
  try {
    const { tableId } = req.params;
    const result = await query(
      `SELECT id, column_name, business_name FROM app_columns WHERE table_id = $1 ORDER BY COALESCE(business_name, column_name)`,
      [tableId]
    );
    res.json({ columns: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /api/synonyms/packs — List available domain packs with term counts ───
router.get('/packs', async (req, res) => {
  try {
    const result = await query(`
      SELECT
        COALESCE(domain_pack, 'universal') as pack_name,
        COUNT(*) as term_count,
        array_agg(DISTINCT category) as categories
      FROM global_synonyms
      GROUP BY domain_pack
      ORDER BY domain_pack NULLS LAST
    `);

    const packDescriptions = {
      'healthcare': 'Medical terminology — lab values, CBC, coagulation, autoimmune markers, diagnoses. Maps abbreviations like GOT, PLT, T-BIL to their full clinical names.',
      'universal': 'Universal ERP/Finance terms — procurement, GL accounts, inventory, HR. Applies across all enterprise applications.',
      'erp': 'Oracle EBS terminology — AP invoices, AR receipts, GL journals, PO procurement, Order Management. Maps business vocabulary like "bookings", "vendor bill", "debit amount" to Oracle EBS table/column names.',
      'erp-sap': 'SAP ECC 6.0 terminology — SD sales orders, FI general ledger, MM purchasing, CO controlling, HR payroll. Maps business vocabulary like "bookings", "spend", "revenue" to SAP table/column names (VBAK, EKKO, BSIS, etc.).',
    };

    const packs = result.rows.map(r => ({
      name: r.pack_name,
      label: r.pack_name === 'universal' ? 'Universal ERP / Finance' : r.pack_name.charAt(0).toUpperCase() + r.pack_name.slice(1),
      termCount: parseInt(r.term_count),
      categories: r.categories.filter(Boolean),
      description: packDescriptions[r.pack_name] || `${r.pack_name} domain synonym pack`,
    }));

    res.json({ packs });
  } catch (err) {
    console.error('GET packs error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── GET /api/synonyms/:appId — List synonyms with summary ───
router.get('/:appId', async (req, res) => {
  try {
    const { appId } = req.params;
    const { status, source, tableId, columnId } = req.query;

    let sql = `
      SELECT s.*,
             c.column_name, c.business_name,
             t.table_name, t.entity_name
      FROM app_synonyms s
      LEFT JOIN app_columns c ON s.column_id = c.id
      LEFT JOIN app_tables t ON s.table_id = t.id
      WHERE s.app_id = $1
    `;
    const params = [appId];
    let idx = 2;

    if (status) { sql += ` AND s.status = $${idx++}`; params.push(status); }
    if (source) { sql += ` AND s.source = $${idx++}`; params.push(source); }
    if (tableId) { sql += ` AND s.table_id = $${idx++}`; params.push(tableId); }
    if (columnId) { sql += ` AND s.column_id = $${idx++}`; params.push(columnId); }

    sql += ` ORDER BY s.created_at DESC`;

    const result = await query(sql, params);

    // Build summary
    const synonyms = result.rows;
    const summary = {
      total: synonyms.length,
      active: synonyms.filter(s => s.status === 'active').length,
      pending_review: synonyms.filter(s => s.status === 'pending_review').length,
      rejected: synonyms.filter(s => s.status === 'rejected').length,
      by_source: {},
    };
    for (const s of synonyms) {
      summary.by_source[s.source] = (summary.by_source[s.source] || 0) + 1;
    }

    res.json({ synonyms, summary });
  } catch (err) {
    console.error('GET synonyms error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /api/synonyms/:appId — Create synonym ───
router.post('/:appId', async (req, res) => {
  try {
    const { appId } = req.params;
    const { term, columnId, tableId, source, confidenceScore } = req.body;

    if (!term || !term.trim()) {
      return res.status(400).json({ error: 'term is required' });
    }

    const result = await query(
      `INSERT INTO app_synonyms (app_id, column_id, table_id, term, source, confidence_score, status, created_by)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (app_id, column_id, term) DO UPDATE SET
         source = EXCLUDED.source,
         confidence_score = EXCLUDED.confidence_score,
         status = EXCLUDED.status,
         updated_at = NOW()
       RETURNING *`,
      [
        appId,
        columnId || null,
        tableId || null,
        term.trim(),
        source || 'builder_curated',
        confidenceScore || 95,
        source === 'ai_generated' ? 'pending_review' : 'active',
        req.user?.id || null,
      ]
    );

    res.json({ synonym: result.rows[0] });
  } catch (err) {
    console.error('POST synonym error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── DELETE /api/synonyms/:appId/all — Clear all synonyms for app (for re-apply) ───
router.delete('/:appId/all', async (req, res) => {
  try {
    const { appId } = req.params;
    const result = await query('DELETE FROM app_synonyms WHERE app_id = $1', [appId]);
    res.json({ deleted: result.rowCount, message: `Cleared all synonyms for app ${appId}` });
  } catch (err) {
    console.error('DELETE all synonyms error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── PUT /api/synonyms/:appId/:id — Update synonym ───
router.put('/:appId/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { term, status, confidenceScore } = req.body;

    const sets = [];
    const params = [];
    let idx = 1;

    if (term !== undefined) { sets.push(`term = $${idx++}`); params.push(term); }
    if (status !== undefined) { sets.push(`status = $${idx++}`); params.push(status); }
    if (confidenceScore !== undefined) { sets.push(`confidence_score = $${idx++}`); params.push(confidenceScore); }
    sets.push(`updated_at = NOW()`);

    params.push(id);
    const result = await query(
      `UPDATE app_synonyms SET ${sets.join(', ')} WHERE id = $${idx} RETURNING *`,
      params
    );

    res.json({ synonym: result.rows[0] });
  } catch (err) {
    console.error('PUT synonym error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── DELETE /api/synonyms/:appId/:id — Delete synonym ───
router.delete('/:appId/:id', async (req, res) => {
  try {
    const { id } = req.params;
    await query('DELETE FROM app_synonyms WHERE id = $1', [id]);
    res.json({ deleted: true });
  } catch (err) {
    console.error('DELETE synonym error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /api/synonyms/:appId/generate — AI-generate synonyms ───
router.post('/:appId/generate', async (req, res) => {
  try {
    const { appId } = req.params;

    // Get columns that don't have synonyms yet
    const cols = await query(`
      SELECT c.id as column_id, c.column_name, c.business_name, c.data_type, c.description,
             t.id as table_id, t.table_name, t.entity_name
      FROM app_columns c
      JOIN app_tables t ON c.table_id = t.id
      WHERE t.app_id = $1
        AND c.id NOT IN (SELECT DISTINCT column_id FROM app_synonyms WHERE app_id = $1 AND column_id IS NOT NULL)
      ORDER BY t.table_name, c.column_name
      LIMIT 500
    `, [appId]);

    if (cols.rows.length === 0) {
      return res.json({ generated: 0, message: 'All columns already have synonyms' });
    }

    // Generate synonyms from business names and descriptions
    let generated = 0;
    for (const col of cols.rows) {
      const synonymTerms = [];

      // Extract from business_name if different from column_name
      if (col.business_name && col.business_name.toLowerCase() !== col.column_name.toLowerCase()) {
        synonymTerms.push(col.business_name);
      }

      // Extract from description "Also known as: X, Y" patterns
      if (col.description) {
        const akaMatch = col.description.match(/(?:also known as|aka|aliases?|synonyms?)[:\s]+([^.]+)/i);
        if (akaMatch) {
          const terms = akaMatch[1].split(/[,;]/).map(t => t.trim()).filter(t => t.length > 1);
          synonymTerms.push(...terms);
        }
      }

      // Generate common synonyms from column name patterns
      // Fix: use word-boundary matching (split on _ / space / -) instead of substring .includes()
      const name = (col.business_name || col.column_name).toLowerCase();
      const nameParts = name.split(/[_\s-]/);
      if (nameParts.includes('amount') || nameParts.includes('amt')) synonymTerms.push('amount', 'value', 'sum');
      if (nameParts.includes('date') || nameParts.includes('dt')) synonymTerms.push('date', 'when');
      if (nameParts.includes('quantity') || nameParts.includes('qty')) synonymTerms.push('quantity', 'count', 'number');
      if (nameParts.includes('status') || nameParts.includes('sts')) synonymTerms.push('status', 'state');
      if (nameParts.includes('name') || nameParts.includes('nm')) synonymTerms.push('name', 'title');
      if (nameParts.includes('description') || nameParts.includes('desc')) synonymTerms.push('description', 'details');
      if (nameParts.includes('code') || nameParts.includes('cd')) synonymTerms.push('code', 'identifier');
      if (nameParts.includes('price') || nameParts.includes('prc')) synonymTerms.push('price', 'cost', 'rate');

      // Block dangerous generic bare terms that cause mass ambiguity
      const BLOCKED_BARE_TERMS = new Set([
        'date', 'when', 'name', 'title', 'id', 'type', 'status', 'state',
        'code', 'number', 'value', 'amount', 'description', 'count', 'sum',
        'details', 'identifier', 'cost', 'rate', 'price', 'quantity'
      ]);

      // Deduplicate, skip terms that match column/business name, and block bare generic terms
      const colNameLower = col.column_name.toLowerCase().replace(/[_\s-]/g, ' ').trim();
      const bizNameLower = (col.business_name || '').toLowerCase().trim();
      const unique = [...new Set(synonymTerms.map(t => t.toLowerCase()))]
        .filter(t => {
          // Skip if synonym is essentially the column name or business name
          if (t === colNameLower || t === bizNameLower) return false;
          // Block bare generic terms — only allow if they're part of a compound (2+ words)
          if (BLOCKED_BARE_TERMS.has(t) && !t.includes(' ')) return false;
          return true;
        });
      for (const term of unique) {
        try {
          await query(
            `INSERT INTO app_synonyms (app_id, column_id, table_id, term, source, confidence_score, status)
             VALUES ($1, $2, $3, $4, 'ai_generated', 75, 'pending_review')
             ON CONFLICT DO NOTHING`,
            [appId, col.column_id, col.table_id, term]
          );
          generated++;
        } catch (e) { /* skip duplicates */ }
      }
    }

    res.json({ generated, message: `Generated ${generated} synonym suggestions for review` });
  } catch (err) {
    console.error('Generate synonyms error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /api/synonyms/:appId/apply-global — Apply global ontology ───
router.post('/:appId/apply-global', async (req, res) => {
  try {
    const { appId } = req.params;
    const { packs } = req.body || {}; // Optional: array of pack names to apply, e.g. ['healthcare', 'universal']

    // Build query based on selected packs
    let globalsSql = 'SELECT * FROM global_synonyms';
    const globalsParams = [];
    if (packs && Array.isArray(packs) && packs.length > 0) {
      const conditions = [];
      let idx = 1;
      for (const pack of packs) {
        if (pack === 'universal') {
          conditions.push('domain_pack IS NULL');
        } else {
          conditions.push(`domain_pack = $${idx++}`);
          globalsParams.push(pack);
        }
      }
      globalsSql += ' WHERE ' + conditions.join(' OR ');
    }
    globalsSql += ' ORDER BY domain_pack NULLS FIRST, term';

    const globals = await query(globalsSql, globalsParams);
    if (globals.rows.length === 0) {
      return res.json({ applied: 0, message: 'No global ontology terms found for the selected packs.' });
    }

    // Get all columns for this app
    const appCols = await query(`
      SELECT c.id as column_id, c.column_name, c.business_name, c.description,
             t.id as table_id, t.table_name, t.entity_name
      FROM app_columns c
      JOIN app_tables t ON c.table_id = t.id
      WHERE t.app_id = $1
    `, [appId]);

    let applied = 0;
    for (const g of globals.rows) {
      const canonical = g.canonical_name.toLowerCase();
      const canonicalUnderscore = canonical.replace(/[\s-]+/g, '_');
      const canonicalNoSpace = canonical.replace(/[\s_-]+/g, '');

      // Strict matching with ambiguity controls
      // Common columns that exist in nearly every table — only allow exact match on FIRST relevant table
      const UBIQUITOUS_COLUMNS = new Set([
        'creation_date', 'created_by', 'last_update_date', 'last_updated_by',
        'org_id', 'request_id', 'program_id', 'program_application_id',
        'program_update_date', 'attribute_category',
      ]);

      // Check if canonical matches a table name (table-level synonym)
      const allTableNames = new Set(appCols.rows.map(c => (c.table_name || '').toLowerCase()));
      const isTableCanonical = allTableNames.has(canonicalUnderscore);

      if (isTableCanonical) {
        // Table-level match: insert ONE synonym per matching table (column_id = NULL)
        const matchingTableIds = new Set();
        for (const col of appCols.rows) {
          const tableName = (col.table_name || '').toLowerCase();
          if (tableName === canonicalUnderscore && !matchingTableIds.has(col.table_id)) {
            matchingTableIds.add(col.table_id);
            try {
              await query(
                `INSERT INTO app_synonyms (app_id, column_id, table_id, term, source, confidence_score, status, global_synonym_id)
                 VALUES ($1, NULL, $2, $3, $4, $5, 'active', $6)
                 ON CONFLICT DO NOTHING`,
                [
                  appId,
                  col.table_id,
                  g.term,
                  g.domain_pack ? 'domain_pack' : 'solix_global',
                  g.domain_pack ? 90 : 95,
                  g.id,
                ]
              );
              applied++;
            } catch (e) { /* skip duplicates */ }
          }
        }
        continue; // Skip column-level matching for table canonicals
      }

      // Column-level matching
      const matched = appCols.rows.filter(col => {
        const colName = (col.column_name || '').toLowerCase();
        const bizName = (col.business_name || '').toLowerCase();
        const colNoUnderscore = colName.replace(/_/g, '');

        // Skip ubiquitous columns (CREATION_DATE etc.) — they match everywhere and add noise
        if (UBIQUITOUS_COLUMNS.has(colName)) return false;

        // Exact column name match
        if (colName === canonical || colName === canonicalUnderscore) return true;
        // Exact business name match
        if (bizName === canonical || bizName === canonicalUnderscore) return true;
        // Column name without separators matches canonical without separators
        if (canonicalNoSpace.length >= 3 && colNoUnderscore === canonicalNoSpace) return true;
        // Business name is an exact phrase match
        if (bizName && canonical.length >= 3 && bizName === canonical) return true;

        return false;
      });

      // Ambiguity cap: if a canonical matches more than 3 columns, limit to first 3
      // (sorted by table relevance — prefer tables with names starting with module prefix)
      const maxMatches = 3;
      const finalMatched = matched.length > maxMatches ? matched.slice(0, maxMatches) : matched;

      for (const col of finalMatched) {
        try {
          await query(
            `INSERT INTO app_synonyms (app_id, column_id, table_id, term, source, confidence_score, status, global_synonym_id)
             VALUES ($1, $2, $3, $4, $5, $6, 'active', $7)
             ON CONFLICT DO NOTHING`,
            [
              appId,
              col.column_id,
              col.table_id,
              g.term,
              g.domain_pack ? 'domain_pack' : 'solix_global',
              g.domain_pack ? 90 : 95,
              g.id,
            ]
          );
          applied++;
        } catch (e) { /* skip duplicates */ }
      }
    }

    res.json({ applied, message: `Applied ${applied} global ontology synonyms to matching columns` });
  } catch (err) {
    console.error('Apply global error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /api/synonyms/admin/seed-erp — One-time seed of ERP domain pack into global_synonyms ───
router.post('/admin/seed-erp', async (req, res) => {
  try {
    const erpPack = [
      // ACCOUNTS PAYABLE
      { term: 'invoice', canonical_name: 'AP_INVOICES_ALL', category: 'Accounts Payable' },
      { term: 'bill', canonical_name: 'AP_INVOICES_ALL', category: 'Accounts Payable' },
      { term: 'vendor bill', canonical_name: 'AP_INVOICES_ALL', category: 'Accounts Payable' },
      { term: 'invoice number', canonical_name: 'INVOICE_NUM', category: 'Accounts Payable' },
      { term: 'bill number', canonical_name: 'INVOICE_NUM', category: 'Accounts Payable' },
      { term: 'invoice amount', canonical_name: 'INVOICE_AMOUNT', category: 'Accounts Payable' },
      { term: 'invoice total', canonical_name: 'INVOICE_AMOUNT', category: 'Accounts Payable' },
      { term: 'bill amount', canonical_name: 'INVOICE_AMOUNT', category: 'Accounts Payable' },
      { term: 'how much was billed', canonical_name: 'INVOICE_AMOUNT', category: 'Accounts Payable' },
      { term: 'billed amount', canonical_name: 'INVOICE_AMOUNT', category: 'Accounts Payable' },
      { term: 'gross amount', canonical_name: 'INVOICE_AMOUNT', category: 'Accounts Payable' },
      { term: 'tax amount', canonical_name: 'TAX_AMOUNT', category: 'Accounts Payable' },
      { term: 'sales tax', canonical_name: 'TAX_AMOUNT', category: 'Accounts Payable' },
      { term: 'freight charge', canonical_name: 'FREIGHT_AMOUNT', category: 'Accounts Payable' },
      { term: 'shipping cost', canonical_name: 'FREIGHT_AMOUNT', category: 'Accounts Payable' },
      { term: 'payment status', canonical_name: 'PAYMENT_STATUS_FLAG', category: 'Accounts Payable' },
      { term: 'paid', canonical_name: 'PAYMENT_STATUS_FLAG', category: 'Accounts Payable' },
      { term: 'unpaid', canonical_name: 'PAYMENT_STATUS_FLAG', category: 'Accounts Payable' },
      { term: 'is paid', canonical_name: 'PAYMENT_STATUS_FLAG', category: 'Accounts Payable' },
      { term: 'payment method', canonical_name: 'PAYMENT_METHOD_CODE', category: 'Accounts Payable' },
      { term: 'how was it paid', canonical_name: 'PAYMENT_METHOD_CODE', category: 'Accounts Payable' },
      { term: 'check', canonical_name: 'AP_CHECKS_ALL', category: 'Accounts Payable' },
      { term: 'payment', canonical_name: 'AP_CHECKS_ALL', category: 'Accounts Payable' },
      { term: 'check number', canonical_name: 'CHECK_NUMBER', category: 'Accounts Payable' },
      { term: 'payment number', canonical_name: 'CHECK_NUMBER', category: 'Accounts Payable' },
      { term: 'payment amount', canonical_name: 'AMOUNT', category: 'Accounts Payable' },
      { term: 'amount paid', canonical_name: 'AMOUNT_PAID', category: 'Accounts Payable' },
      { term: 'how much was paid', canonical_name: 'AMOUNT_PAID', category: 'Accounts Payable' },
      { term: 'vendor', canonical_name: 'AP_SUPPLIERS', category: 'Accounts Payable' },
      { term: 'supplier', canonical_name: 'AP_SUPPLIERS', category: 'Accounts Payable' },
      { term: 'vendor name', canonical_name: 'VENDOR_NAME', category: 'Accounts Payable' },
      { term: 'supplier name', canonical_name: 'VENDOR_NAME', category: 'Accounts Payable' },
      { term: 'who was the supplier', canonical_name: 'VENDOR_NAME', category: 'Accounts Payable' },
      { term: 'vendor id', canonical_name: 'VENDOR_ID', category: 'Accounts Payable' },
      { term: 'supplier id', canonical_name: 'VENDOR_ID', category: 'Accounts Payable' },
      { term: 'invoice date', canonical_name: 'INVOICE_DATE', category: 'Accounts Payable' },
      { term: 'when was it invoiced', canonical_name: 'INVOICE_DATE', category: 'Accounts Payable' },
      { term: 'billing date', canonical_name: 'INVOICE_DATE', category: 'Accounts Payable' },
      { term: 'payment date', canonical_name: 'CHECK_DATE', category: 'Accounts Payable' },
      { term: 'when was it paid', canonical_name: 'CHECK_DATE', category: 'Accounts Payable' },
      { term: 'gl date', canonical_name: 'GL_DATE', category: 'Accounts Payable' },
      { term: 'invoice line', canonical_name: 'AP_INVOICE_LINES_ALL', category: 'Accounts Payable' },
      { term: 'line item', canonical_name: 'AP_INVOICE_LINES_ALL', category: 'Accounts Payable' },
      { term: 'invoice type', canonical_name: 'INVOICE_TYPE_LOOKUP_CODE', category: 'Accounts Payable' },
      { term: 'discount taken', canonical_name: 'DISCOUNT_AMOUNT_TAKEN', category: 'Accounts Payable' },
      { term: 'early payment discount', canonical_name: 'DISCOUNT_AMOUNT_TAKEN', category: 'Accounts Payable' },
      { term: 'voucher number', canonical_name: 'VOUCHER_NUM', category: 'Accounts Payable' },
      { term: 'approval status', canonical_name: 'APPROVAL_STATUS', category: 'Accounts Payable' },
      { term: 'workflow status', canonical_name: 'WFAPPROVAL_STATUS', category: 'Accounts Payable' },
      // ACCOUNTS RECEIVABLE
      { term: 'customer transaction', canonical_name: 'RA_CUSTOMER_TRX_ALL', category: 'Accounts Receivable' },
      { term: 'ar transaction', canonical_name: 'RA_CUSTOMER_TRX_ALL', category: 'Accounts Receivable' },
      { term: 'transaction number', canonical_name: 'TRX_NUMBER', category: 'Accounts Receivable' },
      { term: 'transaction date', canonical_name: 'TRX_DATE', category: 'Accounts Receivable' },
      { term: 'customer invoice', canonical_name: 'RA_CUSTOMER_TRX_ALL', category: 'Accounts Receivable' },
      { term: 'cash receipt', canonical_name: 'AR_CASH_RECEIPTS_ALL', category: 'Accounts Receivable' },
      { term: 'receipt', canonical_name: 'AR_CASH_RECEIPTS_ALL', category: 'Accounts Receivable' },
      { term: 'receipt number', canonical_name: 'RECEIPT_NUMBER', category: 'Accounts Receivable' },
      { term: 'receipt amount', canonical_name: 'AMOUNT', category: 'Accounts Receivable' },
      { term: 'cash received', canonical_name: 'AMOUNT', category: 'Accounts Receivable' },
      { term: 'how much was received', canonical_name: 'AMOUNT', category: 'Accounts Receivable' },
      { term: 'receipt date', canonical_name: 'RECEIPT_DATE', category: 'Accounts Receivable' },
      { term: 'when was payment received', canonical_name: 'RECEIPT_DATE', category: 'Accounts Receivable' },
      { term: 'deposit date', canonical_name: 'DEPOSIT_DATE', category: 'Accounts Receivable' },
      { term: 'receipt status', canonical_name: 'STATUS', category: 'Accounts Receivable' },
      { term: 'customer id', canonical_name: 'PAY_FROM_CUSTOMER', category: 'Accounts Receivable' },
      { term: 'who is the customer', canonical_name: 'BILL_TO_CUSTOMER_ID', category: 'Accounts Receivable' },
      { term: 'bill to customer', canonical_name: 'BILL_TO_CUSTOMER_ID', category: 'Accounts Receivable' },
      { term: 'ship to customer', canonical_name: 'SHIP_TO_CUSTOMER_ID', category: 'Accounts Receivable' },
      { term: 'receipt currency', canonical_name: 'CURRENCY_CODE', category: 'Accounts Receivable' },
      { term: 'invoice currency', canonical_name: 'INVOICE_CURRENCY_CODE', category: 'Accounts Receivable' },
      { term: 'payment application', canonical_name: 'AR_RECEIVABLE_APPLICATIONS_ALL', category: 'Accounts Receivable' },
      { term: 'applied receipt', canonical_name: 'AR_RECEIVABLE_APPLICATIONS_ALL', category: 'Accounts Receivable' },
      { term: 'payment schedule', canonical_name: 'AR_PAYMENT_SCHEDULES_ALL', category: 'Accounts Receivable' },
      { term: 'amount due', canonical_name: 'AR_PAYMENT_SCHEDULES_ALL', category: 'Accounts Receivable' },
      { term: 'salesperson', canonical_name: 'PRIMARY_SALESREP_ID', category: 'Accounts Receivable' },
      { term: 'sales rep', canonical_name: 'PRIMARY_SALESREP_ID', category: 'Accounts Receivable' },
      { term: 'customer po number', canonical_name: 'PURCHASE_ORDER', category: 'Accounts Receivable' },
      // GENERAL LEDGER
      { term: 'journal entry', canonical_name: 'GL_JE_HEADERS', category: 'General Ledger' },
      { term: 'journal', canonical_name: 'GL_JE_HEADERS', category: 'General Ledger' },
      { term: 'je', canonical_name: 'GL_JE_HEADERS', category: 'General Ledger' },
      { term: 'journal name', canonical_name: 'NAME', category: 'General Ledger' },
      { term: 'journal entry name', canonical_name: 'NAME', category: 'General Ledger' },
      { term: 'journal line', canonical_name: 'GL_JE_LINES', category: 'General Ledger' },
      { term: 'je line', canonical_name: 'GL_JE_LINES', category: 'General Ledger' },
      { term: 'debit', canonical_name: 'ENTERED_DR', category: 'General Ledger' },
      { term: 'debit amount', canonical_name: 'ENTERED_DR', category: 'General Ledger' },
      { term: 'credit', canonical_name: 'ENTERED_CR', category: 'General Ledger' },
      { term: 'credit amount', canonical_name: 'ENTERED_CR', category: 'General Ledger' },
      { term: 'accounted debit', canonical_name: 'ACCOUNTED_DR', category: 'General Ledger' },
      { term: 'accounted credit', canonical_name: 'ACCOUNTED_CR', category: 'General Ledger' },
      { term: 'gl account', canonical_name: 'CODE_COMBINATION_ID', category: 'General Ledger' },
      { term: 'chart of accounts', canonical_name: 'CODE_COMBINATION_ID', category: 'General Ledger' },
      { term: 'account code', canonical_name: 'CODE_COMBINATION_ID', category: 'General Ledger' },
      { term: 'company segment', canonical_name: 'SEGMENT1', category: 'General Ledger' },
      { term: 'cost center', canonical_name: 'SEGMENT2', category: 'General Ledger' },
      { term: 'department', canonical_name: 'SEGMENT2', category: 'General Ledger' },
      { term: 'natural account', canonical_name: 'SEGMENT3', category: 'General Ledger' },
      { term: 'accounting period', canonical_name: 'PERIOD_NAME', category: 'General Ledger' },
      { term: 'fiscal period', canonical_name: 'PERIOD_NAME', category: 'General Ledger' },
      { term: 'gl period', canonical_name: 'PERIOD_NAME', category: 'General Ledger' },
      { term: 'accounting date', canonical_name: 'EFFECTIVE_DATE', category: 'General Ledger' },
      { term: 'je date', canonical_name: 'EFFECTIVE_DATE', category: 'General Ledger' },
      { term: 'posted date', canonical_name: 'POSTED_DATE', category: 'General Ledger' },
      { term: 'je source', canonical_name: 'JE_SOURCE', category: 'General Ledger' },
      { term: 'journal source', canonical_name: 'JE_SOURCE', category: 'General Ledger' },
      { term: 'je category', canonical_name: 'JE_CATEGORY', category: 'General Ledger' },
      { term: 'journal category', canonical_name: 'JE_CATEGORY', category: 'General Ledger' },
      { term: 'journal status', canonical_name: 'STATUS', category: 'General Ledger' },
      { term: 'gl balance', canonical_name: 'GL_BALANCES', category: 'General Ledger' },
      { term: 'account balance', canonical_name: 'GL_BALANCES', category: 'General Ledger' },
      { term: 'ledger', canonical_name: 'GL_LEDGERS', category: 'General Ledger' },
      { term: 'set of books', canonical_name: 'GL_SETS_OF_BOOKS', category: 'General Ledger' },
      { term: 'total debits', canonical_name: 'RUNNING_TOTAL_DR', category: 'General Ledger' },
      { term: 'total credits', canonical_name: 'RUNNING_TOTAL_CR', category: 'General Ledger' },
      // PURCHASING
      { term: 'purchase order', canonical_name: 'PO_HEADERS_ALL', category: 'Purchasing' },
      { term: 'po', canonical_name: 'PO_HEADERS_ALL', category: 'Purchasing' },
      { term: 'po header', canonical_name: 'PO_HEADERS_ALL', category: 'Purchasing' },
      { term: 'po number', canonical_name: 'SEGMENT1', category: 'Purchasing' },
      { term: 'purchase order number', canonical_name: 'SEGMENT1', category: 'Purchasing' },
      { term: 'po type', canonical_name: 'TYPE_LOOKUP_CODE', category: 'Purchasing' },
      { term: 'purchase order type', canonical_name: 'TYPE_LOOKUP_CODE', category: 'Purchasing' },
      { term: 'po line', canonical_name: 'PO_LINES_ALL', category: 'Purchasing' },
      { term: 'purchase order line', canonical_name: 'PO_LINES_ALL', category: 'Purchasing' },
      { term: 'unit price', canonical_name: 'UNIT_PRICE', category: 'Purchasing' },
      { term: 'price per unit', canonical_name: 'UNIT_PRICE', category: 'Purchasing' },
      { term: 'how much per unit', canonical_name: 'UNIT_PRICE', category: 'Purchasing' },
      { term: 'ordered quantity', canonical_name: 'QUANTITY', category: 'Purchasing' },
      { term: 'how many were ordered', canonical_name: 'QUANTITY', category: 'Purchasing' },
      { term: 'line amount', canonical_name: 'AMOUNT', category: 'Purchasing' },
      { term: 'po line amount', canonical_name: 'AMOUNT', category: 'Purchasing' },
      { term: 'item description', canonical_name: 'ITEM_DESCRIPTION', category: 'Purchasing' },
      { term: 'what was ordered', canonical_name: 'ITEM_DESCRIPTION', category: 'Purchasing' },
      { term: 'unit of measure', canonical_name: 'UNIT_MEAS_LOOKUP_CODE', category: 'Purchasing' },
      { term: 'supplier part number', canonical_name: 'VENDOR_PRODUCT_NUM', category: 'Purchasing' },
      { term: 'requisition', canonical_name: 'PO_REQUISITION_HEADERS_ALL', category: 'Purchasing' },
      { term: 'purchase requisition', canonical_name: 'PO_REQUISITION_HEADERS_ALL', category: 'Purchasing' },
      { term: 'req', canonical_name: 'PO_REQUISITION_HEADERS_ALL', category: 'Purchasing' },
      { term: 'requisition line', canonical_name: 'PO_REQUISITION_LINES_ALL', category: 'Purchasing' },
      { term: 'po approval status', canonical_name: 'AUTHORIZATION_STATUS', category: 'Purchasing' },
      { term: 'po approved', canonical_name: 'APPROVED_FLAG', category: 'Purchasing' },
      { term: 'approval date', canonical_name: 'APPROVED_DATE', category: 'Purchasing' },
      { term: 'when was po approved', canonical_name: 'APPROVED_DATE', category: 'Purchasing' },
      { term: 'po cancelled', canonical_name: 'CANCEL_FLAG', category: 'Purchasing' },
      { term: 'po closed', canonical_name: 'CLOSED_CODE', category: 'Purchasing' },
      { term: 'buyer', canonical_name: 'AGENT_ID', category: 'Purchasing' },
      { term: 'purchasing agent', canonical_name: 'AGENT_ID', category: 'Purchasing' },
      { term: 'who placed the order', canonical_name: 'AGENT_ID', category: 'Purchasing' },
      { term: 'po creation date', canonical_name: 'CREATION_DATE', category: 'Purchasing' },
      { term: 'when was po created', canonical_name: 'CREATION_DATE', category: 'Purchasing' },
      { term: 'blanket amount', canonical_name: 'BLANKET_TOTAL_AMOUNT', category: 'Purchasing' },
      { term: 'blanket po total', canonical_name: 'BLANKET_TOTAL_AMOUNT', category: 'Purchasing' },
      { term: 'po distribution', canonical_name: 'PO_DISTRIBUTIONS_ALL', category: 'Purchasing' },
      { term: 'charge account', canonical_name: 'PO_DISTRIBUTIONS_ALL', category: 'Purchasing' },
      { term: 'approved supplier', canonical_name: 'PO_APPROVED_SUPPLIER_LIST', category: 'Purchasing' },
      { term: 'qualified vendor', canonical_name: 'PO_APPROVED_SUPPLIER_LIST', category: 'Purchasing' },
      // ORDER MANAGEMENT
      { term: 'sales order', canonical_name: 'OE_ORDER_HEADERS_ALL', category: 'Order Management' },
      { term: 'order', canonical_name: 'OE_ORDER_HEADERS_ALL', category: 'Order Management' },
      { term: 'customer order', canonical_name: 'OE_ORDER_HEADERS_ALL', category: 'Order Management' },
      { term: 'order number', canonical_name: 'ORDER_NUMBER', category: 'Order Management' },
      { term: 'order date', canonical_name: 'ORDERED_DATE', category: 'Order Management' },
      { term: 'when was order placed', canonical_name: 'ORDERED_DATE', category: 'Order Management' },
      { term: 'booked date', canonical_name: 'BOOKED_DATE', category: 'Order Management' },
      { term: 'when was order booked', canonical_name: 'BOOKED_DATE', category: 'Order Management' },
      { term: 'booking date', canonical_name: 'BOOKED_DATE', category: 'Order Management' },
      { term: 'bookings', canonical_name: 'OE_ORDER_HEADERS_ALL', category: 'Order Management' },
      { term: 'order status', canonical_name: 'FLOW_STATUS_CODE', category: 'Order Management' },
      { term: 'flow status', canonical_name: 'FLOW_STATUS_CODE', category: 'Order Management' },
      { term: 'is order open', canonical_name: 'OPEN_FLAG', category: 'Order Management' },
      { term: 'customer po', canonical_name: 'CUST_PO_NUMBER', category: 'Order Management' },
      { term: 'customer purchase order', canonical_name: 'CUST_PO_NUMBER', category: 'Order Management' },
      { term: 'shipping method', canonical_name: 'SHIPPING_METHOD_CODE', category: 'Order Management' },
      { term: 'freight terms', canonical_name: 'FREIGHT_TERMS_CODE', category: 'Order Management' },
      { term: 'order category', canonical_name: 'ORDER_CATEGORY_CODE', category: 'Order Management' },
      { term: 'order cancelled', canonical_name: 'CANCELLED_FLAG', category: 'Order Management' },
      { term: 'order line', canonical_name: 'OE_ORDER_LINES_ALL', category: 'Order Management' },
      { term: 'sales order line', canonical_name: 'OE_ORDER_LINES_ALL', category: 'Order Management' },
      { term: 'ordered quantity', canonical_name: 'ORDERED_QUANTITY', category: 'Order Management' },
      { term: 'how many were ordered', canonical_name: 'ORDERED_QUANTITY', category: 'Order Management' },
      { term: 'shipped quantity', canonical_name: 'SHIPPED_QUANTITY', category: 'Order Management' },
      { term: 'how many were shipped', canonical_name: 'SHIPPED_QUANTITY', category: 'Order Management' },
      { term: 'fulfilled quantity', canonical_name: 'FULFILLED_QUANTITY', category: 'Order Management' },
      { term: 'invoiced quantity', canonical_name: 'INVOICED_QUANTITY', category: 'Order Management' },
      { term: 'cancelled quantity', canonical_name: 'CANCELLED_QUANTITY', category: 'Order Management' },
      { term: 'unit selling price', canonical_name: 'UNIT_SELLING_PRICE', category: 'Order Management' },
      { term: 'selling price', canonical_name: 'UNIT_SELLING_PRICE', category: 'Order Management' },
      { term: 'sale price', canonical_name: 'UNIT_SELLING_PRICE', category: 'Order Management' },
      { term: 'unit list price', canonical_name: 'UNIT_LIST_PRICE', category: 'Order Management' },
      { term: 'list price', canonical_name: 'UNIT_LIST_PRICE', category: 'Order Management' },
      { term: 'line status', canonical_name: 'FLOW_STATUS_CODE', category: 'Order Management' },
      { term: 'ship date', canonical_name: 'ACTUAL_SHIPMENT_DATE', category: 'Order Management' },
      { term: 'actual ship date', canonical_name: 'ACTUAL_SHIPMENT_DATE', category: 'Order Management' },
      { term: 'when did it ship', canonical_name: 'ACTUAL_SHIPMENT_DATE', category: 'Order Management' },
      { term: 'scheduled ship date', canonical_name: 'SCHEDULE_SHIP_DATE', category: 'Order Management' },
      { term: 'promise date', canonical_name: 'PROMISE_DATE', category: 'Order Management' },
      { term: 'request date', canonical_name: 'REQUEST_DATE', category: 'Order Management' },
      { term: 'requested delivery date', canonical_name: 'REQUEST_DATE', category: 'Order Management' },
      { term: 'pricing date', canonical_name: 'PRICING_DATE', category: 'Order Management' },
      { term: 'ordered item', canonical_name: 'ORDERED_ITEM', category: 'Order Management' },
      { term: 'what was ordered', canonical_name: 'ITEM_DESCRIPTION', category: 'Order Management' },
      { term: 'line category', canonical_name: 'LINE_CATEGORY_CODE', category: 'Order Management' },
      { term: 'return reason', canonical_name: 'RETURN_REASON_CODE', category: 'Order Management' },
      { term: 'tax amount', canonical_name: 'TAX_VALUE', category: 'Order Management' },
      { term: 'order tax', canonical_name: 'TAX_VALUE', category: 'Order Management' },
      { term: 'price adjustment', canonical_name: 'OE_PRICE_ADJUSTMENTS', category: 'Order Management' },
      { term: 'discount', canonical_name: 'OE_PRICE_ADJUSTMENTS', category: 'Order Management' },
      { term: 'sales credit', canonical_name: 'OE_SALES_CREDITS', category: 'Order Management' },
      { term: 'commission', canonical_name: 'OE_SALES_CREDITS', category: 'Order Management' },
      { term: 'order hold', canonical_name: 'OE_ORDER_HOLDS_ALL', category: 'Order Management' },
      { term: 'hold on order', canonical_name: 'OE_ORDER_HOLDS_ALL', category: 'Order Management' },
      { term: 'why is order on hold', canonical_name: 'OE_ORDER_HOLDS_ALL', category: 'Order Management' },
    ];

    let inserted = 0;
    for (const s of erpPack) {
      try {
        await query(
          `INSERT INTO global_synonyms (term, canonical_name, category, domain_pack, description)
           VALUES ($1, $2, $3, 'erp', $4)
           ON CONFLICT DO NOTHING`,
          [s.term, s.canonical_name, s.category, `ERP synonym: ${s.term} → ${s.canonical_name}`]
        );
        inserted++;
      } catch (e) {
        console.error(`ERP seed error for "${s.term}":`, e.message);
      }
    }

    res.json({ inserted, total: erpPack.length, message: `Seeded ${inserted} ERP terms into global_synonyms` });
  } catch (err) {
    console.error('Seed ERP error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── POST /api/synonyms/admin/seed-sap — SAP ECC domain pack into global_synonyms ───
router.post('/admin/seed-sap', async (req, res) => {
  try {
    const sapPack = [
      // ═══════════════════════════════════════════════════════════════
      // SD — SALES & DISTRIBUTION
      // ═══════════════════════════════════════════════════════════════

      // Table-level synonyms (canonical_name = SAP table name)
      { term: 'sales order', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'sales orders', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'order', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'orders', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'customer order', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'bookings', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'booking', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'sales booking', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'sales bookings', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'order header', canonical_name: 'VBAK', category: 'Sales' },

      { term: 'order line', canonical_name: 'VBAP', category: 'Sales' },
      { term: 'order item', canonical_name: 'VBAP', category: 'Sales' },
      { term: 'sales order line', canonical_name: 'VBAP', category: 'Sales' },
      { term: 'line item', canonical_name: 'VBAP', category: 'Sales' },
      { term: 'order detail', canonical_name: 'VBAP', category: 'Sales' },

      { term: 'schedule line', canonical_name: 'VBEP', category: 'Sales' },
      { term: 'delivery schedule', canonical_name: 'VBEP', category: 'Sales' },

      { term: 'order flow', canonical_name: 'VBFA', category: 'Sales' },
      { term: 'document flow', canonical_name: 'VBFA', category: 'Sales' },
      { term: 'sales document flow', canonical_name: 'VBFA', category: 'Sales' },
      { term: 'order lifecycle', canonical_name: 'VBFA', category: 'Sales' },

      { term: 'billing document', canonical_name: 'VBRK', category: 'Sales' },
      { term: 'invoice', canonical_name: 'VBRK', category: 'Sales' },
      { term: 'sales invoice', canonical_name: 'VBRK', category: 'Sales' },
      { term: 'billing header', canonical_name: 'VBRK', category: 'Sales' },
      { term: 'billed revenue', canonical_name: 'VBRK', category: 'Sales' },

      { term: 'billing line', canonical_name: 'VBRP', category: 'Sales' },
      { term: 'billing item', canonical_name: 'VBRP', category: 'Sales' },
      { term: 'invoice line', canonical_name: 'VBRP', category: 'Sales' },

      { term: 'delivery', canonical_name: 'LIKP', category: 'Sales' },
      { term: 'shipment', canonical_name: 'LIKP', category: 'Sales' },
      { term: 'delivery header', canonical_name: 'LIKP', category: 'Sales' },
      { term: 'outbound delivery', canonical_name: 'LIKP', category: 'Sales' },

      { term: 'delivery item', canonical_name: 'LIPS', category: 'Sales' },
      { term: 'delivery line', canonical_name: 'LIPS', category: 'Sales' },
      { term: 'shipped item', canonical_name: 'LIPS', category: 'Sales' },

      { term: 'order status', canonical_name: 'VBUK', category: 'Sales' },
      { term: 'sales order status', canonical_name: 'VBUK', category: 'Sales' },

      // Column-level synonyms for SD (canonical_name = SAP column name)
      { term: 'order number', canonical_name: 'VBELN', category: 'Sales' },
      { term: 'sales order number', canonical_name: 'VBELN', category: 'Sales' },
      { term: 'document number', canonical_name: 'VBELN', category: 'Sales' },

      { term: 'order value', canonical_name: 'NETWR', category: 'Sales' },
      { term: 'net value', canonical_name: 'NETWR', category: 'Sales' },
      { term: 'order amount', canonical_name: 'NETWR', category: 'Sales' },
      { term: 'sales amount', canonical_name: 'NETWR', category: 'Sales' },
      { term: 'revenue', canonical_name: 'NETWR', category: 'Sales' },
      { term: 'total sales', canonical_name: 'NETWR', category: 'Sales' },
      { term: 'booking value', canonical_name: 'NETWR', category: 'Sales' },
      { term: 'booking amount', canonical_name: 'NETWR', category: 'Sales' },

      { term: 'order date', canonical_name: 'ERDAT', category: 'Sales' },
      { term: 'creation date', canonical_name: 'ERDAT', category: 'Sales' },
      { term: 'booking date', canonical_name: 'ERDAT', category: 'Sales' },
      { term: 'when was order placed', canonical_name: 'ERDAT', category: 'Sales' },
      { term: 'when was order created', canonical_name: 'ERDAT', category: 'Sales' },

      { term: 'order type', canonical_name: 'AUART', category: 'Sales' },
      { term: 'sales order type', canonical_name: 'AUART', category: 'Sales' },

      { term: 'sales organization', canonical_name: 'VKORG', category: 'Sales' },
      { term: 'sales org', canonical_name: 'VKORG', category: 'Sales' },

      { term: 'distribution channel', canonical_name: 'VTWEG', category: 'Sales' },
      { term: 'channel', canonical_name: 'VTWEG', category: 'Sales' },

      { term: 'division', canonical_name: 'SPART', category: 'Sales' },
      { term: 'product division', canonical_name: 'SPART', category: 'Sales' },

      { term: 'customer number', canonical_name: 'KUNNR', category: 'Sales' },
      { term: 'customer id', canonical_name: 'KUNNR', category: 'Sales' },
      { term: 'sold to', canonical_name: 'KUNNR', category: 'Sales' },

      { term: 'customer po', canonical_name: 'BSTNK', category: 'Sales' },
      { term: 'customer purchase order', canonical_name: 'BSTNK', category: 'Sales' },

      { term: 'delivery date', canonical_name: 'VDATU', category: 'Sales' },
      { term: 'requested delivery date', canonical_name: 'VDATU', category: 'Sales' },

      { term: 'currency', canonical_name: 'WAERK', category: 'Sales' },
      { term: 'order currency', canonical_name: 'WAERK', category: 'Sales' },

      { term: 'material', canonical_name: 'MATNR', category: 'Sales' },
      { term: 'product', canonical_name: 'MATNR', category: 'Sales' },
      { term: 'material number', canonical_name: 'MATNR', category: 'Sales' },
      { term: 'item number', canonical_name: 'MATNR', category: 'Sales' },

      { term: 'order quantity', canonical_name: 'KWMENG', category: 'Sales' },
      { term: 'ordered quantity', canonical_name: 'KWMENG', category: 'Sales' },
      { term: 'how many were ordered', canonical_name: 'KWMENG', category: 'Sales' },

      { term: 'plant', canonical_name: 'WERKS', category: 'Sales' },
      { term: 'shipping plant', canonical_name: 'WERKS', category: 'Sales' },

      // ═══════════════════════════════════════════════════════════════
      // FI — FINANCIAL ACCOUNTING
      // ═══════════════════════════════════════════════════════════════

      // GL Accounting
      { term: 'gl posting', canonical_name: 'BSIS', category: 'General Ledger' },
      { term: 'gl postings', canonical_name: 'BSIS', category: 'General Ledger' },
      { term: 'gl transaction', canonical_name: 'BSIS', category: 'General Ledger' },
      { term: 'gl transactions', canonical_name: 'BSIS', category: 'General Ledger' },
      { term: 'gl line item', canonical_name: 'BSIS', category: 'General Ledger' },
      { term: 'gl line items', canonical_name: 'BSIS', category: 'General Ledger' },
      { term: 'journal entry', canonical_name: 'BSIS', category: 'General Ledger' },
      { term: 'journal entries', canonical_name: 'BSIS', category: 'General Ledger' },
      { term: 'gl account posting', canonical_name: 'BSIS', category: 'General Ledger' },
      { term: 'general ledger', canonical_name: 'BSIS', category: 'General Ledger' },

      { term: 'gl balance', canonical_name: 'GLT0', category: 'General Ledger' },
      { term: 'account balance', canonical_name: 'GLT0', category: 'General Ledger' },
      { term: 'gl totals', canonical_name: 'GLT0', category: 'General Ledger' },
      { term: 'trial balance', canonical_name: 'GLT0', category: 'General Ledger' },

      { term: 'gl account', canonical_name: 'SKA1', category: 'General Ledger' },
      { term: 'chart of accounts', canonical_name: 'SKA1', category: 'General Ledger' },
      { term: 'account master', canonical_name: 'SKA1', category: 'General Ledger' },

      { term: 'gl account description', canonical_name: 'SKAT', category: 'General Ledger' },
      { term: 'account name', canonical_name: 'SKAT', category: 'General Ledger' },

      { term: 'accounting document', canonical_name: 'BKPF', category: 'General Ledger' },
      { term: 'journal header', canonical_name: 'BKPF', category: 'General Ledger' },
      { term: 'fi document', canonical_name: 'BKPF', category: 'General Ledger' },

      { term: 'accounting line item', canonical_name: 'BSEG', category: 'General Ledger' },
      { term: 'journal line', canonical_name: 'BSEG', category: 'General Ledger' },
      { term: 'fi line item', canonical_name: 'BSEG', category: 'General Ledger' },

      { term: 'new gl line item', canonical_name: 'FAGLFLEXA', category: 'General Ledger' },
      { term: 'new gl actual', canonical_name: 'FAGLFLEXA', category: 'General Ledger' },
      { term: 'new gl totals', canonical_name: 'FAGLFLEXT', category: 'General Ledger' },

      // GL column synonyms
      { term: 'posting date', canonical_name: 'BUDAT', category: 'General Ledger' },
      { term: 'document date', canonical_name: 'BLDAT', category: 'General Ledger' },
      { term: 'document type', canonical_name: 'BLART', category: 'General Ledger' },
      { term: 'posting key', canonical_name: 'BSCHL', category: 'General Ledger' },
      { term: 'local amount', canonical_name: 'DMBTR', category: 'General Ledger' },
      { term: 'amount in local currency', canonical_name: 'DMBTR', category: 'General Ledger' },
      { term: 'transaction amount', canonical_name: 'WRBTR', category: 'General Ledger' },
      { term: 'amount in document currency', canonical_name: 'WRBTR', category: 'General Ledger' },
      { term: 'debit credit', canonical_name: 'SHKZG', category: 'General Ledger' },
      { term: 'gl account number', canonical_name: 'HKONT', category: 'General Ledger' },
      { term: 'account number', canonical_name: 'HKONT', category: 'General Ledger' },
      { term: 'fiscal year', canonical_name: 'GJAHR', category: 'General Ledger' },
      { term: 'period', canonical_name: 'MONAT', category: 'General Ledger' },
      { term: 'fiscal period', canonical_name: 'MONAT', category: 'General Ledger' },
      { term: 'company code', canonical_name: 'BUKRS', category: 'General Ledger' },

      // Customer accounting
      { term: 'customer open item', canonical_name: 'BSID', category: 'Accounts Receivable' },
      { term: 'customer receivable', canonical_name: 'BSID', category: 'Accounts Receivable' },
      { term: 'ar open item', canonical_name: 'BSID', category: 'Accounts Receivable' },
      { term: 'accounts receivable', canonical_name: 'BSID', category: 'Accounts Receivable' },
      { term: 'ar aging', canonical_name: 'BSID', category: 'Accounts Receivable' },
      { term: 'outstanding receivable', canonical_name: 'BSID', category: 'Accounts Receivable' },

      { term: 'customer cleared item', canonical_name: 'BSAD', category: 'Accounts Receivable' },
      { term: 'customer payment received', canonical_name: 'BSAD', category: 'Accounts Receivable' },

      // Vendor accounting
      { term: 'vendor open item', canonical_name: 'BSIK', category: 'Accounts Payable' },
      { term: 'vendor payable', canonical_name: 'BSIK', category: 'Accounts Payable' },
      { term: 'ap open item', canonical_name: 'BSIK', category: 'Accounts Payable' },
      { term: 'accounts payable', canonical_name: 'BSIK', category: 'Accounts Payable' },
      { term: 'ap aging', canonical_name: 'BSIK', category: 'Accounts Payable' },
      { term: 'outstanding payable', canonical_name: 'BSIK', category: 'Accounts Payable' },
      { term: 'unpaid vendor invoice', canonical_name: 'BSIK', category: 'Accounts Payable' },

      { term: 'vendor cleared item', canonical_name: 'BSAK', category: 'Accounts Payable' },
      { term: 'vendor payment made', canonical_name: 'BSAK', category: 'Accounts Payable' },
      { term: 'paid vendor invoice', canonical_name: 'BSAK', category: 'Accounts Payable' },

      { term: 'vendor invoice', canonical_name: 'RBKP', category: 'Accounts Payable' },
      { term: 'invoice receipt', canonical_name: 'RBKP', category: 'Accounts Payable' },
      { term: 'vendor bill', canonical_name: 'RBKP', category: 'Accounts Payable' },

      { term: 'vendor invoice line', canonical_name: 'RSEG', category: 'Accounts Payable' },
      { term: 'invoice receipt line', canonical_name: 'RSEG', category: 'Accounts Payable' },

      { term: 'payment run', canonical_name: 'REGUH', category: 'Accounts Payable' },
      { term: 'payment program', canonical_name: 'REGUH', category: 'Accounts Payable' },

      // Master data
      { term: 'customer', canonical_name: 'KNA1', category: 'Master Data' },
      { term: 'customer master', canonical_name: 'KNA1', category: 'Master Data' },
      { term: 'customer name', canonical_name: 'KNA1', category: 'Master Data' },
      { term: 'sold to party', canonical_name: 'KNA1', category: 'Master Data' },

      { term: 'vendor', canonical_name: 'LFA1', category: 'Master Data' },
      { term: 'supplier', canonical_name: 'LFA1', category: 'Master Data' },
      { term: 'vendor master', canonical_name: 'LFA1', category: 'Master Data' },
      { term: 'vendor name', canonical_name: 'LFA1', category: 'Master Data' },
      { term: 'supplier name', canonical_name: 'LFA1', category: 'Master Data' },

      { term: 'bank', canonical_name: 'BNKA', category: 'Master Data' },
      { term: 'bank master', canonical_name: 'BNKA', category: 'Master Data' },
      { term: 'house bank', canonical_name: 'BNKA', category: 'Master Data' },

      { term: 'company', canonical_name: 'T001', category: 'Master Data' },
      { term: 'company code', canonical_name: 'T001', category: 'Master Data' },

      // ═══════════════════════════════════════════════════════════════
      // MM — MATERIALS MANAGEMENT
      // ═══════════════════════════════════════════════════════════════

      { term: 'purchase order', canonical_name: 'EKKO', category: 'Purchasing' },
      { term: 'po', canonical_name: 'EKKO', category: 'Purchasing' },
      { term: 'procurement', canonical_name: 'EKKO', category: 'Purchasing' },
      { term: 'spend', canonical_name: 'EKKO', category: 'Purchasing' },
      { term: 'spending', canonical_name: 'EKKO', category: 'Purchasing' },
      { term: 'purchasing', canonical_name: 'EKKO', category: 'Purchasing' },
      { term: 'purchase orders', canonical_name: 'EKKO', category: 'Purchasing' },

      { term: 'po line', canonical_name: 'EKPO', category: 'Purchasing' },
      { term: 'purchase order item', canonical_name: 'EKPO', category: 'Purchasing' },
      { term: 'po item', canonical_name: 'EKPO', category: 'Purchasing' },
      { term: 'purchase order line', canonical_name: 'EKPO', category: 'Purchasing' },

      { term: 'purchase requisition', canonical_name: 'EBAN', category: 'Purchasing' },
      { term: 'requisition', canonical_name: 'EBAN', category: 'Purchasing' },
      { term: 'pr', canonical_name: 'EBAN', category: 'Purchasing' },

      { term: 'goods receipt', canonical_name: 'EKBE', category: 'Purchasing' },
      { term: 'po history', canonical_name: 'EKBE', category: 'Purchasing' },
      { term: 'three way match', canonical_name: 'EKBE', category: 'Purchasing' },
      { term: '3 way match', canonical_name: 'EKBE', category: 'Purchasing' },

      { term: 'po schedule line', canonical_name: 'EKET', category: 'Purchasing' },
      { term: 'delivery schedule', canonical_name: 'EKET', category: 'Purchasing' },

      // MM column synonyms
      { term: 'po number', canonical_name: 'EBELN', category: 'Purchasing' },
      { term: 'purchase order number', canonical_name: 'EBELN', category: 'Purchasing' },
      { term: 'po amount', canonical_name: 'NETWR', category: 'Purchasing' },
      { term: 'purchase amount', canonical_name: 'NETWR', category: 'Purchasing' },
      { term: 'po value', canonical_name: 'NETWR', category: 'Purchasing' },
      { term: 'spend amount', canonical_name: 'NETWR', category: 'Purchasing' },
      { term: 'po date', canonical_name: 'BEDAT', category: 'Purchasing' },
      { term: 'purchasing date', canonical_name: 'BEDAT', category: 'Purchasing' },
      { term: 'po type', canonical_name: 'BSART', category: 'Purchasing' },
      { term: 'purchasing group', canonical_name: 'EKGRP', category: 'Purchasing' },
      { term: 'purchasing org', canonical_name: 'EKORG', category: 'Purchasing' },
      { term: 'purchasing organization', canonical_name: 'EKORG', category: 'Purchasing' },

      // Material master
      { term: 'material master', canonical_name: 'MARA', category: 'Materials' },
      { term: 'product master', canonical_name: 'MARA', category: 'Materials' },
      { term: 'material', canonical_name: 'MARA', category: 'Materials' },

      { term: 'material description', canonical_name: 'MAKT', category: 'Materials' },
      { term: 'product description', canonical_name: 'MAKT', category: 'Materials' },
      { term: 'material name', canonical_name: 'MAKT', category: 'Materials' },

      { term: 'material valuation', canonical_name: 'MBEW', category: 'Materials' },
      { term: 'standard cost', canonical_name: 'MBEW', category: 'Materials' },
      { term: 'material cost', canonical_name: 'MBEW', category: 'Materials' },
      { term: 'moving average price', canonical_name: 'MBEW', category: 'Materials' },

      { term: 'material document', canonical_name: 'MKPF', category: 'Materials' },
      { term: 'goods movement', canonical_name: 'MSEG', category: 'Materials' },
      { term: 'material movement', canonical_name: 'MSEG', category: 'Materials' },
      { term: 'inventory movement', canonical_name: 'MSEG', category: 'Materials' },

      { term: 'stock', canonical_name: 'MARD', category: 'Materials' },
      { term: 'inventory', canonical_name: 'MARD', category: 'Materials' },
      { term: 'stock on hand', canonical_name: 'MARD', category: 'Materials' },
      { term: 'warehouse inventory', canonical_name: 'MARD', category: 'Materials' },

      { term: 'plant data', canonical_name: 'MARC', category: 'Materials' },
      { term: 'mrp data', canonical_name: 'MARC', category: 'Materials' },

      // ═══════════════════════════════════════════════════════════════
      // CO — CONTROLLING
      // ═══════════════════════════════════════════════════════════════

      { term: 'cost center', canonical_name: 'CSKS', category: 'Controlling' },
      { term: 'cost center master', canonical_name: 'CSKS', category: 'Controlling' },
      { term: 'cost center name', canonical_name: 'CSKT', category: 'Controlling' },
      { term: 'cost center description', canonical_name: 'CSKT', category: 'Controlling' },

      { term: 'cost element', canonical_name: 'CSKB', category: 'Controlling' },
      { term: 'cost element name', canonical_name: 'CSKU', category: 'Controlling' },

      { term: 'profit center', canonical_name: 'CEPC', category: 'Controlling' },
      { term: 'profit center master', canonical_name: 'CEPC', category: 'Controlling' },
      { term: 'profit center name', canonical_name: 'CEPCT', category: 'Controlling' },

      { term: 'co posting', canonical_name: 'COEP', category: 'Controlling' },
      { term: 'cost posting', canonical_name: 'COEP', category: 'Controlling' },
      { term: 'internal cost', canonical_name: 'COSS', category: 'Controlling' },
      { term: 'external cost', canonical_name: 'COSP', category: 'Controlling' },

      { term: 'controlling area', canonical_name: 'TKA01', category: 'Controlling' },

      // CO column synonyms
      { term: 'cost center number', canonical_name: 'KOSTL', category: 'Controlling' },
      { term: 'profit center number', canonical_name: 'PRCTR', category: 'Controlling' },

      // ═══════════════════════════════════════════════════════════════
      // HR — HUMAN RESOURCES
      // ═══════════════════════════════════════════════════════════════

      { term: 'employee', canonical_name: 'PA0001', category: 'Human Resources' },
      { term: 'employee master', canonical_name: 'PA0001', category: 'Human Resources' },
      { term: 'org assignment', canonical_name: 'PA0001', category: 'Human Resources' },
      { term: 'headcount', canonical_name: 'PA0001', category: 'Human Resources' },

      { term: 'personal data', canonical_name: 'PA0002', category: 'Human Resources' },
      { term: 'employee name', canonical_name: 'PA0002', category: 'Human Resources' },

      { term: 'employee address', canonical_name: 'PA0006', category: 'Human Resources' },

      { term: 'salary', canonical_name: 'PA0008', category: 'Human Resources' },
      { term: 'compensation', canonical_name: 'PA0008', category: 'Human Resources' },
      { term: 'basic pay', canonical_name: 'PA0008', category: 'Human Resources' },
      { term: 'employee pay', canonical_name: 'PA0008', category: 'Human Resources' },

      { term: 'timesheet', canonical_name: 'CATSDB', category: 'Human Resources' },
      { term: 'time entry', canonical_name: 'CATSDB', category: 'Human Resources' },
      { term: 'time tracking', canonical_name: 'CATSDB', category: 'Human Resources' },

      { term: 'payroll', canonical_name: 'PAY_RT', category: 'Human Resources' },
      { term: 'payroll result', canonical_name: 'PAY_RT', category: 'Human Resources' },

      { term: 'benefits', canonical_name: 'BEN_ENROLLMENT', category: 'Human Resources' },
      { term: 'benefits enrollment', canonical_name: 'BEN_ENROLLMENT', category: 'Human Resources' },
      { term: 'benefit plan', canonical_name: 'BEN_PLAN', category: 'Human Resources' },
      { term: 'health plan', canonical_name: 'PA0167', category: 'Human Resources' },
      { term: 'insurance', canonical_name: 'PA0168', category: 'Human Resources' },

      // HR column synonyms
      { term: 'employee number', canonical_name: 'PERNR', category: 'Human Resources' },
      { term: 'personnel number', canonical_name: 'PERNR', category: 'Human Resources' },
      { term: 'employee id', canonical_name: 'PERNR', category: 'Human Resources' },

      // ═══════════════════════════════════════════════════════════════
      // PM — PLANT MAINTENANCE
      // ═══════════════════════════════════════════════════════════════

      { term: 'maintenance order', canonical_name: 'AFIH', category: 'Plant Maintenance' },
      { term: 'work order', canonical_name: 'AUFK', category: 'Plant Maintenance' },
      { term: 'service order', canonical_name: 'AUFK', category: 'Plant Maintenance' },

      { term: 'equipment', canonical_name: 'EQUI', category: 'Plant Maintenance' },
      { term: 'equipment master', canonical_name: 'EQUI', category: 'Plant Maintenance' },
      { term: 'asset equipment', canonical_name: 'EQUI', category: 'Plant Maintenance' },

      { term: 'functional location', canonical_name: 'IFLOT', category: 'Plant Maintenance' },

      { term: 'maintenance plan', canonical_name: 'MPLA', category: 'Plant Maintenance' },
      { term: 'preventive maintenance', canonical_name: 'MPLA', category: 'Plant Maintenance' },

      { term: 'inspection lot', canonical_name: 'QALS', category: 'Plant Maintenance' },
      { term: 'quality inspection', canonical_name: 'QALS', category: 'Plant Maintenance' },

      // ═══════════════════════════════════════════════════════════════
      // ASSET ACCOUNTING
      // ═══════════════════════════════════════════════════════════════

      { term: 'fixed asset', canonical_name: 'ANLA', category: 'Asset Accounting' },
      { term: 'asset master', canonical_name: 'ANLA', category: 'Asset Accounting' },
      { term: 'asset', canonical_name: 'ANLA', category: 'Asset Accounting' },

      { term: 'depreciation', canonical_name: 'ANLB', category: 'Asset Accounting' },
      { term: 'asset depreciation', canonical_name: 'ANLB', category: 'Asset Accounting' },

      { term: 'asset class', canonical_name: 'ANKT', category: 'Asset Accounting' },
      { term: 'asset category', canonical_name: 'ANKT', category: 'Asset Accounting' },

      // ═══════════════════════════════════════════════════════════════
      // CROSS-MODULE — High-value business terms
      // ═══════════════════════════════════════════════════════════════

      { term: 'revenue', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'revenue by month', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'monthly revenue', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'quarterly revenue', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'annual revenue', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'total revenue', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'sales revenue', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'top customers', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'customer sales', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'sales by customer', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'customers by sales', canonical_name: 'VBAK', category: 'Sales' },
      { term: 'sales by product', canonical_name: 'VBAP', category: 'Sales' },
      { term: 'top products', canonical_name: 'VBAP', category: 'Sales' },

      { term: 'spend by vendor', canonical_name: 'EKKO', category: 'Purchasing' },
      { term: 'vendor spend', canonical_name: 'EKKO', category: 'Purchasing' },
      { term: 'top vendors', canonical_name: 'EKKO', category: 'Purchasing' },
      { term: 'procurement spend', canonical_name: 'EKKO', category: 'Purchasing' },
      { term: 'total spend', canonical_name: 'EKKO', category: 'Purchasing' },
      { term: 'spending by category', canonical_name: 'EKPO', category: 'Purchasing' },

      { term: 'withholding tax', canonical_name: 'WITH_ITEM', category: 'General Ledger' },
    ];

    let inserted = 0;
    for (const s of sapPack) {
      try {
        await query(
          `INSERT INTO global_synonyms (term, canonical_name, category, domain_pack, description)
           VALUES ($1, $2, $3, 'erp-sap', $4)
           ON CONFLICT DO NOTHING`,
          [s.term, s.canonical_name, s.category, `SAP synonym: ${s.term} → ${s.canonical_name}`]
        );
        inserted++;
      } catch (e) {
        console.error(`SAP seed error for "${s.term}":`, e.message);
      }
    }

    res.json({ inserted, total: sapPack.length, message: `Seeded ${inserted} SAP terms into global_synonyms` });
  } catch (err) {
    console.error('Seed SAP error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
