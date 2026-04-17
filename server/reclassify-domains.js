#!/usr/bin/env node
/**
 * OFBiz Domain Reclassification Script
 *
 * Consolidates 100+ fragmented domains into ~15 canonical OFBiz modules.
 * Uses table name prefix patterns + existing domain name mapping.
 * Run once: node reclassify-domains.js [appId]
 */
require('dotenv').config();
const { query } = require('./db');

// ═══════════════════════════════════════════════════════════════
// CANONICAL DOMAIN TAXONOMY — 15 OFBiz Modules
// ═══════════════════════════════════════════════════════════════
const CANONICAL_DOMAINS = {
  'Order Management': {
    prefixes: ['order_', 'cart_', 'quote_', 'requirement', 'shopping_list', 'allocation_plan', 'return_'],
    existingDomains: [
      'Order Management', 'Order Management - Returns', 'Order Management & Fulfillment',
      'Order Management / Fulfillment', 'Order Management / Shipping', 'Order Management / Logistics',
      'Order Management & Inventory', 'Quote Management', 'Requirements Management',
      'E-Commerce Shopping Lists'
    ]
  },
  'Product Management': {
    prefixes: ['product_', 'prod_', 'good_identification', 'config_option', 'desired_feature', 'supplier_'],
    existingDomains: [
      'Product Catalog Management', 'Product Management', 'Product Store Management',
      'Product Pricing', 'Product Accounting', 'Product Configuration', 'Product Costing',
      'Product Management - Maintenance', 'Product Promotion Management', 'Promotion Management',
      'E-commerce Store Management', 'E-commerce Marketing'
    ]
  },
  'Party & Contact Management': {
    prefixes: ['party_', 'person_', 'contact_mech', 'contact_list', 'postal_address', 'telecom_number',
               'telecom_gateway', 'telecom_method', 'email_address', 'address_match', 'valid_contact',
               'communication_event', 'comm_content'],
    existingDomains: [
      'Party Management', 'Contact Management', 'Communication Management',
      'Party Communication', 'Marketing & Communications', 'Marketing Communications',
      'Marketing/Communications'
    ]
  },
  'Financial Management': {
    prefixes: ['acctg_trans', 'gl_', 'fin_account', 'budget', 'settlement_term', 'variance_reason',
               'custom_time_period', 'period_type'],
    existingDomains: [
      'Financial Management', 'Accounting', 'General Ledger', 'Accounting/Invoicing',
      'Accounting/Financial Management', 'Accounting/Billing', 'Accounting - General Ledger',
      'General Ledger / Accounting', 'Budgeting & Financial Planning', 'Accounting Configuration',
      'Financial Management - Budgeting', 'Accounting/Costing'
    ]
  },
  'Invoice & Billing': {
    prefixes: ['invoice_', 'billing_account'],
    existingDomains: [
      'Billing Management', 'Invoice Management'
    ]
  },
  'Payment Processing': {
    prefixes: ['payment_', 'credit_card', 'eft_account', 'gift_card', 'pay_pal', 'check_account',
               'value_link'],
    existingDomains: [
      'Payment Gateway Configuration', 'Payment Management', 'Payment Processing',
      'Gift Card Management'
    ]
  },
  'Inventory Management': {
    prefixes: ['inventory_item', 'inventory_transfer', 'physical_inventory', 'lot', 'item_issuance'],
    existingDomains: [
      'Inventory Management'
    ]
  },
  'Shipping & Logistics': {
    prefixes: ['shipment_', 'carrier_', 'picklist', 'delivery_', 'shipping_', 'tracking_code'],
    existingDomains: [
      'Shipment Management', 'Shipping Gateway Configuration', 'Logistics/Shipping',
      'Supply Chain Management', 'Logistics/Fulfillment', 'E-commerce Shipping',
      'Shipping & Logistics', 'Logistics and Shipping', 'Order Management / Shipping'
    ]
  },
  'Facility Management': {
    prefixes: ['facility_', 'container', 'container_'],
    existingDomains: [
      'Facility Management'
    ]
  },
  'Human Resources': {
    prefixes: ['empl_', 'employment', 'job_interview', 'pay_grade', 'pay_history', 'salary_step',
               'perf_review', 'performance_note', 'benefit_type', 'party_benefit', 'training_',
               'person_training', 'unemployment', 'termination_type', 'responsibility_type',
               'valid_responsibility', 'skill_type', 'party_skill', 'party_qual', 'party_resume',
               'rejection_reason', 'deduction'],
    existingDomains: [
      'Human Resources', 'Human Resources - Compensation', 'Human Resources - Payroll',
      'Human Resources - Performance Management', 'Human Resources & Billing',
      'Human Resources - Work Management'
    ]
  },
  'Content Management': {
    prefixes: ['content_', 'content', 'data_resource', 'audio_data', 'video_data', 'image_data',
               'electronic_text', 'document', 'java_resource', 'file_extension', 'mime_type',
               'character_set', 'meta_data', 'keyword_thesaurus'],
    existingDomains: [
      'Content Management', 'Data Management', 'Content Management / Theming'
    ]
  },
  'Marketing & Sales': {
    prefixes: ['marketing_', 'market_interest', 'sales_forecast', 'sales_opportunity', 'segment_group',
               'tracking_code', 'web_analytics'],
    existingDomains: [
      'Sales Management', 'Sales Management / CRM', 'CRM/Sales Management',
      'Marketing & Partnerships', 'Marketing Management', 'Marketing Campaign Management',
      'Customer Relationship Management', 'Customer Service Management', 'Customer Service/CRM',
      'Web Analytics Management'
    ]
  },
  'Manufacturing & Work Management': {
    prefixes: ['work_effort', 'work_order', 'work_req', 'tech_data', 'mrp_event', 'timesheet',
               'time_entry', 'cost_component', 'fixed_asset', 'component', 'deliverable'],
    existingDomains: [
      'Work Effort Management', 'Work Management', 'Manufacturing & Work Management',
      'Manufacturing/Production', 'Manufacturing/Scheduling', 'Manufacturing & Costing',
      'Manufacturing Cost Accounting', 'Asset Management', 'Fixed Asset Management'
    ]
  },
  'Tax & Compliance': {
    prefixes: ['tax_authority', 'zip_sales_tax'],
    existingDomains: [
      'Tax Management'
    ]
  },
  'Survey & Feedback': {
    prefixes: ['survey_', 'survey', 'cust_request'],
    existingDomains: [
      'Survey Management'
    ]
  },
  'Agreement & Contract Management': {
    prefixes: ['agreement_', 'agreement', 'addendum', 'term_type'],
    existingDomains: [
      'Agreement Management', 'Terms and Conditions Management'
    ]
  },
  'Subscription Management': {
    prefixes: ['subscription_'],
    existingDomains: [
      'Subscription Management', 'Party Management / Subscription Services'
    ]
  },
  'System & Reference Data': {
    prefixes: ['enumeration', 'status_', 'uom', 'geo', 'geo_', 'standard_language', 'country_',
               'note_data', 'sequence_value', 'entity_key', 'system_property', 'custom_method',
               'custom_screen', 'user_pref', 'web_preference', 'data_source', 'data_template',
               'application_sandbox', 'tenant', 'role_type', 'priority_type', 'quantity_break',
               'old_', 'responding_party', 'portal_', 'portlet_', 'visual_theme', 'web_site',
               'web_user', 'sale_type', 'marital_status', 'accommodation_',
               'email_template', 'vendor'],
    existingDomains: [
      'System Configuration', 'System Administration', 'Common/Reference Data',
      'User Interface Management', 'Portal Management', 'Website Management',
      'Website Security & Access Control', 'Integration/File Transfer',
      'E-commerce Store Management'
    ]
  }
};

async function reclassifyDomains(appId) {
  console.log(`\n🔄 Reclassifying domains for app ${appId}...\n`);

  // Build reverse lookup: existing domain name → canonical domain
  const domainNameMap = {};
  for (const [canonical, config] of Object.entries(CANONICAL_DOMAINS)) {
    for (const existing of config.existingDomains) {
      domainNameMap[existing.toLowerCase()] = canonical;
    }
  }

  // Get all tables
  const result = await query(
    'SELECT id, table_name, entity_metadata FROM app_tables WHERE app_id = $1',
    [appId]
  );

  console.log(`Found ${result.rows.length} tables\n`);

  const stats = { total: 0, reclassified: 0, byDomain: {} };

  for (const row of result.rows) {
    stats.total++;
    const meta = typeof row.entity_metadata === 'string'
      ? JSON.parse(row.entity_metadata || '{}')
      : (row.entity_metadata || {});

    const currentDomain = meta.domain || meta.module || 'Unclassified';
    let newDomain = null;

    // Step 1: Try mapping existing domain name to canonical
    const currentLower = currentDomain.toLowerCase();
    if (domainNameMap[currentLower]) {
      newDomain = domainNameMap[currentLower];
    }

    // Step 2: If still unclassified or unmapped, try table name prefix matching
    if (!newDomain || newDomain === currentDomain) {
      const tableName = row.table_name.toLowerCase();
      for (const [canonical, config] of Object.entries(CANONICAL_DOMAINS)) {
        for (const prefix of config.prefixes) {
          if (tableName.startsWith(prefix) || tableName === prefix) {
            newDomain = canonical;
            break;
          }
        }
        if (newDomain && newDomain !== currentDomain) break;
      }
    }

    // Step 3: If still no match, check if current domain is already canonical
    if (!newDomain) {
      if (CANONICAL_DOMAINS[currentDomain]) {
        newDomain = currentDomain; // already canonical
      } else {
        newDomain = 'System & Reference Data'; // default bucket for truly generic tables
      }
    }

    // Update if changed
    if (newDomain !== currentDomain) {
      meta.domain = newDomain;
      meta._previous_domain = currentDomain; // preserve history
      await query(
        'UPDATE app_tables SET entity_metadata = $1 WHERE id = $2',
        [JSON.stringify(meta), row.id]
      );
      stats.reclassified++;
    } else {
      meta.domain = newDomain; // ensure consistency even if not changed
    }

    stats.byDomain[newDomain] = (stats.byDomain[newDomain] || 0) + 1;
  }

  console.log(`✅ Reclassification complete!\n`);
  console.log(`   Total tables: ${stats.total}`);
  console.log(`   Reclassified: ${stats.reclassified}`);
  console.log(`   Domains: ${Object.keys(stats.byDomain).length}\n`);
  console.log(`Domain distribution:`);

  const sorted = Object.entries(stats.byDomain).sort((a, b) => b[1] - a[1]);
  for (const [domain, count] of sorted) {
    console.log(`   ${String(count).padStart(4)}  ${domain}`);
  }
}

// Run
const appId = process.argv[2] || 1;
reclassifyDomains(appId)
  .then(() => process.exit(0))
  .catch(err => { console.error('Error:', err); process.exit(1); });
