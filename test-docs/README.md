# Enterprise Policy Documents for OEBS Environment

This directory contains 5 comprehensive enterprise policy documents designed for a realistic Oracle E-Business Suite (OEBS) environment. Each document is self-contained, professional, and includes specific thresholds, approval chains, and regulatory references appropriate for a mid-to-large organization.

## Documents Included

### 1. AP_Invoice_Processing_Policy.md (1,663 words)
**Owner:** Senior Manager, Accounts Payable

Core invoice processing procedures covering:
- Three-way match requirements (PO, receipt, invoice) with variance tolerances (±1-2%)
- Approval authority matrix: <$5K auto-approved, $5K-$25K manager, $25K-$100K director, >$100K VP+CFO
- Payment terms: Net 30 standard, Net 15 strategic vendors, 2/10 Net 30 discounts
- Duplicate invoice detection rules and quarterly audit procedures
- Month-end cutoff (invoices received by 25th processed same month)
- Oracle AP module workflow configuration and OEBS-specific controls
- SOX compliance cross-references for segregation of duties

### 2. Collections_and_Credit_Policy.md (2,728 words)
**Owner:** Director, Accounts Receivable

Comprehensive AR and collections framework including:
- Credit limit determination using D&B ratings + internal payment history
- Aging bucket escalation: 30 days (reminder), 60 days (phone call), 90 days (demand letter), 120 days (collections agency)
- Write-off thresholds: <$500 auto-approved after 180 days, >$500 requires CFO approval
- Payment plan terms: max 6 months, minimum $1,000 monthly payment
- Automatic credit hold procedures (>$50K overdue or >90 days past due)
- DSO targets (45 days or less) with monthly reporting
- Oracle AR dunning letter automation and customization
- GDPR compliance for customer contact data

### 3. GDPR_Data_Protection_Policy.md (3,703 words)
**Owner:** Chief Privacy Officer / Legal Counsel

GDPR compliance procedures for ERP data processing covering:
- Lawful basis for processing (contract performance, legitimate interest, legal obligation)
- Data Subject Access Request (DSAR) procedures with 30-day response window
- Right to erasure with explicit exceptions for 7-year financial record retention
- Data minimization and purpose limitation principles
- Cross-border data transfer rules (EU-US Data Privacy Framework)
- Data breach notification (72-hour window to supervisory authority)
- Data Protection Impact Assessment (DPIA) requirements for high-risk processing
- Specific retention schedules by data category
- ERP data categories: customer PII, vendor contacts, employee records, financial data

### 4. SOX_Compliance_Controls.md (3,502 words)
**Owner:** Controller / Chief Audit Executive

Sarbanes-Oxley internal controls framework including:
- Segregation of duties matrix (AP, AR, GL, Procurement modules)
- Journal entry approval matrix: ≤$50K single approval, $50K-$250K dual approval, >$250K CFO approval
- Self-approval prohibition with OEBS workflow enforcement
- Quarterly user access recertification process with detailed procedures
- Period close checklist and sign-off requirements
- Complete audit trail configuration (user, timestamp, before/after values)
- Unusual journal entry documentation and review procedures
- Material weakness and significant deficiency reporting thresholds
- IT General Controls: change management, logical access, backup/disaster recovery
- Variance investigation procedures for GL reconciliations

### 5. Procurement_and_Vendor_Management.md (3,431 words)
**Owner:** Director, Procurement & Vendor Management

Comprehensive procurement and vendor management covering:
- Vendor qualification requirements (insurance, certifications, compliance, D&B rating)
- Competitive bidding thresholds: <$10K direct purchase, $10K-$50K 3 quotes, >$50K formal RFP
- Formal RFP procedures with evaluation criteria and scoring methodology
- Sole source justification requirements and approval authority
- Contract review and renewal procedures (standard terms, SLAs, insurance, liability)
- Vendor performance scorecards: 5 categories (Quality 30%, Delivery 25%, Price 20%, Responsiveness 15%, Compliance 10%)
- Preferred vendor program tiering (Platinum/Gold/Silver/Standard/At-Risk)
- Purchase order approval chain matching AP thresholds
- Emergency procurement procedures and on-call agreements
- Vendor data management and GDPR compliance
- SOX compliance controls for procurement segregation of duties

## Document Characteristics

**Professional Structure:**
- Standardized metadata: Document Owner, Effective Date, Last Reviewed, Classification, Version
- Numbered sections (1.0, 2.0, etc.) with detailed subsections
- Tables for approval matrices, thresholds, and metrics
- Cross-references between related policies
- Document revision history with dates and version numbers
- Approval signatures from appropriate executives

**Regulatory and Compliance Focus:**
- SOX (Sarbanes-Oxley) control requirements
- GDPR (General Data Protection Regulation) compliance procedures
- Audit trail and internal control requirements
- Segregation of duties enforcement
- Documentation and retention requirements
- Material weakness and control deficiency reporting

**OEBS-Specific:**
- Oracle module references (AP, AR, GL, Procurement, Inventory)
- OEBS workflow and automation configuration
- Oracle tables, transaction types, and module-specific controls
- Oracle dunning procedures and approval routing
- Audit trail logging and OEBS security features

**Realistic Business Context:**
- Specific dollar thresholds aligned with typical mid-sized organization risk tolerance
- Practical approval chains with clear accountability
- Real-world exception handling and escalation procedures
- Timeline and deadline specifications
- Performance metrics and reporting requirements
- Business justifications for policy requirements

## Usage Notes

These documents are designed as:
- **Training materials** for finance, procurement, and operations teams
- **Process documentation** for system configuration and workflow setup
- **Audit evidence** for SOX and regulatory compliance
- **Test data** for document analysis, extraction, and classification systems
- **Examples** of enterprise-grade policy documentation in OEBS environments

The documents cross-reference each other (e.g., AP policy references SOX controls, Collections references GDPR, Procurement references AP), demonstrating how enterprise policies integrate across functional areas.

All documents are dated January 1, 2026 (Effective Date) and March 15, 2026 (Last Reviewed), creating a realistic governance timeline.

---

**Created:** March 31, 2026
**Total Words:** 15,027
**Total Files:** 5
**Format:** Markdown (.md)
