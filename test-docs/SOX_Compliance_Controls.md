# Sarbanes-Oxley (SOX) Compliance Controls Policy

**Document Owner:** Controller / Chief Audit Executive
**Effective Date:** January 1, 2026
**Last Reviewed:** March 15, 2026
**Classification:** Internal Use Only
**Version:** 2.3

---

## 1. Purpose and Scope

This policy establishes internal controls over financial reporting (ICFR) to ensure compliance with the Sarbanes-Oxley Act (SOX), specifically Section 302 (CEO/CFO Certification) and Section 404 (Management Assessment of Internal Controls). These controls are designed to prevent and detect errors and irregularities in financial transactions processed through the Oracle E-Business Suite (OEBS). This policy applies to all transactions involving the Accounts Payable, Accounts Receivable, General Ledger, and Procurement modules that impact financial statements.

---

## 2. Segregation of Duties (SoD) Matrix

### 2.1 Segregation of Duties Principle
No single individual should have authority to initiate, approve, and record the same transaction. Segregation of duties prevents fraud and unintentional errors.

### 2.2 SoD Matrix for Accounts Payable (AP)

| Transaction Step | Role | Authority Level | Cannot Also Perform |
|---|---|---|---|
| **Create Purchase Requisition** | Department Manager | Manager | Approve PO, Receive Goods, Process Payment |
| **Create/Modify Purchase Order** | Procurement Specialist | Specialist | Approve PO >$10K, Receive Goods, Process Payment |
| **Approve Purchase Order** | Procurement Manager | Manager | Create PO, Receive Goods, Process Payment |
| **Approve PO >$10K** | Director of Procurement | Director | Create/Approve <$10K PO, Receive Goods, Process Payment |
| **Receive Goods (GRR)** | Receiving Clerk | Clerk | Create PO, Approve PO, Process Payment |
| **Match Invoice** | AP Specialist | Specialist | Approve Invoices, Approve Payment Batches |
| **Approve Invoice (<$5K)** | Automatic System | System | (N/A) |
| **Approve Invoice ($5K–$25K)** | Department Manager | Manager | Create PO, Approve Payment Batches |
| **Approve Invoice ($25K–$100K)** | Department Director | Director | Create/Approve <$25K invoices, Approve Payment Batches |
| **Approve Invoice (>$100K)** | Vice President Finance | VP | Create/Approve <$100K invoices, Approve Payment Batches |
| **Approve Payment Batch** | AP Manager | Manager | Approve individual invoices, Process payment in bank |
| **Release Payment** | Treasurer | Treasurer | Approve invoices, Receive goods, Create POs |
| **Reconcile AP Subledger** | AP Accountant | Accountant | Create/Approve invoices, Release payments |
| **Review & Sign-Off GL** | Controller | Controller | Approve/record transactions, Reconcile AR/AP |

### 2.3 SoD Matrix for Accounts Receivable (AR)

| Transaction Step | Role | Authority Level | Cannot Also Perform |
|---|---|---|---|
| **Create Sales Order** | Sales Representative | Sales | Approve Credit, Record Invoice, Process Payment |
| **Approve Credit** | AR Credit Analyst | Analyst | Create orders >$10K, Record invoices, Process payment |
| **Approve Credit >$50K** | AR Manager | Manager | Create orders, Approve credit <$50K, Process payment |
| **Record Invoice** | AR Billing Specialist | Specialist | Create orders, Approve credit, Record payment |
| **Approve Dunning Letter** | AR Collections Manager | Manager | Create invoices, Approve credit, Process payment |
| **Record Payment** | AR Cash Applications | Specialist | Create invoices, Approve credit, Dunning decisions |
| **Approve Write-Off (<$5K)** | AR Manager | Manager | Create invoices, Approve credit, Record payments |
| **Approve Write-Off ($5K–$25K)** | AR Director | Director | Create/Approve <$5K write-offs, Record payments |
| **Approve Write-Off (>$25K)** | CFO | CFO | Create/Approve <$25K write-offs, Record payments |
| **Reconcile AR Subledger** | AR Accountant | Accountant | Create invoices, Approve credit, Record payments |

### 2.4 SoD System Enforcement
- OEBS is configured with user roles and responsibilities that enforce SoD
- Users are assigned ONLY the roles necessary to perform their job function
- Conflicting roles cannot be assigned to the same user (system prevents assignment)
- Quarterly User Access Review (Section 3) identifies and remediates SoD violations
- Violations are escalated to CFO and require business justification or immediate remediation

---

## 3. User Access Control and Quarterly Recertification

### 3.1 Access Control Framework
- **Principle of Least Privilege:** Users are granted minimum access necessary to perform job functions
- **Role-Based Access Control (RBAC):** Users are assigned to roles with predefined permissions
- **Responsibilities:** Users are assigned defined responsibilities (e.g., "AP Invoice Processor") that determine role assignments

### 3.2 Access Request and Approval Process
When a user requires access to OEBS:
1. Manager submits access request form with:
   - User name and employee ID
   - Business justification (job function and required modules)
   - Specific OEBS responsibilities requested (e.g., "AP Invoice Approval – $5K–$25K")
   - Expected duration of access need
2. IT Security verifies user identity and employment status
3. Business Owner (e.g., Finance Manager) approves or denies access based on job function
4. IT Security implements access in OEBS
5. Manager confirms user received access and training
6. Access approval is documented and retained for audit

### 3.3 Quarterly User Access Recertification
Four times per year (Jan, Apr, Jul, Oct):

**Phase 1: Access Inventory (Week 1–2)**
- IT Security generates user access report listing all OEBS users and assigned roles
- Report includes: user name, employee ID, assigned responsibilities, date access was granted, last activity date

**Phase 2: Manager Certification (Week 3–4)**
- Department managers receive access report for their direct reports
- Manager certifies: "This user requires these roles for their current job function" or "Remove access"
- Manager signs certification confirming accuracy and necessity

**Phase 3: Audit and Remediation (Week 5)**
- Finance Controller reviews all certifications
- For users with conflicting roles: escalate to manager for resolution
- For users with no activity in 90 days: escalate to manager for confirmation of continued need
- For terminated users: verify access has been revoked

**Phase 4: Documentation (Week 6)**
- Recertification summary is documented with:
  - Total users reviewed: [X]
  - SoD violations identified: [X]
  - Access revoked for inactivity: [X]
  - Training gaps identified: [X]
- Documentation is signed by Controller and CFO
- Retained for minimum 7 years (SOX requirement)

### 3.4 Access Removal and Termination
- When user is terminated or transferred, manager notifies IT Security immediately
- IT Security revokes OEBS access on effective termination date
- For high-risk roles (e.g., Payment Release, GL closing), access is revoked at end of business day
- Revocation is documented; access removal verification is retained for audit

---

## 4. Journal Entry Controls and Restrictions

### 4.1 Journal Entry Categories and Approval Requirements

| Entry Type | Amount Threshold | Required Approvals | Example |
|---|---|---|---|
| **Routine Entries** (e.g., accruals, depreciation) | ≤ $50,000 | Single approval by manager | Monthly depreciation accrual |
| **Non-Routine Entries** (e.g., unusual, manual GL coding) | ≤ $50,000 | Single approval by manager | Manual revenue reversal |
| **Large Entries** | $50,001 – $250,000 | Dual approval: Manager + Director | One-time large accrual; acquisition entry |
| **Significant Entries** | > $250,000 | Dual approval: Director + CFO | Major asset write-down; goodwill impairment |
| **Period-Close Entries** | Any amount | Approval by Controller, signed by CFO | Month-end accruals, consolidation entries |

### 4.2 Self-Approval Prohibition
- **No user can create and approve their own journal entry**, regardless of amount
- OEBS workflow prevents submission of entries by the same user who created the entry
- Violation of this rule is considered a control deficiency and is escalated to CFO

### 4.3 Unusual Journal Entry Procedures
Entries that deviate from standard accounting policies require additional review:
- **Definition:** Entries not made under the standard closing procedures (e.g., reversals, accrual adjustments, manual GL coding)
- **Identification:** OEBS flags entries with source = "Manual JE"
- **Documentation:** Entry creator must provide:
  - Business justification: Why is this entry necessary?
  - Supporting documentation: Approval, contract, board resolution, legal advice (as applicable)
  - GL account coding basis: Why is this account appropriate?
  - Preparer name, title, and date
- **Approval:** Director or higher must review and approve unusual entries within 5 business days
- **Quarterly Review:** Controller reviews all unusual entries from the period and certifies appropriateness

### 4.4 Journal Entry Audit Trail
All journal entries maintain complete audit trail in OEBS:
- Entry ID (unique identifier)
- Date created and date posted
- User who created entry (with employee ID)
- User who approved entry (with employee ID)
- GL account, debit/credit amount, description
- **Before/After GL Balances:** System records GL account balance before and after entry
- **Entry Status:** Shows status (Draft, Submitted for Approval, Approved, Posted, Reversed)
- **Reversal Audit Trail:** If entry is reversed, reversal entry is linked to original with reversal reason

### 4.5 Prohibited Journal Entry Types
The following transactions CANNOT be processed via manual journal entries; must use standard transaction modules:
- Invoice recording (must use AP Invoice module)
- Payment processing (must use Payment Manager)
- Cash receipt recording (must use AR Cash Receipt module)
- Goods receipt (must use Inventory GRR module)
- PO creation (must use Procurement module)

Prohibited entries submitted to GL are rejected by system; rejection reason is documented to user and manager.

---

## 5. Period Close Procedures and Sign-Off

### 5.1 Month-End Close Timeline

| Day | Activity | Owner | Approval |
|---|---|---|---|
| **Day 1–3** | AP cutoff; invoices processed for month | AP Manager | AP Manager sign-off |
| **Day 4–5** | AR cutoff; invoices recorded for month | AR Manager | AR Manager sign-off |
| **Day 6–15** | Subledger reconciliation to GL; variance investigation | Account Reconciliation team | Manager approval of reconciliations |
| **Day 16–20** | Manual accruals, reversals, period-close adjustments | Accounting Manager | Manager approval of unusual entries |
| **Day 21–25** | GL review and period close; final journal entries | Controller | Controller approval; CFO sign-off |
| **Day 26–28** | Financial statement consolidation and preparation | Accounting Manager | CFO approval of statements |
| **Day 29–30** | External audit procedures; final sign-off | External Audit Partner | Audit sign-off on financial statements |

### 5.2 Close Checklists and Sign-Off
Each department maintains a close checklist:
- **AP Close Checklist:** All invoices through cutoff date processed; three-way match validation complete; accruals for unmatched receipts recorded; reconciliation to GL complete
- **AR Close Checklist:** All invoices recorded; aging report reviewed; allowance for doubtful accounts calculated and recorded; reconciliation to GL complete
- **GL Close Checklist:** All transactions recorded; accruals entered; unusual entries reviewed; all reconciliations complete; nothing posted outside period
- **Consolidation Checklist:** All intercompany transactions eliminated; equity transactions recorded; assets/liabilities account for impairments; disclosures updated

Each checklist is signed by the department manager confirming completion and accuracy. Signed checklists are retained with monthly close documentation.

### 5.3 Variance Investigation and Resolution
When GL reconciliation produces variance:
- **Variance ≤ $100:** Documented and resolved by accountant; approved by manager
- **Variance $101 – $1,000:** Investigated by supervisor; root cause documented; approved by manager
- **Variance $1,001 – $10,000:** Investigated by manager; escalated to Controller; correcting entry required
- **Variance > $10,000:** Escalated to CFO; special investigation; may trigger external audit review
- **Unresolved Variance:** Period close is not completed; GL is held open until variance is resolved

### 5.4 Financial Statement Sign-Off
- Controller reviews financial statements for completeness and accuracy
- Controller identifies key variances from prior period and documents explanations
- CFO reviews financial statements and signs certifying:
  - "I have reviewed the financial statements and based on my knowledge, they are materially accurate and complete"
  - "I am aware of any material weaknesses or significant deficiencies in internal controls"
- Financial statements are filed with SEC (if applicable) with CEO/CFO certification per SOX Section 302

---

## 6. Audit Trail Requirements

### 6.1 Transaction Audit Trail Components
All financial transactions in OEBS must generate audit trail with:

**1. Transaction Identifier**
- Unique transaction ID (e.g., Invoice Number, PO Number, Check Number)
- Date transaction was initiated
- Date transaction was completed/posted

**2. User Identification**
- Employee ID and name of user who created transaction
- Employee ID and name of user who modified transaction (if applicable)
- Employee ID and name of user who approved transaction
- Employee ID and name of user who posted transaction (for GL entries)

**3. Transaction Content**
- Before image: Original values (if modified)
- After image: Final/current values
- Description of transaction (e.g., invoice amount, GL account, description)
- Source reference (e.g., PO number, customer account, vendor ID)

**4. Approval Status**
- Status at each stage: Draft → Submitted → Approved → Posted
- Approval date and time
- Approval signature/confirmation
- If rejected: Rejection reason and date

**5. Modification Trail**
- If transaction is modified after initial posting, all modifications are recorded
- Reason for modification (e.g., "Correction – invoice amount," "Reversal – duplicate")
- Complete modification history is retained

### 6.2 OEBS Audit Trail Configuration
- Audit trail is automatically enabled for all AP, AR, GL, and Procurement transactions
- User activity is logged with millisecond precision timestamp
- Audit records cannot be deleted or modified (append-only)
- Audit trail data is retained for minimum 7 years per SOX and tax requirements
- Monthly audit trail review identifies unusual activity (see Section 6.3)

### 6.3 Audit Trail Monitoring and Analysis
**Quarterly Activity Review:**
- IT Security generates user activity report showing logins, transaction creations, approvals, and modifications
- High-risk activity is flagged: large transactions, after-hours access, access from unusual locations
- Activities outside user's job function are investigated (e.g., AP user approving customer invoices)
- Unusual activity is documented and escalated to Management

**Fraud Detection:**
- Transactions modified >2 times after initial posting trigger investigation
- Users accessing data outside their responsibility area trigger alert
- Entries posted by users with no explicit approval trigger alert
- Entries posted after termination date trigger alert

---

## 7. IT General Controls (ITGC)

### 7.1 Change Management
All changes to OEBS (system configuration, code, data) must follow formal change management process:

**Change Request Process:**
1. Business owner submits Change Request documenting:
   - What is being changed (report, workflow, formula, master data)
   - Why change is needed (business justification)
   - Impact assessment (which processes/users affected)
   - Testing plan (how change will be tested before production)
   - Rollback plan (how to revert if change causes problems)
2. Change Control Board (IT Security, Finance, Business Owner) reviews and approves or rejects
3. Change is implemented in TEST environment first; user acceptance testing confirms change works as intended
4. Change is scheduled for production deployment with maintenance window
5. Change is implemented and validated in production
6. Post-implementation review confirms change successful with no adverse effects

**Emergency Changes:**
- If system failure requires immediate change, request can be expedited
- Emergency change must still have approval from IT Director and Finance Controller
- Post-implementation documentation and review must be completed within 2 business days

### 7.2 Logical Access and Authentication
- All OEBS users must authenticate with username and strong password (minimum 12 characters, uppercase, lowercase, numbers, symbols)
- Multi-factor authentication is required for access to sensitive functions (e.g., GL closing, large payment approval)
- Passwords are changed every 90 days
- System enforces password complexity and prevents reuse of recent passwords
- Inactive users (no login for 90 days) are automatically locked out; IT must re-enable access

### 7.3 Data Backup and Disaster Recovery
- OEBS database is backed up daily (full backup) and hourly (incremental)
- Backups are stored on-site and off-site (geographic redundancy)
- Backup testing is conducted quarterly to verify recoverability
- Recovery time objective (RTO): 4 hours (system online within 4 hours of failure)
- Recovery point objective (RPO): 1 hour (no more than 1 hour of data loss)
- DR test is performed annually; full failover to backup system is validated
- DR test results are documented and reported to CFO

### 7.4 System Segregation
- OEBS Production environment is segregated from TEST environment
- Code and configuration changes are developed/tested in TEST, not PRODUCTION
- Users do not have access to both PRODUCTION and TEST environments simultaneously
- Production database does not contain test data
- Test environment is refreshed monthly from production to maintain data realism (sensitive data is masked in TEST)

---

## 8. Material Weakness and Significant Deficiency Reporting

### 8.1 Definitions
**Material Weakness:** Deficiency, or combination of deficiencies, in ICFR such that there is a reasonable possibility that undetected misstatement >$[threshold] could occur and not be prevented/detected.

**Significant Deficiency:** Deficiency less severe than material weakness but important enough to merit attention.

**Control Threshold:** Company-wide materiality threshold = $250,000 (amount at which financial statement users would alter decisions)

### 8.2 Identification and Documentation
Material weaknesses and significant deficiencies are identified through:
- Internal audit testing
- External audit feedback
- Management review of control effectiveness
- Monitoring of control exceptions (SoD violations, access reviews, transaction testing)

When deficiency is identified:
- Detailed description of control weakness (what is not working)
- Risk assessment: How could this lead to misstatement? (Scenario)
- Estimated impact: Maximum undetected misstatement potential
- Root cause analysis: Why did control fail?
- Remediation plan: How will control be fixed? Timeline? Owner?
- Interim compensating controls: What mitigates risk until permanent fix?

### 8.3 Reporting Requirements
- **Significant Deficiencies:** Reported to CFO and Audit Committee quarterly
- **Material Weaknesses:** Reported to CFO, CEO, and Audit Committee immediately (within 5 business days)
- **Materiality Assessment:** Finance Controller and CFO jointly assess whether deficiency is material
- **Audit Committee Notification:** If material weakness exists, CEO/CFO must disclose to external auditors and audit committee

### 8.4 Remediation Tracking
Each deficiency is assigned an owner responsible for remediation:
- Deficiency tracking log maintained by Internal Audit
- Monthly status updates on remediation progress
- Remediation completion is verified through control testing
- Once remediation is complete, deficiency is removed from tracking log
- Documentation of remediation is retained for 7 years

---

## 9. Financial Statement Assertion Mapping

SOX controls are mapped to financial statement assertions to ensure completeness:

| Assertion | Key Controls | Testing Method |
|---|---|---|
| **Existence/Occurrence** | Three-way match (AP), credit approval (AR), GL posting controls | Confirm randomly selected transactions to supporting documents |
| **Completeness** | AP/AR cutoff procedures, period-close accruals, reconciliations | Test for unrecorded transactions; confirm all invoices/receipts recorded |
| **Accuracy** | Journal entry approval matrix, dual approval for large entries, variance investigation | Test accuracy of amounts, calculations, GL coding |
| **Valuation/Allocation** | Write-off approval, allowance for doubtful accounts calculation, asset impairment review | Test appropriateness of estimates and judgments |
| **Presentation/Disclosure** | Financial statement review, disclosure checklist, consolidation controls | Verify financial statement disclosures are complete and accurate |

---

## 10. Testing and Documentation

### 10.1 Control Testing Procedures
- **Annual Audit:** Internal audit designs and executes test plan to validate key controls
- **Sample Size:** Minimum 25 transactions per control (or all if <25 total)
- **Test Results:** Documented with: control being tested, sample size, deviations identified, conclusion
- **Deviation Investigation:** If deviation found, investigate root cause and assess control deficiency

### 10.2 Documentation Standards
All control-related documentation is retained:
- Control descriptions and procedures (this policy)
- Approval evidence (signed checklists, email approvals, OEBS approval records)
- Audit trail reports (quarterly user access reviews, transaction activity)
- Test results (internal audit testing, external audit working papers)
- Incident documentation (control deviations, SoD violations, corrective actions)
- **Retention Period:** 7 years minimum (tax and audit requirements)

### 10.3 Documentation Storage
- Physical documents: Secure, locked file cabinet in Finance office; access limited to authorized personnel
- Digital documents: Shared drive with access controls; automatic backup; access monitored
- Audit trail data: OEBS database; backed up daily; access restricted to auditors and IT Security

---

## 11. Relationship to Other Policies

- **AP_Invoice_Processing_Policy.md** – AP controls and approval workflows
- **Collections_and_Credit_Policy.md** – AR controls and credit decisions
- **Procurement_and_Vendor_Management.md** – Procurement controls and PO approvals
- **GDPR_Data_Protection_Policy.md** – Data security and access controls (complements SOX logical access)

---

## 12. Regulatory References

- **Sarbanes-Oxley Act, Section 302** – CEO/CFO Certification of Financial Reports
- **Sarbanes-Oxley Act, Section 404** – Management Assessment of Internal Controls
- **SEC Rule 13a-15(f)** – Disclosure of Changes in Internal Control over Financial Reporting
- **COSO Framework** – Internal Control—Integrated Framework (2013)
- **Public Company Accounting Oversight Board (PCAOB)** – Auditing Standard 1305 (Testing of Controls)

---

## Document Revision History

| Date | Version | Changes | Author |
|---|---|---|---|
| 2025-01-15 | 1.0 | Initial policy creation | Controller |
| 2025-06-20 | 1.5 | Enhanced journal entry controls; added ITGC procedures | CFO |
| 2025-12-01 | 2.0 | Added quarterly user access recertification; expanded audit trail procedures | Chief Audit Executive |
| 2026-01-15 | 2.2 | Clarified SoD matrix for AP/AR; added material weakness reporting thresholds | Controller |
| 2026-03-15 | 2.3 | Updated change management procedures; added financial statement assertion mapping | Chief Audit Executive |

---

**Approved by:** Chief Financial Officer, Audit Committee Chair
**Next Review Date:** January 1, 2027
