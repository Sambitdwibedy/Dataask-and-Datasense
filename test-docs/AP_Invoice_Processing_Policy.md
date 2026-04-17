# Accounts Payable Invoice Processing Policy

**Document Owner:** Senior Manager, Accounts Payable
**Effective Date:** January 1, 2026
**Last Reviewed:** March 15, 2026
**Classification:** Internal Use Only
**Version:** 2.1

---

## 1. Purpose and Scope

This policy establishes mandatory procedures for processing vendor invoices within the Oracle E-Business Suite (OEBS) Accounts Payable module. This policy applies to all invoices received across all legal entities and business units, regardless of invoice amount, currency, or vendor location. All Finance department staff, approvers, and department heads must comply with this policy.

---

## 2. Three-Way Match Requirements

### 2.1 Match Definition
A valid invoice must match three critical documents:
- **Purchase Order (PO):** Purchase requisition approved and created in OEBS, containing authorized quantities, unit prices, and delivery terms
- **Receipt (GRR):** Goods receipt record confirming physical delivery, inspection, and acceptance in OEBS Inventory module
- **Invoice:** Vendor invoice with matching line items, quantities, unit prices, and payment terms

### 2.2 Match Variance Tolerances
- **Quantity variance:** ±2% of PO quantity (rounded to nearest unit)
- **Unit price variance:** ±1% of contracted price
- **Line amount variance:** ±$100 or 2% of line amount, whichever is greater
- **Invoices exceeding variance thresholds must be resolved by Accounts Payable before payment processing**

### 2.3 Non-PO Invoices
Invoices without a corresponding PO are prohibited except for:
- Recurring utility and facility invoices (approved standing orders)
- Professional services with signed statements of work
- Sole-source vendor contracts approved by Procurement
- All non-PO invoices require Senior AP Manager approval before payment

---

## 3. Approval Authority and Thresholds

### 3.1 Approval Matrix

| Invoice Amount | Approval Authority | System Workflow |
|---|---|---|
| < $5,000 | Auto-approved (no further action required) | OEBS auto-approves when three-way match clears |
| $5,000 – $24,999 | Department Manager | Oracle Workflow routing to manager |
| $25,000 – $99,999 | Department Director | Oracle Workflow routing to director |
| ≥ $100,000 | Vice President + CFO | Dual approval required in OEBS workflow |

### 3.2 Approval Workflow
1. Invoice receipt and three-way match validation in OEBS AP module
2. Automatic routing to appropriate approver based on invoice amount
3. Approver reviews invoice details and supporting documentation within 5 business days
4. Upon approval, invoice automatically moves to payment queue
5. Rejections must include clear reason codes (e.g., "Duplicate Invoice," "Price Discrepancy," "Missing Documentation")

### 3.3 Escalation for Missing Approvals
- Invoices pending approval for >5 business days trigger automated escalation email
- Unresolved invoices after 10 business days are escalated to the approver's manager
- No payment can be processed without documented approval at appropriate level

---

## 4. Payment Terms and Discount Management

### 4.1 Standard Payment Terms
- **Standard Terms:** Net 30 (payment due 30 days from invoice date)
- **Strategic Vendors:** Net 15 (negotiated partners with preferred status)
- **Discount Terms:** 2/10 Net 30 (2% discount if paid within 10 days; full amount due within 30 days)

### 4.2 Discount Processing
- OEBS AP module automatically calculates available discounts based on invoice terms and payment date
- Discount eligibility is flagged in the payment batch review
- Finance team prioritizes payment of discount-eligible invoices within 10-day window
- Discount capture minimum threshold: $250 per invoice (must exceed processing cost)
- Monthly discount reconciliation report generated showing captured and missed discounts

### 4.3 Early Payment Programs
- Vendors offering >2% discounts for early payment require Executive VP Finance approval
- All early payment agreements must be documented in the Vendor Master record

---

## 5. Duplicate Invoice Detection and Prevention

### 5.1 Detection Rules
The following scenarios trigger duplicate invoice investigation:
- **Invoice number matches:** Same vendor invoice number received twice
- **Amount match:** Same vendor, same amount, within 15-day window
- **Line-item duplication:** Same PO number, receipt date, and line items in multiple invoices
- **All-match duplication:** Identical vendor, amount, date, and description within 30 days

### 5.2 Duplicate Invoice Procedures
1. AP staff flag suspected duplicates in OEBS using "Hold for Investigation" status
2. Vendor contact is initiated to confirm legitimacy of both invoices
3. Documentation of vendor communication is attached to invoice record
4. One invoice is rejected and marked "Duplicate - Rejected"
5. Rejected invoice is retained for audit trail for 7 years

### 5.3 Quarterly Duplicate Review
Finance conducts quarterly audits to identify duplicates processed in error. Any duplicate payments identified are pursued for credit from vendors.

---

## 6. Exception Handling and Escalation

### 6.1 Common Exceptions

| Exception Type | Resolution | Escalation |
|---|---|---|
| Three-way match discrepancy | AP investigates variance; contacts vendor if needed | If unresolved in 5 days, escalate to Procurement |
| Missing or illegible invoice | Request replacement copy from vendor | If vendor unresponsive after 2 requests, refer to Senior AP Manager |
| Currency discrepancy | Verify exchange rate; recalculate if necessary | Director approval required if rate variance >1% |
| Missing PO | Investigate if goods/services were received without authorization | CFO approval required; may indicate control breakdown |
| Unauthorized charges | Reject invoice; initiate chargeback discussion with vendor | Legal review if vendor disputes rejection |

### 6.2 Escalation Authority
- **AP Manager:** Exceptions up to $25,000
- **Director of Finance:** Exceptions $25,001 – $100,000
- **CFO:** Exceptions >$100,000 or policy violations

### 6.3 Exception Documentation
All exceptions must be documented in OEBS with:
- Date identified
- Description of discrepancy
- Resolution steps taken
- Name and date of approver authorizing resolution

---

## 7. Month-End Cutoff Procedures

### 7.1 Cutoff Rules
- **Invoices received by 5:00 PM EST on the 25th** of the month will be processed and accrued in that month's financial statements
- Invoices received after 5:00 PM on the 25th will be held and processed the following month
- **Exception:** Invoices matching POs and receipts dated in the prior month must be accrued in the prior month regardless of invoice receipt date (per GAAP accrual accounting)

### 7.2 Accrual Processing
1. At month-end close, AP generates a list of unmatched receipts (goods received, not invoiced)
2. Finance Accounting creates manual accrual entries for estimated amounts
3. When actual invoice arrives (following month), accrual is reversed and actual invoice is recorded
4. Accrual discrepancies >$500 require investigation and documentation

### 7.3 Period Close Sign-Off
- AP Manager certifies all invoices for the period have been processed in OEBS
- Controller reviews period close report and approves for consolidation into General Ledger
- Certification is documented in the month-end close binder retained for audit purposes

---

## 8. Oracle AP Module Workflow and Configuration

### 8.1 OEBS AP Standard Processes
- All invoices must be entered into the OEBS AP module (Transaction Type: "Standard Invoice")
- Three-way match is configured with tolerances per Section 2.2
- Approval routing is automated based on approval thresholds in Section 3.1
- Payment processing uses Oracle Payment Manager for check, ACH, or wire transfer

### 8.2 Required OEBS Fields
- Vendor Name and ID (from Vendor Master)
- Invoice Number and Date
- PO Number and Receipt Number (when applicable)
- Line-item descriptions matching receipt details
- Payment terms code
- GL Account coding (distribution lines)

### 8.3 System Controls
- Duplicate invoice detection is enabled at header level
- Invoice on hold status prevents payment processing
- Audit trail captures all modifications (user, timestamp, before/after values)
- Monthly reconciliation of AP subledger to General Ledger is mandatory

---

## 9. Supporting Documentation Requirements

### 9.1 Required Documents
- Original vendor invoice (or certified copy)
- Purchase Order (if three-way match required)
- Goods Receipt Record from OEBS Inventory
- Any supporting documentation for exceptions (e.g., receiving discrepancy notes, vendor communications)

### 9.2 Document Retention
- Original invoices and supporting documentation retained for 7 years per tax and financial audit requirements
- Digital storage in OEBS or document management system
- Physical backup of critical documents maintained in secure, temperature-controlled environment

---

## 10. Compliance and Audit

### 10.1 Internal Controls
This policy enforces Sarbanes-Oxley (SOX) compliance controls including:
- Segregation of duties (Procurement creates PO, Receiving confirms receipt, AP processes payment)
- Approval authority based on dollar amount
- Complete audit trail in OEBS
- See **SOX_Compliance_Controls.md** for detailed control matrix

### 10.2 Audit Procedures
- Internal audit conducts quarterly testing of invoice processing controls
- External auditors test AP controls during annual financial audit
- Exceptions and control deviations are documented and remediated

### 10.3 Policy Violations
- Invoices processed without required approvals are escalated to Finance Leadership
- Repeated violations by individuals result in disciplinary action up to termination
- Systematic control failures trigger process reengineering

---

## 11. Roles and Responsibilities

| Role | Responsibility |
|---|---|
| **Accounts Payable Specialist** | Data entry, three-way match validation, exception resolution |
| **AP Manager** | Invoice approval (up to $25K), escalation authority, period close |
| **Department Manager** | Approval of invoices $5K–$25K |
| **Department Director** | Approval of invoices $25K–$100K |
| **Vice President Finance** | Approval of invoices >$100K, policy oversight |
| **CFO** | Dual approval for invoices >$100K, policy exception authority |

---

## 12. References and Related Policies

- **SOX_Compliance_Controls.md** – Segregation of duties and journal entry controls
- **Procurement_and_Vendor_Management.md** – Purchase order creation and vendor qualification
- Oracle E-Business Suite Accounts Payable User Guide
- Internal Revenue Code Section 163(j) – Business Interest Deduction
- GAAP Standards ASC 835 – Interest Costs

---

## Document Revision History

| Date | Version | Changes | Author |
|---|---|---|---|
| 2025-01-15 | 1.0 | Initial policy creation | Finance Director |
| 2025-06-20 | 1.5 | Updated approval thresholds; added discount management | AP Manager |
| 2026-01-01 | 2.0 | Added GDPR considerations for vendor data; enhanced exception procedures | Finance Director |
| 2026-03-15 | 2.1 | Clarified month-end cutoff; SOX control cross-references | AP Manager |

---

**Approved by:** Chief Financial Officer
**Next Review Date:** January 1, 2027
