# Collections and Credit Management Policy

**Document Owner:** Director, Accounts Receivable
**Effective Date:** January 1, 2026
**Last Reviewed:** March 15, 2026
**Classification:** Internal Use Only
**Version:** 2.2

---

## 1. Purpose and Scope

This policy governs credit decisions, accounts receivable management, collections procedures, and write-off authorizations within the Oracle E-Business Suite (OEBS) Accounts Receivable module. This policy applies to all customer accounts across all business lines and geographies. The primary objectives are to minimize credit risk, optimize cash collection, and comply with applicable accounting standards and regulatory requirements.

---

## 2. Credit Limit Determination and Customer Credit Decisions

### 2.1 Credit Limit Assessment Criteria
Credit limits are determined using a multi-factor approach:

**Primary Factor: Dun & Bradstreet (D&B) Rating**
- AAA/High credit rating: Credit limit up to 90 days revenue
- A/Good credit rating: Credit limit up to 60 days revenue
- B/Fair credit rating: Credit limit up to 45 days revenue
- C/Poor credit rating: Credit limit up to 30 days revenue
- No D&B rating: Default to 30-day limit; re-evaluate after 6 months of payment history

**Secondary Factors:**
- **Internal Payment History:** Customers with <2% late payment rate may increase limit by 25%
- **Revenue Volume:** Customers >$1M annual revenue may increase limit by 15%
- **Industry Risk:** High-risk industries (construction, retail) capped at 60-day limit
- **Geographic Risk:** Emerging markets capped at 45-day limit regardless of D&B rating
- **Personal Guarantees:** Private companies may receive 20% increase if CEO/owner guarantee is provided

### 2.2 Initial Credit Limit Assignment
1. Sales requests credit limit for new customer
2. AR Credit Analyst retrieves D&B rating and evaluates secondary factors
3. Credit limit is calculated per 2.1 criteria
4. Limit ≤ $50,000: AR Credit Analyst approves
5. Limit $50,001–$250,000: AR Manager approval required
6. Limit > $250,000: CFO approval required
7. Credit limit is entered into OEBS Customer Master and documented in credit file

### 2.3 Credit Limit Increases
Existing customers requesting credit limit increases follow the approval process in 2.2. Increases are reconsidered annually or when payment performance deteriorates.

### 2.4 Credit Holds
Credit holds are automatically applied when:
- Customer balance exceeds credit limit
- Account is >90 days past due
- Account balance exceeds $50,000 AND overdue >30 days
- D&B rating drops to "C" or lower
- Customer files for bankruptcy or insolvency

When a credit hold is applied:
- New sales orders are blocked in OEBS until hold is released
- AR Credit Analyst contacts customer to arrange payment
- Hold is documented in the account notes with reason and resolution plan
- Hold cannot be released without documented payment or AR Manager approval

---

## 3. Aging Bucket Actions and Collection Procedures

### 3.1 Collections Escalation Schedule

| Aging Bucket | Days Past Due | Action Required | Responsible Party | Timing |
|---|---|---|---|---|
| Current | 0–29 days | Routine dunning letter (automated) | OEBS AR system | On invoice due date + 5 days |
| 30+ | 30–59 days | Phone call and reminder letter | AR Collections Rep | Within 3 business days of reaching 30 days |
| 60+ | 60–89 days | Demand letter + escalation | AR Collections Manager | Within 1 business day of reaching 60 days |
| 90+ | 90–119 days | Collections agency referral (notification) | AR Director | Within 5 business days of reaching 90 days |
| 120+ | 120+ days | Formal collections agency engagement | CFO/Legal | Immediate; refer to external agency |

### 3.2 Dunning Letter Procedures
- **Day 5:** First automated dunning letter (OEBS AR Dunning module); copy to AR Credit Analyst
- **Day 15:** Second reminder letter if payment not received
- **Day 25:** Final notice before escalation

### 3.3 Phone Call and Personal Contact
- **30-day bucket:** AR Collections Representative calls customer; documents conversation in OEBS account notes
- **Contact log includes:** Date, time, person contacted, stated payment date, obstacles discussed, next follow-up
- If customer is unreachable after 2 attempts, escalate to manager for alternative contact strategy

### 3.4 Demand Letter
- **60-day bucket:** Formal demand letter from AR Collections Manager (template in AR Operations Manual)
- Letter states: amount due, due date, consequences of non-payment, contact for payment arrangements
- Letter is sent via certified mail; delivery confirmation retained for 7 years
- Copy is added to customer credit file in OEBS

### 3.5 Collections Agency Referral
- **90-day bucket:** AR Director notifies customer in writing that account will be referred to external collections agency
- **120-day bucket (if still unpaid):** Account is formally referred to selected collections agency
- Customer remains on credit hold; new sales prohibited until resolved
- Monthly reports from collections agency are reviewed by AR Director

### 3.6 Legal Action
- Accounts >180 days past due with balance >$25,000 are escalated to Legal department
- Legal evaluates viability of civil suit based on contract terms and customer assets
- All legal actions require CFO approval and documentation of business rationale

---

## 4. Write-Off Procedures and Thresholds

### 4.1 Write-Off Authorization Matrix

| Balance Amount | Days Past Due | Authorization Required | Documentation |
|---|---|---|---|
| < $500 | 180+ days | AR Manager approval | Write-off memo with customer contact attempts |
| $500–$5,000 | 180+ days | AR Director + Controller approval | Write-off memo + management review |
| $5,001–$25,000 | 180+ days | CFO approval | Write-off memo + business justification |
| > $25,000 | 180+ days | CFO + Audit Committee approval | Comprehensive write-off analysis + Board notification |

### 4.2 Write-Off Processing
1. AR staff documents all collection attempts in OEBS account notes
2. Documentation includes: contact dates, conversations, promises to pay, reasons for non-payment
3. Write-off request is submitted to appropriate approval authority with supporting documentation
4. Upon approval, AR staff creates AR credit memo to offset invoice balance
5. Credit memo is recorded in GL with account code GL-4215 "Bad Debt Expense"
6. Customer account is updated with status "Written Off – Uncollectible"
7. Account is retained in OEBS for 7 years for audit and potential future collection

### 4.3 Reinstatement of Written-Off Accounts
If a customer pays a written-off amount after write-off:
1. Payment is credited to "Recoveries – Prior Write-Offs" (GL-4216)
2. Original write-off memo is updated with recovery date and amount
3. Customer account status is updated to reflect partial/full recovery

### 4.4 Quarterly Write-Off Review
AR Director reviews all write-offs >$1,000 quarterly to ensure compliance with this policy. Deviations are investigated and reported to CFO.

---

## 5. Payment Plan and Compromise Procedures

### 5.1 Payment Plan Terms
Customers unable to pay in full may request a formalized payment plan:
- **Maximum duration:** 6 months
- **Minimum monthly payment:** $1,000 (or amount to clear balance within 6 months, whichever is greater)
- **Interest:** No additional interest charged; however, unpaid interest per original contract terms continues to accrue
- **Default:** Missed payment 15+ days triggers immediate acceleration of entire remaining balance; collections escalation continues

### 5.2 Payment Plan Approval
- Plans <$10,000: AR Manager approval
- Plans $10,001–$50,000: AR Director approval
- Plans >$50,000: CFO approval
- All payment plans are documented in a signed agreement; copy retained in customer file for 7 years

### 5.3 Compromise Settlement
In limited cases, AR Director (or CFO for >$50K) may approve acceptance of a payment less than the full amount owed:
- Only considered when customer is insolvent or bankrupt
- Minimum recovery must be ≥50% of current balance
- Business rationale must be documented (cost of collection >50% of balance, customer financial distress, time value of money)
- Requires CFO approval for any compromise >$25,000

---

## 6. Customer Credit Hold Procedures

### 6.1 Automatic Credit Holds
Credit holds are triggered automatically in OEBS when:
- Customer balance exceeds credit limit by any amount
- Account is >90 days past due
- Customer's D&B rating drops to "C" or lower
- Customer is >30 days past due AND balance >$50,000

### 6.2 Credit Hold Notification and Resolution
1. OEBS generates automatic alert to sales team and customer account manager
2. AR Credit Analyst contacts customer with payment/resolution options
3. Customer account notes are updated with hold reason and next follow-up date
4. Sales team is notified that new orders are not permitted until hold is released

### 6.3 Hold Release Procedures
- **Balance exceeds limit:** Hold released when balance is reduced to <100% of limit
- **Overdue >90 days:** Hold released when account is brought current or formal payment plan is established
- **D&B rating drop:** Hold released when updated rating is received and credit limit is re-evaluated
- **Large overdue balance:** Hold released only by AR Manager or higher authority

### 6.4 Duration and Escalation
- Holds that remain unresolved for >30 days are escalated to AR Director
- Holds that remain unresolved for >60 days are escalated to CFO
- Documentation of all hold resolution efforts is maintained in OEBS

---

## 7. Days Sales Outstanding (DSO) Target and Performance Monitoring

### 7.1 DSO Target
- **Company-wide DSO target:** 45 days or less
- **Industry benchmark:** 50 days (accounts for seasonal variations by business line)
- **Calculation:** (Accounts Receivable balance / Annual revenue) × 365

### 7.2 Monthly DSO Reporting
- AR Manager calculates DSO monthly and reports to CFO
- DSO is tracked by business line and customer segment
- Monthly DSO trend analysis identifies deterioration requiring investigation

### 7.3 DSO Performance Thresholds

| DSO Performance | Action |
|---|---|
| ≤ 45 days | On target; standard collections procedures |
| 46–55 days | Caution; investigate aging distribution; increase collection calls |
| 56–70 days | Alert; AR Director reviews collections strategy; increased management oversight |
| > 70 days | Escalation; CFO review; consider accounts receivable financing or factoring |

### 7.4 Root Cause Analysis
When DSO exceeds 50 days for >2 months:
1. AR Director analyzes aging by customer, product line, and region
2. Identifies customers with unusual delays
3. Investigates root causes (disputes, financial difficulty, system issues)
4. Develops action plan to address systemic issues
5. Reports findings and corrective actions to CFO

---

## 8. Oracle AR Dunning Letter Configuration and Automation

### 8.1 Dunning Rules in OEBS
- **Rule 1 (Day 5):** Automated dunning letter; "Amount Due: [invoice amount]; Due Date: [due date]"
- **Rule 2 (Day 15):** Second reminder letter; includes late fees per contract (if applicable)
- **Rule 3 (Day 25):** Final notice before escalation; includes AR contact information and payment arrangement options

### 8.2 Dunning Exceptions
Certain invoice types are excluded from automated dunning:
- Invoices on credit hold (manual follow-up only)
- Invoices subject to customer dispute (hold dunning pending resolution)
- Invoices in payment plan status (dunning suppressed; plan payment date used instead)

### 8.3 Dunning Message Customization
Dunning letters are customized by customer segment:
- **Preferred Customers:** Professional tone, emphasis on payment partnership, discount incentives
- **Standard Customers:** Standard template per AR Operations Manual
- **At-Risk Customers:** Firm tone; clear consequences and contact information for collections

### 8.4 Dunning Report and Effectiveness Tracking
- Monthly report of dunning letters sent, bounces, and payment response rates
- Effectiveness measured as % of invoices paid within 5 days of first dunning letter
- Dunning strategy is adjusted if effectiveness <40% for any customer segment

---

## 9. Dispute Resolution and Credit Memo Procedures

### 9.1 Customer Dispute Process
When a customer disputes an invoice:
1. Sales or customer service documents dispute reason in OEBS account notes
2. Invoice is placed on "Disputed – Hold for Investigation" status
3. Dunning is suppressed for disputed invoice
4. AR Credit Analyst investigates dispute (verifies shipment, quality, pricing)
5. Resolution is documented and agreed to in writing by both parties

### 9.2 Credit Memo Issuance
Valid disputes are resolved via credit memo:
- **Credit memo amount:** Must match documented dispute (no rounding up)
- **Approval authority:** <$500 AR Analyst; $500–$5,000 AR Manager; >$5,000 AR Director
- **Posting:** Credit memo is applied to dispute invoice, reducing customer balance
- **Documentation:** Supporting documentation (damage report, pricing correction) retained for 7 years

### 9.3 Disputed Invoice Aging
Disputed invoices are tracked separately from current receivables:
- Aged dispute report is reviewed monthly
- Disputes open >45 days require AR Manager escalation and resolution
- Disputes open >90 days require AR Director and customer executive escalation

---

## 10. Regulatory Compliance and Reporting

### 10.1 Financial Accounting Standards
This policy enforces compliance with:
- **ASC 326 (Credit Losses):** Allowance for doubtful accounts is calculated using historical loss rates and current aging analysis
- **ASC 606 (Revenue Recognition):** Credit decisions are made before revenue is recognized; collectibility is assessed
- **GAAP:** All write-offs, compromises, and disputes are documented and recorded in accordance with generally accepted accounting principles

### 10.2 Allowance for Doubtful Accounts
- Calculated quarterly using aging analysis and historical write-off rates
- Current aging: allowance rate = 1.5% for current, 3% for 30–59 days, 8% for 60–89 days, 25% for 90+ days
- Allowance is reconciled to actual write-offs; variance >10% triggers investigation

### 10.3 GDPR Data Protection
Customer payment data and contact information are processed in accordance with **GDPR_Data_Protection_Policy.md**:
- Phone numbers and email addresses collected for collection purposes are used only for that purpose
- Customer data is not shared with third parties without explicit consent (except collections agencies under contract)
- Payment plans and collection outcomes are retained for 7 years (business retention requirement); personal data deleted after customer account is closed (minimum 1 year) unless legal hold applies

### 10.4 Regulatory Reporting
- Monthly AR subledger is reconciled to GL account 1200 (Accounts Receivable)
- Quarterly aging analysis is provided to external auditors
- Annual write-offs and recoveries are reported to management and audit committee

---

## 11. Customer Communication and Dispute Prevention

### 11.1 Proactive Communication
- Invoice mailed or emailed within 2 business days of shipment
- Invoice includes clear due date, payment terms, discount terms (if applicable), and invoice number
- Invoice includes payment instructions: check mailing address, ACH routing information, online payment portal link

### 11.2 Disputes Prevention
- Customer service representatives are trained to document all customer agreements (pricing, terms, delivery dates) in OEBS
- Sales orders reflect agreed-upon terms; invoice is generated from sales order to ensure consistency
- Shipping documents match sales order to catch discrepancies at delivery

### 11.3 Payment Arrangement Options
- **Check:** Payment mailed to lockbox; cleared in 3–5 business days
- **ACH:** Direct debit from customer bank account; cleared in 1 business day
- **Credit Card:** Accepted for invoices <$10,000; 2.5% processing fee (passed through to customer)
- **Online Portal:** Self-service payment portal; supports check, ACH, and credit card
- **Wire Transfer:** Available for large payments; customer arranges directly

---

## 12. Roles and Responsibilities

| Role | Responsibility |
|---|---|
| **AR Credit Analyst** | Credit decisions ≤$50K, collection calls, dispute investigation, DSO reporting |
| **AR Collections Representative** | Day-to-day collection calls, dunning management, payment arrangement coordination |
| **AR Manager** | Collections supervision, approval of write-offs <$5K, escalation authority |
| **AR Director** | Collections policy oversight, approval of write-offs <$25K, customer escalations, DSO management |
| **Sales Team** | Timely order entry, customer contact, dispute notification |
| **CFO** | Approval of large write-offs and compromises, allowance for doubtful accounts review, policy exception authority |

---

## 13. Related Policies and References

- **AP_Invoice_Processing_Policy.md** – Vendor payment approval and controls
- **GDPR_Data_Protection_Policy.md** – Customer data privacy and handling
- **SOX_Compliance_Controls.md** – Segregation of duties and journal entry controls
- ASC 326 – Measurement of Credit Losses on Financial Instruments
- ASC 606 – Revenue from Contracts with Customers
- Oracle E-Business Suite Accounts Receivable User Guide
- Fair Debt Collection Practices Act (FDCPA) – Requirements for third-party collections agencies

---

## Document Revision History

| Date | Version | Changes | Author |
|---|---|---|---|
| 2025-01-20 | 1.0 | Initial policy creation | AR Director |
| 2025-08-10 | 1.5 | Updated DSO targets; added payment plan procedures | AR Manager |
| 2026-01-01 | 2.0 | Added GDPR compliance; updated dispute process | AR Director |
| 2026-03-15 | 2.2 | Clarified write-off thresholds; enhanced collections procedures; added OEBS configuration details | AR Manager |

---

**Approved by:** Chief Financial Officer
**Next Review Date:** January 1, 2027
