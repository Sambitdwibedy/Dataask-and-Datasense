# Procurement and Vendor Management Policy

**Document Owner:** Director, Procurement & Vendor Management
**Effective Date:** January 1, 2026
**Last Reviewed:** March 15, 2026
**Classification:** Internal Use Only
**Version:** 2.1

---

## 1. Purpose and Scope

This policy establishes procurement procedures and vendor management standards for the organization. The policy governs all purchases of goods and services, vendor selection, contract management, and performance evaluation. All personnel involved in procurement, from requisition initiation through payment, must comply with this policy. This policy applies across all business units, legal entities, and geographies. The primary objectives are to ensure competitive bidding, maintain vendor quality, manage costs, and comply with regulatory requirements.

---

## 2. Vendor Qualification and Onboarding

### 2.1 Vendor Eligibility Requirements
All vendors must meet the following baseline requirements before being activated in the Oracle E-Business Suite (OEBS) Procurement module:

**Legal and Compliance:**
- Valid business license or proof of legal entity status
- Tax identification number (EIN, VAT number, or equivalent)
- Proof of insurance (General Liability minimum $1M; Workers Compensation if applicable)
- Conflict of interest certification (vendor certifies no conflict with organization's stakeholders)

**Financial Stability:**
- Dun & Bradstreet rating OR Credit reference check
- Business establishment date (must be operational for ≥2 years)
- For vendors >$100K annual spend: Financial statement review (last 2 years tax returns or audited statements)

**Regulatory Compliance:**
- Compliance with industry certifications (ISO, medical device approvals, etc., if applicable to services)
- Proof of compliance with SOX/FCPA (if U.S. public company vendor) or equivalent
- No vendor is activated if appears on OFAC (Office of Foreign Assets Control) sanctions list

**Quality Certifications (if applicable):**
- Quality certifications relevant to services (ISO 9001, AS/EN 9100 for aerospace, GMP for pharmaceutical, etc.)
- Safety certifications if services involve workplace safety

**Diversity Status (self-identification):**
- Minority-owned business enterprise (MBE)
- Woman-owned business enterprise (WBE)
- Small business enterprise (SBE)
- Service-disabled veteran-owned small business (SDVOSB)
- Diversity status is tracked for reporting and spend analytics

### 2.2 Vendor Onboarding Process

**Step 1: Vendor Profile Creation (Days 1–3)**
- Procurement Specialist collects vendor information:
  - Company name, address, contact information
  - Tax ID, banking information (for payment processing)
  - Insurance certificates, compliance documentation
  - Authorized approvers for POs and invoices
- Information is entered into OEBS Vendor Master

**Step 2: Compliance Review (Days 4–5)**
- Procurement Compliance Officer reviews:
  - Insurance compliance (coverage limits, active policies)
  - Sanctions list screening (OFAC, UN, EU, UK lists)
  - Regulatory compliance based on service type
- Compliance review is documented in Vendor Master; any deficiencies are resolved before activation

**Step 3: Finance Review (Days 6–7)**
- AP Manager confirms:
  - Banking information is valid (vendor account holder matches vendor name)
  - No duplicate vendor records exist in system
  - Credit reference check completed (for vendors >$50K annual spend)
- AP Manager adds payment terms and credit limits to Vendor Master

**Step 4: Activation (Day 8)**
- Procurement Manager approves vendor activation
- Vendor status is set to "Active" in OEBS
- Vendor is notified of activation and provided:
  - PO process and expectations
  - Payment terms and conditions
  - Performance metrics and reporting requirements
  - Contact information for order and billing inquiries

### 2.3 Vendor Master Maintenance
Vendor Master records are reviewed annually:
- Contact information is verified and updated
- Insurance certificates are verified as current
- Payment terms are re-evaluated based on spending patterns
- Performance history (Section 5) is reviewed to determine tier status
- Any changes in vendor status are documented in Vendor Master
- Inactive vendors (no purchase activity for 2+ years) are archived

---

## 3. Competitive Bidding Requirements

### 3.1 Procurement Approval Thresholds and Bidding Requirements

| Purchase Amount | Bidding Requirement | Approver | Documentation |
|---|---|---|---|
| < $10,000 | Direct purchase (single-quote) | Procurement Specialist | Single quote obtained from qualified vendor |
| $10,000 – $49,999 | 3 Competitive Quotes | Procurement Manager | Written quotes from 3 vendors; comparison matrix |
| $50,000 – $250,000 | Formal RFP (Request for Proposal) | Director of Procurement | RFP document; evaluation scorecard; award justification |
| > $250,000 | Formal RFP + Management Approval | VP Supply Chain + CFO | RFP document; detailed evaluation; CFO sign-off on pricing |

### 3.2 Direct Purchase Procedures (<$10,000)
- Department submits purchase requisition to Procurement Specialist
- Specialist confirms vendor is approved and pricing is reasonable
- Specialist creates PO in OEBS and routes for approval per **AP_Invoice_Processing_Policy.md**
- Single quote does not require formal documentation but pricing should be consistent with historical rates

### 3.3 Competitive Bidding Procedures ($10,000–$49,999)
- Department submits requisition with specifications to Procurement Manager
- Procurement Manager identifies 3 qualified vendors capable of fulfilling requirement
- Manager requests written quote from each vendor with:
  - Detailed specifications matching requirement
  - Unit pricing and total price
  - Delivery timeline
  - Terms and conditions
  - Reference customers (for services)
- Quotes are received within 5 business days
- Procurement Manager creates comparison matrix:
  - Pricing comparison (identify lowest-cost option)
  - Quality/specifications review (confirm each vendor meets requirements)
  - Delivery timeline comparison
  - Vendor capability review (experience, certifications, references)
- Award is made to vendor with best overall value (lowest cost meeting all requirements)
- Award justification is documented in OEBS; losing bidders are notified

### 3.4 Formal RFP Procedures ($50,000+)

**RFP Development (Weeks 1–2):**
- Cross-functional team (Procurement, Operations, Finance, Technical) develops RFP document
- RFP includes:
  - Business requirements and technical specifications
  - Service level expectations (availability, response times, quality metrics)
  - Contract terms and conditions (payment terms, insurance, liability, IP ownership)
  - Evaluation criteria and scoring weights (e.g., Price 40%, Quality 30%, Delivery 20%, References 10%)
  - Proposed contract timeline and milestones
  - Instructions for submission (format, deadline, submission method)
- RFP is approved by Director of Procurement and CFO
- Minimum of 5 qualified vendors are identified and invited to bid
- RFP is distributed with 15-day response window

**RFP Response and Evaluation (Weeks 3–6):**
- Vendors submit proposals by RFP deadline
- Late submissions are rejected automatically
- Procurement team logs all submissions and confirms completeness
- Evaluation committee (Procurement, Operations, Finance, and subject matter experts) scores proposals using evaluation scorecard
- Scoring methodology:
  - Price: Lowest-cost compliant proposal receives full points; others pro-rated
  - Quality/Capability: Evaluated against requirements; points awarded for exceeding requirements
  - References: References are contacted; feedback informs scoring
  - Total score determines ranking
- Evaluation results are reviewed by CFO; final vendor selection is approved by CFO and VP Supply Chain

**RFP Award and Contract (Week 7–8):**
- Selected vendor is notified of award
- Losing vendors are notified with feedback on their proposal
- Contract is negotiated based on RFP terms and proposal
- Contract is reviewed by Legal and signed by both parties
- PO is created in OEBS referencing contract
- Vendor is provided with kickoff meeting with stakeholders

### 3.5 Sole Source Justification
Purchases may be made from a single vendor (bypassing competitive bidding) only when:
- Vendor is the only source capable of meeting requirement (manufacturer-specific part, proprietary software, exclusive service)
- Emergency procurement is required and competitive bidding would cause unacceptable delay
- Continuing a prior contract offers best value due to switching costs, integration, or customer experience

**Sole Source Approval:**
- <$25K: Procurement Manager approval and documented justification
- $25K–$100K: Director of Procurement approval and documented justification
- >$100K: VP Supply Chain and CFO approval with detailed business case

Sole source justifications are retained in Procurement file for audit review.

---

## 4. Purchase Order Creation and Approval

### 4.1 PO Approval Matrix
Purchase Orders are created in OEBS Procurement module and routed for approval based on amount:

| PO Amount | Approver | Workflow Timing |
|---|---|---|
| < $5,000 | Procurement Specialist (auto-approve) | Approved upon creation if vendor is qualified |
| $5,000 – $24,999 | Department Manager | Routed to manager; approval required within 3 business days |
| $25,000 – $99,999 | Department Director | Routed to director; approval required within 3 business days |
| ≥ $100,000 | VP Supply Chain + CFO | Dual approval required within 5 business days |

### 4.2 Required PO Fields
All POs in OEBS must include:
- Vendor Name and ID (from Vendor Master)
- Ship-to Location and Address
- Bill-to Address and Cost Center
- Line items with:
  - Description of goods/services
  - Part number (if applicable)
  - Unit of measure (each, case, hour, month, etc.)
  - Unit price and quantity
  - Total line amount
  - GL Account coding (cost center, project code if applicable)
- Delivery date or service period
- Payment terms (Net 30, 2/10 Net 30, etc.)
- Terms and conditions (reference standard T&Cs or attached special terms)
- Special instructions or delivery notes

### 4.3 PO Issuance
- Upon final approval, PO is automatically issued to vendor via email with OEBS-generated PDF
- Vendor is expected to acknowledge receipt and confirm ability to fulfill
- PO number is provided to vendor and used for all communications
- Physical POs may be issued for sensitive items or when vendor requires signed copy

### 4.4 PO Changes and Amendments
Changes to approved POs must follow change management process:
- **Minor changes** (delivery date, small quantity adjustment <10%): Procurement Specialist approval
- **Moderate changes** (price change <5%, specification change): Procurement Manager approval
- **Significant changes** (price change >5%, scope change): Director or VP approval (same as original PO amount)
- Change order is documented in OEBS with approval trail
- Vendor is notified of changes in writing; confirmation of acceptance is obtained

---

## 5. Vendor Performance Management and Scorecards

### 5.1 Performance Scorecard Framework
Vendors generating >$50K annual spend are evaluated quarterly using standardized scorecard:

**Five Evaluation Categories:**
1. **Quality** (weight: 30%) – Quality of goods/services delivered, defect rate, rework required
2. **Delivery** (weight: 25%) – On-time delivery percentage, lead time performance, responsiveness to schedule changes
3. **Price/Cost Management** (weight: 20%) – Competitive pricing, compliance with negotiated rates, cost reduction proposals
4. **Responsiveness** (weight: 15%) – Communication and responsiveness to inquiries, problem resolution speed, flexibility
5. **Compliance** (weight: 10%) – Regulatory compliance, safety record, documentation completeness, audit performance

### 5.2 Scorecard Metrics

| Category | Metric | Target | Data Source |
|---|---|---|---|
| **Quality** | Defect rate (% of received units with defects) | ≤2% | Receiving inspection records |
| | Returns/rework cost as % of purchases | <1% | Finance records |
| **Delivery** | On-time delivery (% of deliveries meeting agreed date ±2 days) | ≥95% | Goods receipt records |
| | Average lead time vs. promised | Within 5% | PO vs. receipt dates |
| **Price** | Cost variance (actual vs. negotiated) | 0–2% favorable | Invoice analysis |
| | Cost reduction proposals submitted | ≥1 per year | Project tracking |
| **Responsiveness** | Response time to inquiries (hours) | ≤24 hours | Email/call logs |
| | Problem resolution rate within 5 days | ≥90% | Issue log |
| **Compliance** | Insurance current and compliant | 100% | Certificate verification |
| | Regulatory audit findings | 0 critical findings | Audit reports |

### 5.3 Quarterly Scorecard Evaluation Process

**Week 1:** Data Collection
- Procurement Analyst collects quarterly performance data from:
  - Receiving/QA: Quality and delivery metrics
  - Finance: Price/cost variance analysis
  - Operational teams: Responsiveness feedback
  - Compliance: Insurance and regulatory status

**Week 2:** Scoring
- Procurement Manager scores vendor on each metric (1–5 scale):
  - 5 = Exceeds expectations
  - 4 = Meets expectations
  - 3 = Acceptable with minor issues
  - 2 = Below expectations; improvement needed
  - 1 = Unacceptable; significant problems
- Weighted score is calculated: (Quality × 0.30) + (Delivery × 0.25) + (Price × 0.20) + (Responsiveness × 0.15) + (Compliance × 0.10)
- Overall score ranges from 1.0 (lowest) to 5.0 (highest)

**Week 3:** Vendor Communication
- Vendor is notified of scorecard results
- Scores ≥4.0: Vendor is recognized as strong performer (preferred vendor status)
- Scores 3.0–3.9: Vendor is notified of improvement areas; improvement plan is requested
- Scores <3.0: Vendor receives formal improvement notice; 90-day improvement plan is required

### 5.4 Preferred Vendor Program and Tiering

**Tier Structure:**

| Tier | Score Range | Benefits | Conditions |
|---|---|---|---|
| **Platinum** | 4.8–5.0 | Preferred status; priority for new RFPs; price increases capped at inflation; volume commitments | Maintain score for 2+ consecutive quarters |
| **Gold** | 4.5–4.7 | Preferred status; eligible for RFP participation; quarterly business reviews | Maintain score for 2+ quarters; improve to Platinum-level |
| **Silver** | 4.0–4.4 | Standard vendor status; full RFP participation | Maintain or improve score |
| **Standard** | 3.0–3.9 | Conditional status; improvement plan required; RFP participation | Improvement plan agreed; score trending up |
| **At-Risk** | <3.0 | Probationary status; improvement plan required; no new commitments; RFP participation suspended | 90-day improvement plan with specific metrics |

### 5.5 Vendor Performance Reviews
Annual vendor reviews are conducted with all significant vendors (>$50K spend):
- Comprehensive review of annual performance across all scorecards and categories
- Discussion of strategic opportunities, cost reduction initiatives, and service improvements
- Contract renewal evaluation and pricing discussions
- At-risk vendors may face contract non-renewal if performance does not improve

---

## 6. Contract Review and Renewal Procedures

### 6.1 Contract Management Process
All vendor contracts >$10,000 or >1-year term are:
- Drafted using OEBS-linked contract template or negotiated agreement
- Reviewed by Legal department (contract terms, liability, insurance, IP ownership)
- Reviewed by Finance department (payment terms, cost escalation, penalties)
- Reviewed by Operations department (service levels, performance metrics)
- Approved by appropriate authority (Procurement Manager for <$50K, Director for $50K–$250K, VP for >$250K)
- Executed by both parties and retained in contract repository (OEBS or document management system)

### 6.2 Key Contract Terms
Standard contract terms include:
- **Scope of Work:** Detailed description of goods/services with specifications
- **Payment Terms:** Pricing, payment schedule, invoicing procedures
- **Term and Renewal:** Contract duration, renewal options, termination provisions
- **Service Level Agreements (SLAs):** Response times, availability, quality standards with penalties for non-compliance
- **Insurance:** Required coverage types and limits; vendor certificates of insurance
- **Confidentiality:** Protection of both parties' confidential information
- **Intellectual Property:** Ownership of work product and any IP created
- **Liability Limitations:** Cap on vendor liability (typically not less than amount paid in prior 12 months)
- **Indemnification:** Vendor indemnifies organization for third-party claims
- **Compliance:** Compliance with laws, regulations, SOX, FCPA, sanctions lists, data protection laws
- **Audit Rights:** Organization may audit vendor's books and records
- **Termination:** Termination for convenience (60 days notice) and for cause (immediate, if vendor breaches)

### 6.3 Contract Renewal Procedures
90 days prior to contract expiration:
- Procurement Manager initiates renewal evaluation:
  - Review vendor performance scorecard (last 2 years)
  - Assess market pricing for similar services
  - Determine if competitive bidding is appropriate
- **If renewal:** Negotiate updated terms, pricing, and term; execute amended contract
- **If non-renewal:** Notify vendor 90 days prior; transition to new vendor
- **If non-renewal due to performance:** Provide vendor formal notice with performance deficiencies and remediation opportunity; allow 30-day cure period if applicable

---

## 7. Emergency Procurement Procedures

### 7.1 Emergency Procurement Definition
Emergency procurement is authorized when:
- Unplanned equipment failure requires immediate replacement to avoid business interruption
- Urgent customer need requires accelerated delivery (market opportunity)
- Supply chain disruption threatens critical material availability
- Emergency is documented with specific business justification

### 7.2 Emergency Procurement Process
- Department Head submits emergency procurement request with:
  - Description of emergency and business impact
  - Required delivery timeline
  - Vendor recommendation and justification
  - Estimated cost
- Director of Procurement approves emergency; normal competitive bidding is waived
- PO is issued immediately to selected vendor
- Post-purchase review: Within 10 business days, competitive quotes are obtained to verify emergency vendor pricing was reasonable
- If pricing significantly exceeds market rate (>15%), surplus cost is flagged and discussed with department; future emergencies are addressed with vendor on-call agreements

### 7.3 Vendor On-Call Agreements
For recurring emergency needs, on-call agreements are established:
- Vendor agrees to maintain inventory and support emergency requests
- Emergency pricing is pre-negotiated and capped
- Response time (same-day or next-day delivery) is specified
- Annual retainer fee may be paid for availability guarantee

---

## 8. Vendor Data Management and GDPR Compliance

### 8.1 Vendor Data Collected in OEBS
The Vendor Master record captures:
- Company name, address, contact information (business contact, not personal)
- Tax ID, banking information
- Insurance information and certificates
- Performance data and scorecards
- Contract and pricing information

### 8.2 GDPR Compliance
Vendor contact data is processed in accordance with **GDPR_Data_Protection_Policy.md**:
- **Lawful Basis:** Contract performance (Article 6(1)(b)) — data is necessary to perform vendor relationship and process orders/payments
- **Data Minimization:** Only business contact information is collected; personal data (personal phone, personal email) is not collected unless explicitly provided by vendor
- **Retention:** Vendor data is retained while relationship is active; inactive vendors are archived 2 years after last transaction
- **Data Subject Access:** If vendor contact person requests access to their personal data (as data subject), request is processed per DSAR procedures in GDPR policy
- **Confidentiality:** Vendor contact information is not shared with third parties without vendor consent (except with banks/payment processors under data processing agreements)

---

## 9. Purchase Compliance and Regulatory Requirements

### 9.1 SOX Compliance Controls
Procurement processes include SOX compliance controls:
- **Segregation of Duties:** Requisition created by user, approved by manager, PO created by Procurement, goods received by Receiving, invoice processed by AP, payment released by Treasurer (see **SOX_Compliance_Controls.md**)
- **Approval Authority:** PO approval authority based on amount (segregated from other transaction approvals)
- **Audit Trail:** OEBS captures complete audit trail of requisition, PO creation, approval, receipt, and payment
- **Three-Way Match:** Invoice must match PO and goods receipt before payment (see **AP_Invoice_Processing_Policy.md**)

### 9.2 Regulatory Compliance
Procurement processes enforce:
- **Anti-Bribery Compliance:** Vendors certify compliance with FCPA (Foreign Corrupt Practices Act); no gifts or entertainment from vendors >$50 in value
- **Sanctions Compliance:** All vendors screened against OFAC sanctions lists; no business with sanctioned entities or individuals
- **Conflict of Interest:** Vendors and procurement personnel certify no conflicts of interest
- **Environmental Compliance:** Suppliers of certain goods certify environmental compliance (e.g., conflict minerals, hazardous substances)

### 9.3 Diversity and Supplier Development
- Spend analysis by vendor diversity status (MBE, WBE, SBE, SDVOSB)
- Diversity spend targets are established and monitored annually
- Diversity vendors are prioritized for RFP participation when capabilities match
- Supplier development programs support diverse vendor growth and capability

---

## 10. Roles and Responsibilities

| Role | Responsibility |
|---|---|
| **Department Head/Manager** | Initiates purchase requisitions; approves purchases within authorized limits; identifies vendor needs |
| **Procurement Specialist** | Vendor research, quote requests, PO creation <$5K; vendor master maintenance |
| **Procurement Manager** | Competitive bidding (3-quote), RFP management, contract negotiation, vendor approval, scorecard evaluation |
| **Director of Procurement** | Approval authority for large POs ($25K–$250K), formal RFPs, strategic vendor relationships, tier management |
| **Vendor Compliance Officer** | Vendor qualification, insurance verification, sanctions screening, compliance documentation |
| **Finance / AP Manager** | Vendor master setup, payment terms, invoice processing, budget tracking |
| **VP Supply Chain** | Strategic sourcing, category management, vendor tier governance, CFO approval coordination |
| **CFO** | Approval of large contracts (>$250K), strategic vendor relationships, policy oversight |

---

## 11. Related Policies and References

- **AP_Invoice_Processing_Policy.md** – AP approval and three-way match requirements
- **SOX_Compliance_Controls.md** – Segregation of duties and audit trail requirements
- **GDPR_Data_Protection_Policy.md** – Vendor data privacy and handling
- Oracle E-Business Suite Procurement User Guide
- Standard Procurement Terms and Conditions (attached as Appendix A)
- Conflict of Interest Policy (General Company Policy)
- Anti-Corruption Compliance Policy (SOX requirement)

---

## Document Revision History

| Date | Version | Changes | Author |
|---|---|---|---|
| 2025-01-20 | 1.0 | Initial policy creation | Director of Procurement |
| 2025-06-15 | 1.5 | Enhanced vendor onboarding procedures; added tier management | Procurement Manager |
| 2025-10-01 | 1.8 | Added emergency procurement procedures; expanded contract renewal process | VP Supply Chain |
| 2026-01-10 | 2.0 | Added GDPR compliance procedures; cross-referenced SOX controls | Director of Procurement |
| 2026-03-15 | 2.1 | Clarified sole-source justification; enhanced vendor performance metrics | Procurement Manager |

---

**Approved by:** Chief Financial Officer, VP Supply Chain
**Next Review Date:** January 1, 2027
