# GDPR Data Protection and Privacy Policy

**Document Owner:** Chief Privacy Officer / Legal Counsel
**Effective Date:** January 1, 2026
**Last Reviewed:** March 15, 2026
**Classification:** Internal Use Only
**Version:** 1.8

---

## 1. Purpose and Scope

This policy establishes procedures for processing personal data within the Oracle E-Business Suite (OEBS) and related systems in compliance with the General Data Protection Regulation (GDPR). This policy applies to all personal data processed by the organization, including customer information, vendor contacts, employee records, and third-party data subjects located in or related to the European Union (EU). All personnel, contractors, and third-party service providers must comply with this policy.

**Definitions:**
- **Personal Data:** Any information relating to an identified or identifiable natural person
- **Data Subject:** The person to whom personal data relates
- **Data Controller:** The entity determining the purposes and means of processing (typically our organization)
- **Data Processor:** Entity processing data on behalf of the controller (e.g., cloud service providers)
- **Processing:** Any operation performed on personal data (collection, storage, use, sharing, deletion)

---

## 2. Lawful Basis for Processing

### 2.1 Legal Bases Under GDPR Article 6
All processing of personal data must satisfy one of the following lawful bases:

**Basis 1: Contract Performance (Article 6(1)(b))**
- **Scope:** Processing necessary to perform a contract with the data subject
- **Examples:** Customer billing data, shipping addresses, purchase order contact information
- **Examples in OEBS:** Customer Master records, vendor contact data for order fulfillment
- **Retention:** Data retained for contract duration + 7 years (statute of limitations for business records)

**Basis 2: Legitimate Interest (Article 6(1)(f))**
- **Scope:** Processing necessary for legitimate interests of controller or third party, provided interests do not override data subject rights
- **Examples:** Fraud prevention, credit scoring, internal business analytics, vendor performance management
- **Examples in OEBS:** Customer credit history analysis (Collections module), vendor payment history for contract renewal
- **Legitimate Interest Assessment:** Prior to processing, organization conducts Legitimate Interest Assessment (LIA) documenting:
  - Purpose and necessity of processing
  - Data minimization steps taken
  - Data subject expectations
  - Impact on data subject rights
  - LIA is retained for minimum 3 years

**Basis 3: Compliance with Legal Obligation (Article 6(1)(c))**
- **Scope:** Processing required by law or regulatory requirement
- **Examples:** Tax reporting (VAT, income tax), accounting records (7-year retention), anti-money laundering (AML) checks
- **Examples in OEBS:** Customer tax identification numbers, vendor W-9 forms (U.S.) or VAT numbers (EU)

**Basis 4: Vital Interests (Article 6(1)(d))**
- **Scope:** Processing necessary to protect vital interests of data subject or another person
- **Application:** Rarely applicable in OEBS context; not generally used for business processing

**Basis 5: Consent (Article 6(1)(a))**
- **Scope:** Explicit, informed consent from data subject to process personal data
- **Standard:** Consent must be freely given, specific, informed, and unambiguous (not pre-ticked)
- **Examples:** Customer opt-in to marketing communications, employee consent to HR analytics
- **Documentation:** All consent is documented with date and method; consent can be withdrawn at any time

### 2.2 Special Categories of Personal Data (Article 9)
Processing of special categories (racial/ethnic origin, political opinions, religious beliefs, trade union membership, genetic data, biometric data, health data, sex life data) is prohibited EXCEPT when:
- Data subject has given explicit consent, OR
- Processing is necessary for employment law compliance (wages, benefits), OR
- Processing is for occupational health and safety

**OEBS Scope:** Special category data is NOT typically processed in OEBS. If discovered, processing is immediately halted and Legal Counsel is notified.

---

## 3. Data Subject Access Requests (DSAR) and Right to Access

### 3.1 DSAR Procedures
When a data subject requests access to their personal data:

**Receipt and Initial Processing:**
1. Request is received via email (privacy@company.com) or physical mail
2. Data Protection Officer (DPO) or Privacy Team acknowledges receipt within 5 business days
3. Identity of requestor is verified (copy of ID or other proof; for remote requests, e-ID validation)
4. Request is registered in DSAR log with date, subject name, and requested information

**Data Compilation (Days 1–20 of 30-day window):**
1. Privacy Team identifies all systems containing data subject's personal data (including OEBS, HR systems, email, documents)
2. OEBS queries are run using data subject identifier (customer ID, vendor ID, employee ID):
   - Accounts Receivable module: invoices, payments, credit records, communication history
   - Accounts Payable module: purchase orders, payments, communications
   - Procurement module: vendor master, contract records
   - General Ledger: transaction records with data subject references (if any)
3. Data compilation may include metadata (dates of interactions, user who accessed record)
4. Confidential data of third parties is redacted (e.g., other employee names in email chains, other vendor contact details)

**Response (Days 21–30 of 30-day window):**
1. Compiled data is provided to data subject in commonly used, portable format (PDF, CSV, or direct access to OEBS portal if applicable)
2. Response letter explains:
   - What data is held and where
   - Categories of recipients
   - Data retention schedule
   - Data subject rights (rectification, erasure, portability, objection)
   - Right to lodge complaint with supervisory authority (Data Protection Authority)
3. For complex requests (>1,000 records, >3 systems), timeline may be extended 60 additional days; data subject is notified of extension and reason

### 3.2 Exemptions to DSAR
Access may be restricted if:
- Request is manifestly unfounded, excessive, or repetitive
- Request could compromise rights of third parties
- Request conflicts with pending legal proceedings
- Organization has reasonable grounds to believe request is from unauthorized third party
Denial must be documented with specific legal basis and provided to data subject.

### 3.3 No Fee Policy
- DSARs are processed free of charge (no administrative fees)
- If requests are excessive or manifestly unfounded, reasonable administrative cost may be charged (capped at actual cost)

---

## 4. Right to Erasure and Right to Be Forgotten

### 4.1 Erasure Conditions
Data subjects have the right to erasure when:
- Personal data is no longer necessary for original purpose
- Data subject withdraws consent (if consent was lawful basis)
- Data subject objects to processing and legitimate interest is overridden
- Personal data was unlawfully processed
- Data subject is a child and consent was given without parental authorization

### 4.2 Limits to Erasure (CANNOT be deleted)
Certain data CANNOT be deleted even upon erasure request:

**Financial Records (7-year retention):**
- Customer invoices and payment records (tax compliance)
- Vendor invoices and purchase orders (financial audit)
- General Ledger transaction records (accounting standards)
- Basis: Legal obligation under tax law and SOX compliance (see **SOX_Compliance_Controls.md**)

**Legal and Regulatory Requirements:**
- Records subject to pending litigation or regulatory investigation
- Records required by law to be retained (e.g., AML records: 5+ years)
- Basis: Compliance with legal obligation (Article 6(1)(c))

**Fraud and Abuse Prevention:**
- Records necessary to prevent fraud, identity theft, or abuse
- Basis: Legitimate interest in protecting organization and other data subjects (Article 6(1)(f))

**Contractual Records (3 years post-termination):**
- Vendor contracts and related payment records (potential disputes, audits)
- Customer contracts and related records (warranty, service level disputes)
- Basis: Legal obligation and legitimate interest

### 4.3 OEBS Erasure Process
When erasure is approved (for data not subject to retention requirements):

1. **Anonymization** (preferred method): Personal identifiers are replaced with pseudonyms (e.g., Customer ID "CUST-12345" with no linked name)
   - This permits data to remain for historical/analytical purposes while removing personal linkage

2. **Deletion** (when anonymization not possible): Data is permanently deleted from OEBS:
   - OEBS provides soft delete (record marked as deleted; not recoverable by standard queries)
   - Backup copies are deleted after retention period expires (e.g., after 7 years for financial records subject to eventual deletion)

3. **Third-Party Notification**: Data Processors (cloud providers, payroll vendors) are notified of erasure requirement and must confirm deletion within 30 days

### 4.4 Right to Erasure Limitations Notification
If erasure is denied due to legal retention requirements, data subject is informed:
- Specific reason for retention
- Estimated duration of retention
- Right to lodge complaint with Data Protection Authority
- Right to restrict processing (see Section 5)

---

## 5. Data Minimization and Purpose Limitation

### 5.1 Data Minimization Principle
- Only collect personal data that is necessary for specified purpose
- Regularly review stored data and delete data that is no longer necessary
- Implement technical measures to limit access to minimum necessary data

**OEBS Implementation:**
- Customer Master record collects: name, address, contact, tax ID, credit limit, payment terms (necessary for invoicing)
- Customer Master does NOT collect: personal medical information, social security numbers, or other unrelated personal data
- Annual data audit: Finance team reviews Customer Master records and removes outdated or unnecessary fields

### 5.2 Purpose Limitation Principle
- Personal data collected for one purpose cannot be reused for new purpose without legal basis or explicit consent

**OEBS Implementation:**
- Customer phone numbers collected for dunning letters (Collections process) cannot be used for marketing without explicit opt-in consent
- Vendor contact information collected for purchase orders cannot be shared with HR for recruiting without vendor permission

### 5.3 Data Retention Schedules
The following retention schedule applies to OEBS data:

| Data Category | Retention Period | Legal Basis |
|---|---|---|
| **Customer Financial Records** | 7 years | Tax compliance, statute of limitations |
| **Customer Account (non-financial)** | 1 year after last transaction | Business convenience; after 1 year, data is anonymized |
| **Vendor Financial Records** | 7 years | Tax compliance, audit trail |
| **Vendor Master (active)** | Duration of relationship | Contract performance |
| **Vendor Master (inactive)** | 2 years | Business recovery, potential disputes |
| **Employee Records** | 3 years after separation | Employment law, wage/hour compliance |
| **Credit Scores & Risk Assessments** | Recalculated annually; old versions deleted | Legitimate interest in current credit status |
| **Dunning Letters & Collections Logs** | 7 years | Financial record retention, litigation defense |
| **DSAR Records & Responses** | 3 years | Audit trail of data subject rights exercise |

### 5.4 Automated Data Retention Execution
- OEBS is configured with automated purge jobs that execute deletion based on retention schedule
- Purge jobs run monthly; execution is logged and monitored
- Purge exceptions (data subject has pending DSAR, litigation hold) are documented and manually reviewed

---

## 6. Cross-Border Data Transfers

### 6.1 EU-US Data Privacy Framework
When personal data of EU data subjects is transferred to the United States:
- Organization relies on the EU-US Data Privacy Framework (adopted July 2023)
- Framework certifies adequate data protection in the U.S. for Privacy Shield certified organizations
- Organization confirms DPA status before engaging U.S.-based service providers

### 6.2 Standard Contractual Clauses (SCCs)
For transfers to non-adequacy jurisdictions (excluding Framework-certified orgs):
- Data Processing Agreements (DPAs) include Standard Contractual Clauses (Module One: Controller-to-Processor)
- SCCs include Supplementary Measures (encryption, access controls, audit rights) to mitigate transfer risk
- Supplementary Measures are reviewed annually and updated if jurisdiction's laws strengthen data protection

### 6.3 Third-Party Service Providers
When engaging OEBS vendors or cloud service providers (e.g., Oracle Cloud Infrastructure):
- **Oracle Corporation DPA:** Includes SCCs; data processing addendum executed before any data transfer
- **Subprocessors:** List of Oracle subprocessors is reviewed; organization has right to object to subprocessor changes
- **Data Location:** EU data is encrypted at rest and in transit; audit access by non-EU personnel is prohibited

### 6.4 Data Subject Notification
When processing involves transfer to non-adequate jurisdiction, data subjects are informed:
- During DSAR response process
- In privacy notices (e.g., customer terms and conditions)
- Risk mitigation measures in place (encryption, audit controls)

---

## 7. Data Breach Notification and Incident Response

### 7.1 Breach Definition and Scope
A personal data breach is defined as:
- Unauthorized or accidental access, disclosure, alteration, or loss of personal data
- Examples: Data theft, ransomware attack, unauthorized employee access, accidental exposure in email, data sent to wrong recipient

**OEBS Scope:** Breaches involving customer data, vendor data, or employee data stored in OEBS, databases, or backups.

### 7.2 Incident Detection and Reporting
- All employees are trained to report suspected breaches immediately to IT Security team (security@company.com)
- IT Security assesses breach scope within 24 hours
- Determination is made: Is this a reportable breach under GDPR?

**Reportable = Assessment confirms:**
- Unauthorized processing occurred, AND
- There is substantial risk of harm to data subjects (e.g., unauthorized access to payment data, medical data, personal identifiers)

**Non-reportable = Assessment confirms:**
- Unauthorized access did not occur (e.g., data already deleted, encryption prevents access), AND/OR
- Substantial risk of harm is unlikely (e.g., limited disclosure to trusted party, data could not be correlated)

### 7.3 Notification Timeline (72-Hour Rule)

**Hour 0–24 (Internal Investigation):**
- IT Security investigates scope and nature of breach
- Determines: What data was compromised? Who accessed it? Duration of unauthorized access?
- Determines: Can breach be contained? What mitigation is needed?
- Notifies CFO and Legal Counsel

**Hour 24–48 (Authority Notification Preparation):**
- If reportable, Legal prepares notification to Data Protection Authority (DPA)
- Notification includes: Description of breach, likely consequences, measures taken/proposed
- CFO/Legal approves notification content

**Hour 48–72 (DPA Notification):**
- Notification is submitted to relevant DPA (e.g., Ireland's Data Protection Commission for EU data)
- Submission is documented with timestamp and confirmation number
- Notification is sent to organization's supervisory authority

### 7.4 Data Subject Notification
If breach poses high risk of harm, data subjects are notified (without undue delay, as soon as feasible):
- Notification is sent via email or phone (contact information from OEBS records)
- Notification includes:
  - Nature of breach and personal data involved
  - Likely consequences
  - Measures taken to mitigate risk (e.g., access revoked, systems patched)
  - Recommendations: password change, credit monitoring, fraud alert
  - Contact for questions: privacy@company.com
- Notification is documented; copy is retained with incident record

**Exception:** Notification may be omitted if:
- Data is encrypted and encryption key was not compromised
- Data was not actually accessed (e.g., hard drive stolen but encrypted)
- Organization has implemented mitigating measures rendering breach inconsequential

### 7.5 Incident Record and Root Cause Analysis
All breaches (reportable and non-reportable) are documented:
- Incident record includes: date, time, nature, scope, data subjects affected, cause, containment measures
- Root cause analysis is completed within 2 weeks
- Corrective actions are identified and tracked to completion
- Post-incident review: measures to prevent similar breach are implemented

---

## 8. Data Protection Impact Assessment (DPIA)

### 8.1 When DPIA is Required
A DPIA is mandatory for new processing activities involving:
- Large-scale processing of special category data
- Systematic monitoring or profiling
- Automated decision-making with legal/significant impact
- Sharing of data with third parties for new purposes
- Processing of biometric or genetic data
- Processing involving cross-border transfers

**OEBS Context:** Example — implementing new AI/ML-based credit scoring in Collections module would require DPIA.

### 8.2 DPIA Process

**Step 1: Initiation (Week 1)**
- Business owner submits request describing new processing activity
- DPO reviews and confirms DPIA necessity
- DPIA team is assembled (Privacy Officer, IT Security, Business Stakeholder, Legal)

**Step 2: Impact Analysis (Weeks 2–3)**
- Describe processing activity: lawful basis, data categories, data subjects, recipients, retention
- Identify risks: risks to data subject rights, risks to confidentiality/integrity/availability of data
- Assess risks: likelihood and severity (low/medium/high)
- Document mitigating measures: technical controls (encryption, access controls), organizational controls (training, contracts)

**Step 3: Consultation (Week 4)**
- DPIA findings are shared with Data Protection Authority if high-risk processing identified
- Authority provides opinion within 6 weeks (or may require additional measures)

**Step 4: Approval and Implementation (Week 5)**
- DPIA is reviewed and approved by Legal Counsel and CFO
- Approved DPIA is retained for minimum 3 years
- Mitigating measures are implemented before processing commences

### 8.3 DPIA Template and Documentation
DPIAs are documented using standard template:
- Processing activity name and description
- Lawful basis
- Necessity and proportionality assessment
- Data subject rights impact analysis
- Third-party impact analysis
- Risk identification and mitigation
- Conclusion and approval sign-off

---

## 9. Data Subject Rights Support and Procedures

### 9.1 Right to Rectification
Data subjects can request correction of inaccurate personal data:
- Request is submitted to privacy@company.com
- AR or AP team verifies accuracy of OEBS records
- Correction is made within 10 business days
- If data subject disputes correction, note of dispute is added to record
- No cost to data subject

### 9.2 Right to Restrict Processing
Data subjects can request that processing be limited to storage only (no use):
- Request is submitted to privacy@company.com
- Processing restriction is flagged in OEBS ("Data subject has requested restriction")
- Data is not used for any purpose except storage and litigation defense
- Restriction remains until data subject withdraws request or legal basis expires
- Data subject is informed of restriction status and options

### 9.3 Right to Data Portability
Data subjects can request their data in machine-readable format for transfer to another organization:
- Request is submitted with data subject identification
- Privacy Team compiles data and provides in CSV or JSON format within 30 days
- Data includes contact information, transaction history, and all personal identifiers
- Data subject can then provide to another organization without restriction

### 9.4 Right to Object
Data subjects can object to processing based on legitimate interest:
- Request is submitted stating objection reason
- Legitimate interest is re-evaluated
- If legitimate interest cannot be justified to override data subject objection, processing stops
- If legitimate interest is justified, data subject is informed and may lodge complaint with DPA

### 9.5 Automated Decision-Making and Profiling
- Organization does not use purely automated decision-making to determine credit limits, denial of service, or significant impact to data subject
- Credit decisions involve human review by AR Credit Analyst; automated scoring informs decision but does not dictate outcome
- Data subjects have right to human review of any automated decision

---

## 10. Privacy Notices and Transparency

### 10.1 Privacy Notice Content
Privacy notices are provided to customers and vendors at point of collection:
- Identity of controller and DPO contact information
- Purposes and lawful basis for processing
- Recipients of data (third parties, subprocessors)
- Data retention schedule
- Data subject rights and how to exercise them
- Right to lodge complaint with Data Protection Authority
- Consequences of not providing data (e.g., cannot process invoice without vendor address)

### 10.2 Customer Privacy Notice (Point of Sale)
- Provided with invoice or order confirmation
- States: "We collect your name, address, and payment information to fulfill your order and send invoices. We retain this data for 7 years for tax compliance. You have the right to access, correct, or request deletion of your data."
- Includes contact: privacy@company.com

### 10.3 Vendor Privacy Notice (Onboarding)
- Provided with vendor setup form or contract
- States: "We process your company name, contact, tax ID, and payment information for invoice processing and tax compliance. We retain data for 7 years."
- References this GDPR policy
- Includes DPA provisions

---

## 11. Data Protection Officer (DPO) Role and Responsibilities

### 11.1 DPO Designation
- Organization has appointed a Data Protection Officer to oversee GDPR compliance
- DPO is independent and reports directly to CFO/Board
- DPO cannot be removed without cause

### 11.2 DPO Responsibilities
- Monitor GDPR compliance across organization
- Advise on lawful basis for processing activities
- Develop and update data protection policies
- Respond to data subject requests
- Conduct DSARs and data breach investigations
- Ensure Data Processing Agreements are in place with third parties
- Conduct DPIAs for high-risk processing
- Train employees on GDPR requirements
- Coordinate with Data Protection Authority

### 11.3 DPO Contact
- Email: privacy@company.com (monitored by DPO team)
- Phone: [Internal extension for Data Protection team]
- Data subjects and employees can contact DPO directly with privacy concerns

---

## 12. Training and Compliance Monitoring

### 12.1 GDPR Training
- All employees receive GDPR training at onboarding
- Annual refresher training is mandatory (1 hour)
- Finance team (AP/AR) receives specialized training on OEBS data handling and retention
- Training covers: data minimization, lawful basis, data subject rights, breach reporting, DPA obligations

### 12.2 Compliance Audit and Monitoring
- Annual GDPR compliance audit by Internal Audit or External Counsel
- Audit covers: lawful basis documentation, DPA compliance, data retention adherence, breach incident management
- Audit findings are reported to CFO and Audit Committee
- Compliance metrics: % of DPIAs completed, DSAR response time, breach notification timeline, training completion rate

### 12.3 Non-Compliance Consequences
- Failure to comply with GDPR policies may result in disciplinary action (up to termination)
- Unauthorized access or disclosure of personal data is treated as data breach
- Repeated violations may result in regulatory reporting to Data Protection Authority

---

## 13. Relationship to Other Policies

- **AP_Invoice_Processing_Policy.md** – Vendor contact data retention and use
- **Collections_and_Credit_Policy.md** – Customer communication and data use in collections
- **SOX_Compliance_Controls.md** – Audit trail and system access controls
- **Procurement_and_Vendor_Management.md** – Vendor data collection and processing

---

## 14. Related Regulations and References

- **General Data Protection Regulation (GDPR)** – EU Regulation 2016/679
- **EU-US Data Privacy Framework** – Schrems II adequacy decision (2023)
- **Standard Contractual Clauses (SCCs)** – EU Model Contracts for data transfer
- **Data Protection Impact Assessment Template** – EU Article 35 DPIA guidance
- **International Data Transfer Impact Assessment (DTIA)** – UK ICO guidance
- **Oracle Data Privacy and Security Addendum** – Oracle Cloud Infrastructure data terms

---

## Document Revision History

| Date | Version | Changes | Author |
|---|---|---|---|
| 2025-01-10 | 1.0 | Initial policy creation | Chief Privacy Officer |
| 2025-04-15 | 1.3 | Added EU-US Data Privacy Framework references | Legal Counsel |
| 2025-09-20 | 1.5 | Enhanced OEBS-specific procedures; added DPIA requirements | DPO |
| 2026-01-01 | 1.7 | Updated retention schedules; clarified erasure limits | Chief Privacy Officer |
| 2026-03-15 | 1.8 | Clarified financial record retention (7 years); added breach notification procedures | Legal Counsel |

---

**Approved by:** Chief Financial Officer, Chief Privacy Officer, General Counsel
**Next Review Date:** January 1, 2027
