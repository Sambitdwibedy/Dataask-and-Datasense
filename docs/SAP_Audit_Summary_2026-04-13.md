# SAP ECC 6.0 Prototype — Audit Summary (v0.4.73)

**Date:** April 13, 2026
**Version:** v0.4.73 (final)
**App ID:** 5 (Data Ask) / Data Sense AKG Builder
**Status:** Published and Live

---

## Pipeline Summary

| Metric | v0.4.70 (Baseline) | v0.4.73 (Final) | Delta |
|--------|-------------------|-----------------|-------|
| Tables | 124 | 128 (+4 DD metadata) | +4 |
| Columns | 2052 | 2078 | +26 |
| Relationships | 489 | 622 | **+133 (+27%)** |
| Synonyms | ~7800 | 7886 | +86 |
| QPD Tests | 178/178 (100%) | 178/178 (100%) | — |
| Manual Queries | 21/23 (91.3%) | 21/23 (91.3%) | — |
| Context Documents | 0 | 2 (schema ref) | +2 |

## Relationship Quality Breakdown

| Strategy | Confidence | Count | Description |
|----------|-----------|-------|-------------|
| Strategy 3b (value overlap) | 80 | 475 | Shared column names with value overlap confirmation |
| Strategy 3b (strong) | 85 | 14 | Higher confidence overlap matches |
| Strategy 4 (DD declared FKs) | 95 | 133 | From DD05S/DD08L metadata tables |
| **Total** | | **622** | |

## DD Metadata Tables Added

| Table | Records | Purpose |
|-------|---------|---------|
| DD02T | 124 | Table descriptions (all 124 SAP tables) |
| DD08L | 350 | FK definitions (declared foreign keys) |
| DD05S | 350 | FK field assignments (column-level FK mapping) |
| DD03L | 265 | Field definitions (key columns with types) |

Source: LeanX SAP R/3 ERP documentation (confirmed ECC 6.0, NOT S/4HANA)

## Manual Query Test Results (23 queries)

### Finance (FI) — 4/4 PASS
- List all company codes → 3 rows (T001)
- Show all vendor names and cities → 21 rows (LFA1)
- Total open payables by vendor name → 9 rows (BSIK+LFA1)
- GL account balances by company code → 27 rows (GLT0+SKA1)

### Materials Management (MM) — 4/4 PASS
- List all materials with descriptions → 65 rows (MAKT)
- Purchase order headers with vendor names → 35 rows (EKKO+EKPO+LFA1)
- Total PO value by vendor name → 9 rows (EKKO+EKPO+LFA1)
- Material stock by plant → 45 rows (MARD+T001W)

### Sales & Distribution (SD) — 3/3 PASS
- Sales orders with customer names → 36 rows (VBAK+KNA1)
- Total billed revenue by customer → 5 rows (VBRP+VBRK+KNA1)
- Delivery documents with items → 11 rows (LIKP+LIPS)

### Human Resources (HR) — 2/3 (1 DISAMBIG)
- Employee names and departments → 20 rows (PA0002+HRP1000)
- Employee pay scale information → DISAMBIG (correctly identifies ambiguous query)
- Headcount by org unit → 9 rows (HRP1000+PA0001)

### Plant Maintenance (PM) — 2/2 PASS
- Maintenance orders with descriptions → 25 rows (AFIH)
- Equipment with functional locations → 12 rows (EQUI+IFLOT)

### Controlling (CO) — 2/2 PASS
- Cost center names and descriptions → 10 rows (CSKT)
- Actual costs by cost center → 10 rows (COEP+CSKT)

### Cross-Module — 4/5 (1 FAIL)
- POs with vendor names and totals → 35 rows (EKKO+EKPO+LFA1)
- Customers with total sales values → 16 rows (VBAK+KNA1)
- Employees with cost center names → 20 rows (PA0001+CSKT)
- Top 10 materials by PO items → 10 rows (EKPO)
- Average salary by department → FAIL (0 rows — ANSAL column data gap or join path issue)

## Key Improvements in v0.4.73

1. **Strategy 4 DD metadata** — 133 new declared FK relationships at confidence 95, providing authoritative join paths that the LLM can rely on instead of inferring from column names alone

2. **Context document** — 18KB schema reference organized by SAP module (FI, CO, MM, SD, HR, PM) gives the enrichment and NL2SQL stages domain context about SAP table purposes and relationships

3. **128-table coverage** — DD tables included in vector embeddings and synonym coverage, improving semantic schema linking

## Known Limitations

1. **Synthetic DD data** — FK relationships derived from LeanX documentation, not extracted from a live SAP system. ABAP extraction guide provided for the SAP team to extract real metadata.

2. **Average salary by department** — Fails due to either empty ANSAL column or broken join path through PA0008→PA0001→CSKT. Requires data validation.

3. **Disambiguation on ambiguous queries** — System correctly identifies vague queries but some users may expect a default interpretation. Post-demo enhancement: smart defaults + offer pattern.

## Files Delivered

| File | Location | Purpose |
|------|----------|---------|
| sap.sqlite.gz | data/ (repo) | 128-table database with DD metadata |
| sap_schema_reference.txt | data/ (repo) | Module-level schema reference |
| SAP_DD_Extraction_Guide_2026-04-13.md | IDA/ (workspace) | ABAP extraction guide for SAP team |
| build_sap_metadata.py | IDA/ (workspace) | Reproducible DD table builder script |
| SAP_Audit_Summary_2026-04-13.md | IDA/ (workspace) | This document |

## Engineering Handoff Readiness

- [x] Code deployed (v0.4.73 on Railway)
- [x] Pipeline complete (all 10 steps)
- [x] QPD tests passing (178/178)
- [x] Manual queries validated (21/23)
- [x] DD metadata integrated (Strategy 4 active)
- [x] Context document uploaded
- [x] Schema reference in repo
- [x] ABAP extraction guide for SAP team
- [ ] Pending: Real DD metadata from live SAP system
- [ ] Pending: OEBS synonym fixes from parallel session
