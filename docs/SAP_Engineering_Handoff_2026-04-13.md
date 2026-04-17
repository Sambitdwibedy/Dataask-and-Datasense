# SAP ECC 6.0 — Engineering Handoff Notes

**Date:** April 13, 2026
**From:** Mark Lee (CPO) / Claude (AI assistant)
**To:** Engineering team (Sambit + team)
**Version:** v0.4.73 (deployed on Railway)

---

## What's Being Handed Off

A working SAP ECC 6.0 prototype that demonstrates Data Sense (AKG builder) and Data Ask (NL2SQL query interface) against a 128-table SAP schema. Two early access customers depend on SAP AKGs, making this handoff time-sensitive.

## Current State

- **App ID 5** on Railway (data-ask-production-b6a6.up.railway.app)
- **Published and live** — end users can query via Data Ask
- **128 tables**, 2078 columns, 622 relationships, 7886 synonyms
- **178/178 QPD tests passing** (100%)
- **21/23 manual queries passing** (91.3%) across FI, MM, SD, HR, PM, CO modules

## Architecture Decisions

### Relationship Discovery (Strategy 3b + Strategy 4)

SAP schemas do NOT declare foreign keys in the database itself. SAP stores FK metadata in Data Dictionary (DD) tables. We handle this with two strategies:

1. **Strategy 3b** (confidence 80-85): Shared column name matching with value overlap confirmation. This found 489 relationships. Works for any schema without declared PKs.

2. **Strategy 4** (confidence 95): Reads DD05S/DD08L metadata tables to extract declared FK relationships. This added 133 new high-confidence relationships. The code is in `server/routes/pipeline.js` lines 760-830.

Strategy 4 is the preferred approach when DD metadata is available because it provides authoritative join paths that the LLM can trust.

### DD Metadata Tables

Four synthetic DD tables were added to `data/sap.sqlite.gz`:

| Table | Records | Source |
|-------|---------|--------|
| DD02T | 124 | Table descriptions from LeanX documentation |
| DD08L | 350 | FK definitions |
| DD05S | 350 | FK field assignments |
| DD03L | 265 | Key field definitions |

**Important:** These are synthetic, derived from public LeanX documentation for SAP R/3 ERP (= ECC 6.0). They are NOT from a live SAP system. An ABAP extraction guide has been provided (see `SAP_DD_Extraction_Guide_2026-04-13.md` in the IDA workspace folder) for the SAP team to extract real metadata from a customer's ECC 6.0 environment.

### Context Document

A schema reference document (`data/sap_schema_reference.txt`, 18KB) is uploaded as a context document in the Data Sense pipeline. It provides module-level context (FI, CO, MM, SD, HR, PM) with FK relationships and a key column glossary. The enrichment and NL2SQL stages use this to produce better descriptions and more accurate SQL.

### What's Different from OEBS

| Aspect | OEBS (App 4) | SAP (App 5) |
|--------|-------------|-------------|
| FK source | FND_FOREIGN_KEYS table (real) | DD05S/DD08L (synthetic) |
| Strategy | Strategy 4 (declared FKs) | Strategy 3b + 4 (hybrid) |
| Tables | ~200+ | 128 |
| Synonym source | ERP pack (210 terms) + AI-generated | AI-generated only |
| Context docs | Yes (schema reference) | Yes (schema reference) |

## Code Files to Know

| File | What It Does |
|------|-------------|
| `server/routes/pipeline.js` | Pipeline orchestration, Strategy 3b/4 relationship discovery (lines 700-850) |
| `server/routes/query-engine.js` | NL2SQL pipeline — classification, schema linking, SQL generation |
| `server/routes/ask.js` | Unified ask endpoint (POST /api/ask with appId in body) |
| `server/routes/context.js` | Context document upload/management/merge |
| `server/routes/synonyms.js` | Synonym management, AI generator, ERP pack seeding |
| `client/index.html` | Single-page app for Data Sense builder + Data Ask UI |
| `data/sap.sqlite.gz` | SAP prototype database (128 tables + DD metadata) |
| `data/sap_schema_reference.txt` | Context document for SAP enrichment |

## Recommendations for Engineering

### Immediate (before customer demos)

1. **Rebuild OEBS embeddings** — The OEBS synonym fix (v0.4.71-v0.4.74) added 1,179 synonyms but embeddings were NOT rebuilt. Run the Index step for app 4 to include them in vector search.

2. **Get real SAP DD metadata** — Use the ABAP extraction guide to extract DD02T, DD03L, DD05S, DD08L from a real ECC 6.0 system. Replace synthetic data with real data and re-run pipeline.

3. **Seed SAP synonyms** — The ERP pack currently only covers OEBS column names (AP, AR, GL, PO, OM). SAP uses different column names (BUKRS, LIFNR, MATNR, etc.) that need SAP-specific synonym mappings.

### Short-term (next sprint)

4. **Scale testing** — Current SAP prototype has synthetic data. Test with real customer data volumes (100K+ rows per table) to validate performance.

5. **Strategy 4 robustness** — Strategy 4 currently does case-insensitive matching for DD table detection (lowercase in information_schema, uppercase for queries). Verify this works with different PostgreSQL configurations.

6. **Average salary query fix** — The only failing cross-module query. Investigate whether PA0008.ANSAL has data and whether the join path PA0008→PA0001→CSKT resolves correctly.

### Medium-term

7. **DD table auto-detection** — Instead of hardcoding DD table patterns in FK_METADATA_PATTERNS, auto-detect DD-like tables by schema inspection.

8. **SAP module classification** — Enrich table descriptions with SAP module tags (FI, MM, SD, etc.) to improve schema linking for module-specific queries.

## How to Re-run the Pipeline

```bash
# Authenticate
TOKEN=$(curl -s -X POST https://data-ask-production-b6a6.up.railway.app/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"mark@solix.com","password":"demo2026"}' | jq -r '.token')

# Run each step
for step in connect profile discover context enrich synonyms; do
  curl -s -X POST "https://data-ask-production-b6a6.up.railway.app/api/pipeline/5/run-step" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"step\":\"$step\"}"
  echo ""
done

# Human review steps (force complete)
for step in curate; do
  curl -s -X POST "https://data-ask-production-b6a6.up.railway.app/api/pipeline/5/run-step" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"step\":\"$step\",\"action\":\"complete\"}"
  echo ""
done

# Final steps
for step in index validate publish; do
  curl -s -X POST "https://data-ask-production-b6a6.up.railway.app/api/pipeline/5/run-step" \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "{\"step\":\"$step\"}"
  echo ""
done
```

## Test Query for Quick Verification

```bash
curl -s -X POST "https://data-ask-production-b6a6.up.railway.app/api/ask" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"question":"List all company codes","appId":5}' | jq '.answer, .results.rowCount'
```

Expected: 3 company codes (1000 Solix Technologies, 2000 Solix Europe, 3000 Solix Asia Pacific)
