# Data Ask Consolidated App — Smoke Test Plan

**Version:** Post-Merge (Phases 1–3 Complete)
**Date:** March 30, 2026
**Purpose:** Verify the consolidated Data Ask app (BOKG Builder + Data Ask) works end-to-end before deploying to Railway.

---

## Pre-Deployment Steps

### 1. Version Bump
Open `Data Ask/app/client/index.html` and bump `APP_VERSION` (search for it near the top of the file). Use a version like `0.3.0-consolidated` or the next logical version number.

### 2. Git Push (via Fresh Clone)
Since iCloud lock files block commits from the mounted repo, always push via a fresh clone:

```bash
# From a terminal (not iCloud-synced path):
cd /tmp
rm -rf ida-data-ask-push
git clone https://github.com/solixtech/ida-data-ask.git ida-data-ask-push
cd ida-data-ask-push

# Copy the merged files from your working directory
cp -R "/Users/mark/Library/Mobile Documents/com~apple~CloudDocs/Claude Workspace/IDA/Data Ask/app/" ./app/

git add -A
git commit -m "Consolidate BOKG Builder into Data Ask (Phases 1-3)"
git push origin main
```

### 3. Railway Deploy
After push, Railway should auto-deploy from main. Monitor the build logs for:
- `npm install` completes without errors
- Server starts on port 3002
- Database connection established
- All startup functions run (initDatabase, ensureAdminUser, etc.)

---

## Test Checklist

### A. BOKG Builder Verification (Frozen — Should Still Work)

| # | Test | Expected Result | Pass? |
|---|------|----------------|-------|
| A1 | Open BOKG Builder URL (https://bokg-builder-production.up.railway.app) | App loads, login screen appears | ☐ |
| A2 | Log in as mark@solix.com | Dashboard loads with existing apps | ☐ |
| A3 | Open an existing app (e.g., OEBS) | App detail page loads, pipeline stages visible | ☐ |
| A4 | Run a test query in Query Engine | SQL generates and results display | ☐ |

> **BOKG Builder must remain untouched.** These tests confirm it's still operational as the safety net.

---

### B. Data Ask — Basic Startup

| # | Test | Expected Result | Pass? |
|---|------|----------------|-------|
| B1 | Open Data Ask URL (https://ida-data-ask-production.up.railway.app) | App loads — you should see the BOKG Builder UI (since the client was copied) | ☐ |
| B2 | Hit `/api/health` | Returns JSON with `service: 'ida-data-ask'`, status ok, version matches bump | ☐ |
| B3 | Hit `/api/diag` | Returns table counts for all tables (app_tables, app_columns, doc_sources, etc.) | ☐ |

---

### C. Authentication

| # | Test | Expected Result | Pass? |
|---|------|----------------|-------|
| C1 | Log in as admin: `mark@solix.com` / `admin123` | Dashboard loads, admin features visible | ☐ |
| C2 | Log out, log in as end-user: `analyst@solix.com` / `analyst123` | Dashboard loads, limited to query/browse features | ☐ |

---

### D. Application & Pipeline (BOKG Builder Features)

| # | Test | Expected Result | Pass? |
|---|------|----------------|-------|
| D1 | View Applications list | All previously created apps appear (OEBS, OFBiz, etc.) | ☐ |
| D2 | Open an app → Schema tab | Tables and columns display with enrichment data | ☐ |
| D3 | Open an app → Relationships tab | Foreign key and inferred relationships display | ☐ |
| D4 | Open an app → Curation tab | Curated entities and quality scores visible | ☐ |
| D5 | Open an app → Data Browser | Can browse table data, pagination works | ☐ |
| D6 | Run pipeline enrichment on an app (if safe to re-run) | Pipeline stages complete without errors | ☐ |

---

### E. NL2SQL Query Engine (Structured Queries)

| # | Test | Expected Result | Pass? |
|---|------|----------------|-------|
| E1 | Open Query Engine for an app | Query input appears, voice button visible | ☐ |
| E2 | Type: "How many invoices are there?" | SQL generates, results table displays, row count shown | ☐ |
| E3 | Type: "Top 10 vendors by total amount" | Results show vendor ranking, chart toggle works | ☐ |
| E4 | Click Export CSV on a result | CSV downloads with correct data | ☐ |
| E5 | Click Export Excel on a result | XLSX downloads with correct data | ☐ |
| E6 | Check that "Schema-Linked" badge appears on structured queries | Green badge visible for schema-linked SQL | ☐ |

---

### F. Voice Conversation Mode

| # | Test | Expected Result | Pass? |
|---|------|----------------|-------|
| F1 | Click the microphone button | Voice mode activates, greeting plays | ☐ |
| F2 | Ask a simple data question by voice | Haiku routes it, disambiguation or direct query fires | ☐ |
| F3 | Ask a multi-part question | Conversation router collects metrics, then batch-executes | ☐ |
| F4 | Verify TTS responses play clearly | Audio plays without echo or volume dip | ☐ |

---

### G. Intent Routing (New — Phase 3)

This is the key new functionality. Test that document/process questions route correctly.

| # | Test | Expected Result | Pass? |
|---|------|----------------|-------|
| G1 | In the Ask interface (`/api/ask` or chat UI), type a data question: "How many orders last month?" | Intent badge shows "Structured" or similar, SQL results display | ☐ |
| G2 | Type a document question: "What is the AP approval workflow?" | Intent badge shows **"Document Search"** (purple), answer comes from RAG engine | ☐ |
| G3 | Type a hybrid question: "Show me overdue invoices and explain the collection policy" | Intent badge shows **"Hybrid (Data + Docs)"** (cyan), both data results and document answer display | ☐ |
| G4 | Type something vague: "help" | Intent classified as CLARIFY, helpful guidance message appears | ☐ |
| G5 | Via voice, ask a process question: "How do I create a purchase order?" | Conversation router emits `doc_query` type, document answer plays via TTS | ☐ |

---

### H. Unstructured Search & Citations (If Documents Uploaded)

> **Prerequisite:** At least one document collection with uploaded/processed documents for the test app. If none exist, skip this section or upload a test PDF first via the Documents tab.

| # | Test | Expected Result | Pass? |
|---|------|----------------|-------|
| H1 | Open Documents tab for an app | Document collections list appears | ☐ |
| H2 | Upload a test document (PDF or DOCX) | File uploads, processing starts, status shows "ready" when done | ☐ |
| H3 | Ask a question about the document content | Answer appears with citations panel below | ☐ |
| H4 | Check citations display | Purple-themed cards with filename, chunk index, match score, excerpt | ☐ |

---

### I. UI Verification

| # | Test | Expected Result | Pass? |
|---|------|----------------|-------|
| I1 | On a structured query result, verify chart toggle works | Chart renders and toggles on/off | ☐ |
| I2 | On a document search result, verify chart toggle is **hidden** | No chart button for UNSTRUCTURED results | ☐ |
| I3 | On a document search result, verify export toolbar is **hidden** | No CSV/Excel export for document answers | ☐ |
| I4 | On a document search result, verify "no results" message doesn't appear | Should show the document answer, not "no results found" | ☐ |
| I5 | Schema-Linked badge does NOT appear on document search results | Badge hidden when intent is UNSTRUCTURED | ☐ |

---

## Quick Sanity Check (5-Minute Version)

If short on time, just hit these critical paths:

1. **B1** — App loads
2. **C1** — Admin login works
3. **D1** — Applications list appears
4. **E2** — A structured query works end-to-end
5. **G2** — A document question routes correctly (intent badge appears)
6. **F1** — Voice mode activates

If all six pass, the consolidation is solid and ready for broader testing.

---

## Known Considerations

- **No documents uploaded yet?** Tests G2, G3, G5, and all of Section H depend on having documents in the system. The intent router will still classify correctly, but the unstructured engine will return "no relevant information found."
- **First deploy may be slow** — npm install needs to fetch new dependencies (mammoth, pgvector, openai SDK).
- **Database migrations** — The consolidated index.js runs `initDatabase()` on startup which creates any missing tables (doc_collections, doc_sources, doc_chunks, ida_conversations). These should auto-create on first boot.
- **Environment variables** — Data Ask Railway project needs: `DATABASE_URL`, `ANTHROPIC_API_KEY`, `OPENAI_API_KEY` (for TTS), `APP_VERSION`.
