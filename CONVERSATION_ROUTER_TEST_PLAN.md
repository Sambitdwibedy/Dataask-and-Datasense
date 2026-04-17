# Unified Conversation Router — Test Plan

**Version:** 0.3.1
**Date:** March 31, 2026
**Scope:** All 11 response types routed by server-side Haiku (no client-side regex)

---

## Architecture Change

**Before:** Three-brain system — client-side regex patterns detected confirm/farewell/document intents, then an if/else chain routed by phase. Haiku only saw query/clarify/greeting/farewell. Ambiguous phrases ("Yes, put a briefing together for me") fell through the cracks.

**After:** Single-brain system — ALL user input goes to server (Haiku). Client sends full context: `phase`, `resultsOnScreen`, `pendingQueries`. Haiku classifies into 11 response types. Client only guards `executing` and `farewell` phases locally.

---

## Test Matrix — All 11 Response Types

### 1. QUERY (single data request)

| # | Input | Phase | Expected Type | Expected Behavior |
|---|-------|-------|---------------|-------------------|
| 1.1 | "How many invoices are there?" | greeting/collecting | query | Queued, ack spoken, auto-listen |
| 1.2 | "Show me AR aging" | collecting | query | Added to queue, "anything else?" |
| 1.3 | "Also show me revenue" (follow-up turn) | collecting | query | Single new query added (NOT multi_query) |
| 1.4 | "bookings" (after Haiku asked "bookings or revenue?") | collecting | query | Acts on clarification answer immediately |

### 2. MULTI_QUERY (multiple items in one message)

| # | Input | Phase | Expected Type | Expected Behavior |
|---|-------|-------|---------------|-------------------|
| 2.1 | "Show me bookings, AR aging, and collections" | collecting | multi_query | All 3 queued, ack with count |
| 2.2 | "Both" (after Haiku offered bookings vs revenue) | collecting | multi_query | Both options queued |
| 2.3 | "Revenue and order counts for the quarter" | collecting | multi_query | 2 queries with time scope preserved |

### 3. DOC_QUERY (document/process questions)

| # | Input | Phase | Expected Type | Expected Behavior |
|---|-------|-------|---------------|-------------------|
| 3.1 | "What is the AP approval workflow?" | collecting | doc_query | Queued with UNSTRUCTURED intent |
| 3.2 | "How do I create a purchase order?" | collecting | doc_query | Document search, not SQL |
| 3.3 | "Explain the collections policy" | reviewing | doc_query | New query cycle starts |

### 4. CONFIRM_DONE (done collecting)

| # | Input | Phase | Pending? | Expected Type | Expected Behavior |
|---|-------|-------|----------|---------------|-------------------|
| 4.1 | "That's all" | collecting | Yes | confirm_done | Batch-execute pending queries |
| 4.2 | "Go ahead" | collecting | Yes | confirm_done | Batch-execute |
| 4.3 | "No more, thank you" | collecting | Yes | confirm_done | Batch-execute (NOT farewell) |
| 4.4 | "Yes, that's it" | collecting | Yes | confirm_done | Batch-execute |
| 4.5 | "Run those" | collecting | Yes | confirm_done | Batch-execute |
| 4.6 | "That's all" | collecting | No | confirm_done → farewell | Session ends gracefully |
| 4.7 | "Thank you, that's all I need" | collecting | Yes | confirm_done | Critical: must NOT farewell |

### 5. FAREWELL (end session)

| # | Input | Phase | Expected Type | Expected Behavior |
|---|-------|-------|---------------|-------------------|
| 5.1 | "Bye" | reviewing | farewell | Session ends with summary |
| 5.2 | "Thank you Katy" | reviewing | farewell | Session ends warmly |
| 5.3 | "No thank you, I appreciate your help" | reviewing | farewell | Appreciative close handled |
| 5.4 | "Thanks, that's all for now" | reviewing | farewell | Session ends |
| 5.5 | "Goodbye" | collecting (no pending) | farewell | Session ends |

### 6. BRIEFING (compile results into document)

| # | Input | Phase | Results? | Expected Type | Expected Behavior |
|---|-------|-------|----------|---------------|-------------------|
| 6.1 | "Put together a briefing for me" | reviewing | Yes | briefing | Briefing generated, blob URL created |
| 6.2 | "Yes, put a briefing together for me" | reviewing | Yes | briefing | "Yes, " prefix doesn't break it |
| 6.3 | "Summarize this for my meeting" | reviewing | Yes | briefing | Natural phrasing works |
| 6.4 | "Create a report with these results" | reviewing | Yes | briefing | Report variant works |
| 6.5 | "Package this up" | reviewing | Yes | briefing | Informal phrasing works |

### 7. FOLLOW_UP (drill down into result)

| # | Input | Phase | Results? | Expected Type | Expected Behavior |
|---|-------|-------|----------|---------------|-------------------|
| 7.1 | "Break that down by quarter" | reviewing | Yes | follow_up | New query auto-executes |
| 7.2 | "Show me just the top 5" | reviewing | Yes | follow_up | Refined query runs |
| 7.3 | "Filter for overdue only" | reviewing | Yes | follow_up | Drill-down executes |
| 7.4 | "More detail on the second one" | reviewing | Yes (2+) | follow_up | ref=2, targets specific result |

### 8. CORRECTION (fix previous query)

| # | Input | Phase | Results? | Expected Type | Expected Behavior |
|---|-------|-------|----------|---------------|-------------------|
| 8.1 | "I meant revenue, not bookings" | reviewing | Yes | correction | Corrected query auto-executes |
| 8.2 | "Actually by month, not by quarter" | reviewing | Yes | correction | Re-run with fix |
| 8.3 | "No, for all customers" | reviewing | Yes | correction | Scope fix applied |

### 9. CHITCHAT (conversational, non-data)

| # | Input | Phase | Expected Type | Expected Behavior |
|---|-------|-------|---------------|-------------------|
| 9.1 | "How are you?" | any | chitchat | Warm response, redirects to data |
| 9.2 | "What can you do?" | greeting | chitchat | Capability description |
| 9.3 | "Who made you?" | any | chitchat | Brief answer |
| 9.4 | "Good morning" (not first turn) | collecting | chitchat or greeting | Natural acknowledgment |

### 10. HYBRID (structured + unstructured)

| # | Input | Phase | Expected Type | Expected Behavior |
|---|-------|-------|---------------|-------------------|
| 10.1 | "Show me overdue invoices and explain the collection policy" | collecting | hybrid | Both queries queued, auto-execute |
| 10.2 | "What's our AR aging and how does our dunning process work?" | collecting | hybrid | Data + doc queries |

### 11. GREETING

| # | Input | Phase | Expected Type | Expected Behavior |
|---|-------|-------|---------------|-------------------|
| 11.1 | "Hello" | greeting | greeting | Warm greeting with name |
| 11.2 | "Hey Katy" | greeting | greeting | Personalized greeting |

### 12. CLARIFY

| # | Input | Phase | Expected Type | Expected Behavior |
|---|-------|-------|---------------|-------------------|
| 12.1 | "Show me the sales numbers" | collecting | clarify | Asks: bookings, revenue, or order counts? |
| 12.2 | "Financial data" | collecting | clarify | Asks which area: AP, AR, GL? |

---

## Phase Transition Tests

| # | Scenario | Start Phase | Expected End Phase |
|---|----------|-------------|--------------------|
| P1 | User asks first question | greeting | collecting |
| P2 | User adds more questions | collecting | collecting |
| P3 | User says "that's all" with pending | collecting | executing → reviewing |
| P4 | Idle timeout with 1 pending query (4s) | collecting | executing → reviewing |
| P5 | User says "bye" during reviewing | reviewing | farewell |
| P6 | User asks new question during reviewing | reviewing | collecting |
| P7 | User asks for briefing during reviewing | reviewing | reviewing (stays) |
| P8 | Input arrives during executing | executing | deferred (processed after) |

---

## Critical Edge Cases

| # | Scenario | Expected Behavior |
|---|----------|-------------------|
| E1 | "Thank you that's all" during COLLECTING with 3 pending | confirm_done → batch-execute (NOT farewell) |
| E2 | "Yes, put a briefing together for me" during REVIEWING | briefing (the "Yes, " prefix must not confuse Haiku) |
| E3 | "Also revenue" as follow-up (not in same message) | query (single), NOT multi_query |
| E4 | "Break that down" with no results on screen | clarify (Haiku should ask "break what down?") |
| E5 | STT garble: "call thank you" (mangled "that's all, thank you") | confirm_done (Haiku handles context better than regex) |
| E6 | Stuck loop: two consecutive error responses | Bail out with helpful message |
| E7 | Double farewell prevention | Second farewell ignored |
| E8 | Hybrid where doc search has no documents | Structured part runs, doc part returns "no documents available" |

---

## Voice-Specific Tests

| # | Test | Expected Result |
|---|------|----------------|
| V1 | Mic activates after TTS completes | No delay, no chime (stream reuse) |
| V2 | TTS plays without AEC suppression | Mic stopped before TTS starts |
| V3 | Silence detection works after auto-listen | AudioContext resumed, AnalyserNode active |
| V4 | Single query idle timeout (4s) | Auto-executes after 4 seconds |
| V5 | Multi query idle timeout (12s) | Auto-executes after 12 seconds |

---

## Quick Sanity Check (5-Minute Version)

1. **Open app, click mic** → Greeting plays (type: greeting)
2. **Say "How many invoices?"** → Queued ack, auto-listen (type: query)
3. **Say "Also AR aging"** → Two items queued (type: query)
4. **Say "That's all"** → Batch-execute fires (type: confirm_done)
5. **Results display, say "Break that down by month"** → Follow-up executes (type: follow_up)
6. **Say "Put together a briefing"** → Briefing generated (type: briefing)
7. **Say "Thank you Katy"** → Session ends warmly (type: farewell)

If all 7 pass, the unified router is working correctly.

---

## Regression Watch

- Typed queries (text box) should still work identically — they go through the same `/converse` endpoint
- Multi-query batch execution should still produce individual result cards
- Chart toggle, CSV/Excel export should still work on structured results
- Schema-Linked badge should still appear on structured queries
- Document search citations should still display with purple cards
