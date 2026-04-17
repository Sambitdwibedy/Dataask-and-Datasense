/**
 * Ask Routes — Unified chat endpoint for Data Ask
 *
 * POST /api/ask — Main endpoint: classify intent → route to engine(s) → return response
 * GET /api/ask/history — Conversation history
 */
const express = require('express');
const { query } = require('../db');
const { classifyIntent } = require('../services/intent-router');
const { answerFromDocuments } = require('../services/unstructured-engine');
const { queryStructuredData } = require('../services/structured-engine');
const {
  classifyIntentRich,
  schemaLink,
  buildBOKGContext,
  loadValueDictionaries,
  getFewShotExamples,
  getContextDocumentSection,
  generateAndExecuteWithRetry,
  generateWithSelfConsistency,
  buildColumnBusinessNames,
} = require('./query-engine');

const router = express.Router();

// ─── Full BOKG Pipeline Helper ───
// Runs the same pipeline as query-engine.js /converse: schema link → BOKG context
// → few-shot examples → value dictionaries → context docs → NL2SQL → execute.
async function runFullBOKGPipeline(appId, question, evidence = null, pipelineOptions = {}) {
  const { dialect = 'postgresql', selfConsistency = 1, dryRun = false } = pipelineOptions;

  const appResult = await query('SELECT name, config FROM applications WHERE id = $1', [appId]);
  const appName = appResult.rows[0]?.name || 'Unknown';
  const appConfig = appResult.rows[0]?.config || {};
  const qeConfig = appConfig.query_engine || {};

  const [linkedTableIds, valueDictionaries, fewShotSection, contextSection] = await Promise.all([
    schemaLink(appId, question, qeConfig.schema_link_threshold),
    loadValueDictionaries(appId),
    getFewShotExamples(appId, question),
    getContextDocumentSection(appId),
  ]);
  const bokgContext = await buildBOKGContext(appId, linkedTableIds, question, qeConfig.column_link_threshold);

  // Inject evidence/hints into the question if provided (BIRD benchmark format)
  const questionWithEvidence = evidence
    ? `${question}\n\nHINT: ${evidence}`
    : question;

  const options = { dialect, selfConsistency, dryRun };

  const result = await generateWithSelfConsistency(
    appId, questionWithEvidence, bokgContext, appName, fewShotSection, valueDictionaries, contextSection, qeConfig.model, options
  );

  const rows = result.rows || [];
  const columns = result.columns || (rows.length > 0 ? Object.keys(rows[0]) : []);
  const topScore = linkedTableIds?.[0]?.relevanceScore || 0;

  // Build business-friendly column names (same as voice /converse path)
  const columnBusinessNames = await buildColumnBusinessNames(appId, columns, result.sql);

  return {
    sql: result.sql,
    results: { rows, columns, rowCount: rows.length },
    column_business_names: columnBusinessNames,
    confidence: topScore >= 10 ? 'high' : topScore >= 5 ? 'medium' : 'low',
    tablesUsed: linkedTableIds?.map(t => t.table_name || t.name) || [],
    execError: result.execError,
    tokenUsage: {
      input: result.token_usage?.input_tokens || 0,
      output: result.token_usage?.output_tokens || 0,
      model: qeConfig.model || 'claude-sonnet-4-20250514',
    },
  };
}

// ─── Unified Ask Endpoint ───

// POST /api/ask
router.post('/', async (req, res) => {
  try {
    const { question, appId: rawAppId, workspaceId, sessionId, collectionId, searchAllWorkspaces, skipDisambiguation, evidence, dialect, selfConsistency, dryRun } = req.body;
    console.log('[Ask] question:', question?.substring(0, 60), 'skipDisambig:', !!skipDisambiguation, 'appId:', rawAppId, 'wsId:', workspaceId);
    if (!question) {
      return res.status(400).json({ error: 'question is required' });
    }

    const userId = req.user?.id || null;
    const sid = sessionId || `session-${Date.now()}`;

    // Resolve workspace → appId (new model) or use appId directly (backward compat)
    let appId = rawAppId;
    let wsId = workspaceId || null;
    let allUserWorkspaceIds = [];

    if (wsId) {
      // New workspace model: resolve appId from workspace
      const wsResult = await query('SELECT app_id FROM workspaces WHERE id = $1', [wsId]);
      if (wsResult.rows.length > 0 && wsResult.rows[0].app_id) {
        appId = wsResult.rows[0].app_id;
      }
    } else if (appId) {
      // Backward compat: try to find workspace for this app
      const wsLookup = await query('SELECT id FROM workspaces WHERE app_id = $1 LIMIT 1', [appId]);
      if (wsLookup.rows.length > 0) wsId = wsLookup.rows[0].id;
    }

    if (!appId && !wsId) {
      return res.status(400).json({ error: 'workspaceId or appId required' });
    }

    // If cross-workspace search requested, load all user's workspace IDs
    if (searchAllWorkspaces && userId) {
      const allWs = await query(
        `SELECT w.id, w.app_id FROM workspaces w
         JOIN workspace_members wm ON w.id = wm.workspace_id
         WHERE wm.user_id = $1 AND wm.enabled = TRUE AND w.status = 'active'`,
        [userId]
      );
      allUserWorkspaceIds = allWs.rows.map(r => r.id);
    }

    // Load recent conversation history for context
    const historyResult = await query(
      `SELECT role, content FROM ida_conversations
       WHERE session_id = $1 AND (app_id = $2 OR workspace_id = $3)
       ORDER BY created_at DESC LIMIT 10`,
      [sid, appId, wsId]
    );
    const conversationHistory = historyResult.rows.reverse();

    // 1. Classify intent (workspace-aware: knows about both structured data and documents)
    const classification = await classifyIntent(question, { appId, workspaceId: wsId });
    const { intent, confidence: intentConfidence, reasoning } = classification;

    // Save user message
    await query(
      `INSERT INTO ida_conversations (app_id, workspace_id, user_id, session_id, role, content, intent, confidence)
       VALUES ($1, $2, $3, $4, 'user', $5, $6, $7)`,
      [appId, wsId, userId, sid, question, intent, String(intentConfidence)]
    );

    let response;

    switch (intent) {
      case 'STRUCTURED': {
        // ── Disambiguation check: vague queries get slot-based guided prompts ──
        // The rich classifyIntent (from query-engine) detects when a query like
        // "show me sales" needs clarification before SQL generation.
        // Skip if user already went through disambiguation (composed query from slots).
        if (!skipDisambiguation) try {
          const { classification: richClass } = await classifyIntentRich(appId, question);
          if (richClass.disambiguation_needed && richClass.slot_questions && richClass.slot_questions.length > 0) {
            console.log('[Ask] Disambiguation triggered for:', question, '→ slots:', richClass.slot_questions.length);
            response = {
              intent: 'DISAMBIGUATION',
              intentConfidence,
              interpretation: richClass.interpretation,
              disambiguation_reason: richClass.disambiguation_reason || '',
              slot_questions: richClass.slot_questions,
              suggestions: richClass.suggestions || [],
              slots: richClass.slots || {},
              confidence: richClass.confidence,
              answer: richClass.disambiguation_reason || 'Let me help you be more specific.',
              tokenUsage: classification.tokenUsage,
            };
            break;
          }
        } catch (disambigErr) {
          console.warn('[Ask] Rich classify failed, proceeding to SQL generation:', disambigErr.message);
        }

        // ── Full BOKG Pipeline (same as query-engine.js /converse) ──
        const pipelineResult = await runFullBOKGPipeline(appId, question, evidence, { dialect, selfConsistency, dryRun });
        pipelineResult.results = deduplicateRows(pipelineResult.results);

        let answer = null;
        if (!pipelineResult.execError && pipelineResult.results.rows.length > 0) {
          answer = await generateResultsSummary(question, pipelineResult.results, pipelineResult.sql);
        }

        response = {
          intent,
          intentConfidence,
          answer: answer || (pipelineResult.execError ? `Query error: ${pipelineResult.execError}` : 'Query executed but returned no results.'),
          sql: pipelineResult.sql,
          results: pipelineResult.results,
          column_business_names: pipelineResult.column_business_names,
          confidence: pipelineResult.confidence,
          tablesUsed: pipelineResult.tablesUsed,
          tokenUsage: mergeTokenUsage(classification.tokenUsage, pipelineResult.tokenUsage),
        };
        break;
      }

      case 'UNSTRUCTURED': {
        // Search documents in current workspace, or across all if requested
        const docSearchOpts = {
          appId,
          workspaceId: wsId,
          collectionId,
          conversationHistory,
          crossWorkspaceIds: searchAllWorkspaces ? allUserWorkspaceIds : null,
        };
        const result = await answerFromDocuments(question, docSearchOpts);
        response = {
          intent,
          intentConfidence,
          answer: result.answer,
          citations: result.citations,
          confidence: result.confidence,
          chunksUsed: result.chunksUsed,
          tokenUsage: mergeTokenUsage(classification.tokenUsage, result.tokenUsage),
        };
        break;
      }

      case 'HYBRID': {
        // Run both engines in parallel — structured against current workspace's app,
        // documents optionally across all workspaces
        const docSearchOpts = {
          appId,
          workspaceId: wsId,
          collectionId,
          conversationHistory,
          crossWorkspaceIds: searchAllWorkspaces ? allUserWorkspaceIds : null,
        };
        const [structuredResult, unstructuredResult] = await Promise.allSettled([
          runFullBOKGPipeline(appId, question, evidence, { dialect, selfConsistency, dryRun }),
          answerFromDocuments(question, docSearchOpts),
        ]);

        const structured = structuredResult.status === 'fulfilled' ? structuredResult.value : null;
        const unstructured = unstructuredResult.status === 'fulfilled' ? unstructuredResult.value : null;
        if (structured?.results) structured.results = deduplicateRows(structured.results);

        // Merge results — synthesize a coherent answer that avoids contradictions
        let answer = '';
        const hasStructured = structured?.results?.rows?.length > 0;
        const hasDocAnswer = !!unstructured?.answer;

        if (hasStructured && hasDocAnswer) {
          // Both engines returned results — use LLM to synthesize a coherent combined answer
          answer = await synthesizeHybridAnswer(question, structured, unstructured);
        } else if (hasStructured) {
          const summary = await generateResultsSummary(question, structured.results, structured.sql);
          answer = `**Data Analysis:**\n${summary || 'Query returned results (see table below).'}`;
        } else if (hasDocAnswer) {
          answer = `**Documentation Context:**\n${unstructured.answer}`;
        }
        if (!answer) {
          answer = 'I could not find relevant information from either the database or uploaded documents for this question.';
        }

        response = {
          intent,
          intentConfidence,
          answer,
          sql: structured?.sql,
          results: structured?.results,
          column_business_names: structured?.column_business_names,
          citations: unstructured?.citations || [],
          confidence: structured?.confidence || unstructured?.confidence || 'low',
          tablesUsed: structured?.tablesUsed,
          chunksUsed: unstructured?.chunksUsed,
          tokenUsage: mergeTokenUsage(
            classification.tokenUsage,
            structured?.tokenUsage,
            unstructured?.tokenUsage
          ),
        };
        break;
      }

      case 'CLARIFY':
      default: {
        // The intent router's Haiku model sometimes misclassifies structured queries
        // as CLARIFY (e.g. on OEBS where table names differ from what Haiku expects).
        // Use the rich classify to either: (a) produce disambiguation slot questions,
        // or (b) confirm the query IS specific enough and upgrade to STRUCTURED.
        if (appId) {
          try {
            const { classification: richClass } = await classifyIntentRich(appId, question);
            if (!skipDisambiguation && richClass.disambiguation_needed && richClass.slot_questions && richClass.slot_questions.length > 0) {
              console.log('[Ask] CLARIFY upgraded to DISAMBIGUATION for:', question);
              response = {
                intent: 'DISAMBIGUATION',
                intentConfidence,
                interpretation: richClass.interpretation,
                disambiguation_reason: richClass.disambiguation_reason || '',
                slot_questions: richClass.slot_questions,
                suggestions: richClass.suggestions || [],
                slots: richClass.slots || {},
                confidence: richClass.confidence,
                answer: richClass.disambiguation_reason || 'Let me help you be more specific.',
                tokenUsage: classification.tokenUsage,
              };
              break;
            } else if (skipDisambiguation || (!richClass.disambiguation_needed && (richClass.confidence === 'high' || richClass.confidence === 'medium'))) {
              // Rich classify says this is a real structured query — upgrade to full BOKG pipeline
              console.log('[Ask] CLARIFY upgraded to STRUCTURED for:', question, '(rich confidence:', richClass.confidence, ')');
              const pResult = await runFullBOKGPipeline(appId, question, evidence, { dialect, selfConsistency, dryRun });
              pResult.results = deduplicateRows(pResult.results);

              let pAnswer = null;
              if (!pResult.execError && pResult.results.rows.length > 0) {
                pAnswer = await generateResultsSummary(question, pResult.results, pResult.sql);
              }

              response = {
                intent: 'STRUCTURED',
                intentConfidence,
                answer: pAnswer || (pResult.execError ? `Query error: ${pResult.execError}` : 'Query executed but returned no results.'),
                sql: pResult.sql,
                results: pResult.results,
                column_business_names: pResult.column_business_names,
                confidence: pResult.confidence,
                tablesUsed: pResult.tablesUsed,
                tokenUsage: mergeTokenUsage(classification.tokenUsage, pResult.tokenUsage),
              };
              break;
            }
          } catch (err) {
            console.warn('[Ask] Rich classify in CLARIFY failed:', err.message);
          }
        }

        response = {
          intent: 'CLARIFY',
          intentConfidence,
          answer: `I'd like to help, but I need a bit more context. ${reasoning || 'Could you be more specific about what you\'re looking for?'}\n\nYou can ask me:\n• Data questions (e.g., "How many invoices were processed last month?")\n• Documentation questions (e.g., "What is the AP approval workflow?")\n• Or both combined`,
          confidence: 'low',
          tokenUsage: classification.tokenUsage,
        };
        break;
      }
    }

    // Save assistant response
    await query(
      `INSERT INTO ida_conversations (app_id, workspace_id, user_id, session_id, role, content, intent, response_data, confidence, token_usage)
       VALUES ($1, $2, $3, $4, 'assistant', $5, $6, $7, $8, $9)`,
      [appId, wsId, userId, sid, response.answer,
       response.intent, JSON.stringify(response),
       response.confidence, JSON.stringify(response.tokenUsage)]
    );

    // Add session ID to response
    response.sessionId = sid;
    res.json(response);

  } catch (err) {
    console.error('Ask error:', err);
    res.status(500).json({ error: err.message, intent: 'ERROR' });
  }
});

// ─── Conversation History ───

// GET /api/ask/history?sessionId=X&appId=Y
router.get('/history', async (req, res) => {
  try {
    const { sessionId, appId } = req.query;
    if (!sessionId || !appId) {
      return res.status(400).json({ error: 'sessionId and appId required' });
    }

    const result = await query(
      `SELECT id, role, content, intent, confidence, response_data, created_at
       FROM ida_conversations
       WHERE session_id = $1 AND app_id = $2
       ORDER BY created_at ASC`,
      [sessionId, appId]
    );
    res.json({ messages: result.rows });
  } catch (err) {
    console.error('History error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/ask/sessions?appId=X — List recent sessions
router.get('/sessions', async (req, res) => {
  try {
    const { appId } = req.query;
    if (!appId) return res.status(400).json({ error: 'appId required' });

    const result = await query(
      `SELECT session_id, MIN(created_at) as started_at, MAX(created_at) as last_message,
              COUNT(*) as message_count,
              (SELECT content FROM ida_conversations ic2
               WHERE ic2.session_id = ic.session_id AND ic2.role = 'user'
               ORDER BY ic2.created_at ASC LIMIT 1) as first_question
       FROM ida_conversations ic
       WHERE app_id = $1
       GROUP BY session_id
       ORDER BY MAX(created_at) DESC
       LIMIT 50`,
      [appId]
    );
    res.json({ sessions: result.rows });
  } catch (err) {
    console.error('Sessions error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── Helper: Generate NL summary of SQL results ───

// ─── Helper: Synthesize HYBRID answer from both engines ───
// Avoids contradictions like "I can't show invoice data" when the structured engine already returned it.
async function synthesizeHybridAnswer(question, structured, unstructured) {
  try {
    const Anthropic = require('@anthropic-ai/sdk');
    const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

    const dataPreview = JSON.stringify(structured.results.rows.slice(0, 10), null, 2);
    const docAnswer = unstructured.answer || '';

    const response = await client.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 1024,
      system: `You are Data Ask, an enterprise data assistant. You are synthesizing a HYBRID answer that combines structured database results with document/policy context.

RULES:
- Start with a "**Data Analysis:**" section summarizing the query results (2-3 sentences, mention key numbers)
- Follow with "---" separator and a "**Documentation Context:**" section with the relevant policy/document insights
- IMPORTANT: The structured engine already returned the data results shown below. NEVER say you "cannot provide" or "don't have access to" data that is clearly present in the results.
- Remove any disclaimers from the document answer that claim data is unavailable — the data IS available in the structured results.
- Keep the document answer focused on policy/process insights, not data disclaimers.
- Do not use bullet points in the Data Analysis section.`,
      messages: [{
        role: 'user',
        content: `Question: ${question}

STRUCTURED RESULTS (${structured.results.rowCount || structured.results.rows.length} rows from SQL):
SQL: ${structured.sql}
${dataPreview}

DOCUMENT ANSWER:
${docAnswer}

Synthesize these into a single coherent hybrid answer.`,
      }],
    });
    return response.content[0].text;
  } catch (err) {
    // Fallback: simple concatenation if synthesis fails
    console.error('Hybrid synthesis error:', err.message);
    const summary = await generateResultsSummary(question, structured.results, structured.sql);
    let answer = `**Data Analysis:**\n${summary || 'Query returned results (see table below).'}`;
    if (unstructured?.answer) {
      answer += `\n\n---\n\n**Documentation Context:**\n${unstructured.answer}`;
    }
    return answer;
  }
}

async function generateResultsSummary(question, results, sql) {
  try {
    const Anthropic = require('@anthropic-ai/sdk');
    const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

    const preview = JSON.stringify(results.rows.slice(0, 20), null, 2);
    const response = await client.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 512,
      system: 'You are Data Ask, an enterprise data assistant. Summarize SQL query results in 2-3 concise sentences. Mention key numbers and trends. Do not use bullet points.',
      messages: [{
        role: 'user',
        content: `Question: ${question}\nSQL: ${sql}\nResults (${results.rowCount} rows):\n${preview}`,
      }],
    });
    return response.content[0].text;
  } catch (err) {
    return null;
  }
}

// ─── Helper: Deduplicate result rows ───
// Joins to child tables (e.g. invoice lines) can fan out header rows.
// Remove exact-duplicate rows to keep results clean.
function deduplicateRows(results) {
  if (!results || !results.rows || results.rows.length === 0) return results;
  const seen = new Set();
  const uniqueRows = [];
  for (const row of results.rows) {
    const key = JSON.stringify(row);
    if (!seen.has(key)) {
      seen.add(key);
      uniqueRows.push(row);
    }
  }
  if (uniqueRows.length < results.rows.length) {
    console.log(`[Dedup] Removed ${results.rows.length - uniqueRows.length} duplicate rows (${results.rows.length} → ${uniqueRows.length})`);
  }
  return { ...results, rows: uniqueRows, rowCount: uniqueRows.length };
}

// ─── Helper: Merge token usage from multiple stages ───

function mergeTokenUsage(...usages) {
  const merged = { stages: [], totalInput: 0, totalOutput: 0, totalCost: 0 };
  for (const usage of usages) {
    if (!usage) continue;
    merged.stages.push(usage);
    merged.totalInput += usage.input || 0;
    merged.totalOutput += usage.output || 0;
    merged.totalCost += parseFloat(usage.cost || 0);
  }
  return merged;
}

module.exports = router;
