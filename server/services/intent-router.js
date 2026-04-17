/**
 * Intent Router — Classifies user questions into query intent categories
 *
 * Uses Claude Haiku for fast, cheap classification:
 *   STRUCTURED   → NL2SQL against the knowledge graph (app_tables/columns/relationships)
 *   UNSTRUCTURED → RAG search against uploaded documents
 *   HYBRID       → Both structured + unstructured, merged response
 *   CLARIFY      → Question is too vague; ask for clarification
 */
const { query } = require('../db');

// ─── Intent Classification ───

/**
 * Classify a user question into an intent category.
 * Uses app context (available tables, document collections) to decide.
 *
 * Returns { intent, confidence, reasoning }
 */
async function classifyIntent(question, { appId, workspaceId } = {}) {
  const Anthropic = require('@anthropic-ai/sdk');
  const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

  // Gather context about what's available in this workspace
  let structuredContext = 'No structured data available.';
  let unstructuredContext = 'No documents uploaded.';

  if (appId) {
    // Check for structured data (AKG tables)
    const [tablesRows] = await query(
      `SELECT COUNT(*) as table_count FROM app_tables WHERE app_id = ?`,
      [appId]
    );
    const tableCount = parseInt(tablesRows[0].table_count);

    if (tableCount > 0) {
      const [sampleTables] = await query(
        `SELECT table_name, entity_name, JSON_UNQUOTE(JSON_EXTRACT(entity_metadata, '$.domain')) as domain
         FROM app_tables WHERE app_id = ?
         ORDER BY table_name LIMIT 20`,
        [appId]
      );
      const tableList = sampleTables
        .map(t => `${t.entity_name || t.table_name} (${t.domain || 'uncategorized'})`)
        .join(', ');
      structuredContext = `${tableCount} database tables available including: ${tableList}`;
    }
  }

  // Check for unstructured documents — prefer workspace scope, fall back to app scope
  if (workspaceId) {
    const [docsRows] = await query(
      `SELECT COUNT(*) as doc_count FROM doc_sources ds
       JOIN doc_collections dc ON ds.collection_id = dc.id
       WHERE dc.workspace_id = ? AND ds.status = 'ready'`,
      [workspaceId]
    );
    const docCount = parseInt(docsRows[0].doc_count);

    if (docCount > 0) {
      const [sampleDocs] = await query(
        `SELECT ds.filename FROM doc_sources ds
         JOIN doc_collections dc ON ds.collection_id = dc.id
         WHERE dc.workspace_id = ? AND ds.status = 'ready'
         ORDER BY ds.created_at DESC LIMIT 10`,
        [workspaceId]
      );
      const docList = sampleDocs.map(d => d.filename).join(', ');
      unstructuredContext = `${docCount} documents available including: ${docList}`;
    }
  } else if (appId) {
    // Fallback: search by app_id (backward compat)
    const [docsRows] = await query(
      `SELECT COUNT(*) as doc_count FROM doc_sources WHERE app_id = ? AND status = 'ready'`,
      [appId]
    );
    const docCount = parseInt(docsRows[0].doc_count);

    if (docCount > 0) {
      const [sampleDocs2] = await query(
        `SELECT filename FROM doc_sources WHERE app_id = ? AND status = 'ready' ORDER BY created_at DESC LIMIT 10`,
        [appId]
      );
      const docList = sampleDocs2.map(d => d.filename).join(', ');
      unstructuredContext = `${docCount} documents available including: ${docList}`;
    }
  }

  const systemPrompt = `You are an intent classifier for Data Ask, an enterprise data system.
Your job is to classify user questions into one of four categories:

STRUCTURED — The question asks for specific data, metrics, counts, lists, aggregations, or comparisons that can be answered by querying a database. This includes questions about reference/lookup data like types, statuses, categories, or codes stored in master tables. Examples: "How many invoices last month?", "Top 10 vendors by spend", "What are the different order statuses?", "List all budget types", "What product categories exist?", "Show me the payment terms", "What types of adjustments are there?".

UNSTRUCTURED — The question asks about business processes, step-by-step procedures, how-to instructions, or policy explanations that require human-written documentation to answer. Examples: "How do I create a purchase order?", "What is the approval workflow?", "Explain the GL reconciliation process", "What's the policy for expense reports?".

HYBRID — The question needs BOTH data from the database AND context from documents to fully answer. Examples: "Why are AP aging numbers high and what's the escalation process?", "Show me overdue invoices and explain the collection policy".

CLARIFY — The question is too vague, ambiguous, or nonsensical to classify. You need more information. Examples: "Help me", "What about that thing?", "Numbers".

IMPORTANT: When the question asks "what are the types/kinds/categories of X" or "what X exist" — this is STRUCTURED, not UNSTRUCTURED. Enterprise databases store type/status/category definitions in reference tables. Only classify as UNSTRUCTURED when the user is asking for process knowledge or how-to guidance that wouldn't be stored as database records.

Available data sources:
- Structured: ${structuredContext}
- Unstructured: ${unstructuredContext}

Respond with EXACTLY this JSON format (no markdown, no code fences):
{"intent": "STRUCTURED|UNSTRUCTURED|HYBRID|CLARIFY", "confidence": 0.0-1.0, "reasoning": "brief explanation"}`;

  const response = await client.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 256,
    system: systemPrompt,
    messages: [{ role: 'user', content: question }],
  });

  let text = response.content[0].text.trim();

  // Strip markdown code blocks if Haiku wrapped the JSON
  const codeBlockMatch = text.match(/```(?:json)?\s*\n?([\s\S]*?)```/);
  if (codeBlockMatch) text = codeBlockMatch[1].trim();

  const tokenUsage = {
    input: response.usage.input_tokens,
    output: response.usage.output_tokens,
    model: 'claude-haiku-4-5-20251001',
  };

  // Try JSON parse
  try {
    const parsed = JSON.parse(text);
    return {
      intent: parsed.intent || 'STRUCTURED',
      confidence: parsed.confidence || 0.5,
      reasoning: parsed.reasoning || '',
      tokenUsage,
    };
  } catch (err) {
    // Fallback: try to extract intent keyword from text
    const intentMatch = text.match(/\b(STRUCTURED|UNSTRUCTURED|HYBRID|CLARIFY)\b/);
    // Default to STRUCTURED if we have tables, rather than CLARIFY
    const fallbackIntent = intentMatch ? intentMatch[1] : (structuredContext !== 'No structured data available.' ? 'STRUCTURED' : 'CLARIFY');
    return {
      intent: fallbackIntent,
      confidence: 0.4,
      reasoning: `Fallback classification from: ${text.substring(0, 100)}`,
      tokenUsage,
    };
  }
}

module.exports = { classifyIntent };
