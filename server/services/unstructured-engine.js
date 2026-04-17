/**
 * Unstructured Engine — Vector Search + Full-Text Search + RAG Synthesis
 *
 * Performs hybrid search (semantic + keyword) against doc_chunks,
 * then synthesises an answer with Claude using retrieved context.
 */
const { query } = require('../db');
const { embedSingleText, EMBEDDING_MODEL, EMBEDDING_DIMS } = require('./document-pipeline');

// ─── Configuration ───
const TOP_K = 10;             // Max chunks to retrieve
const SEMANTIC_WEIGHT = 0.7;  // Weight for vector similarity
const KEYWORD_WEIGHT = 0.3;   // Weight for full-text match
const MIN_SCORE = 0.3;        // Minimum combined score to include

// ─── Hybrid Search ───

/**
 * Full-text search (MySQL LIKE-based fallback — no pgvector available).
 * Returns top-K chunks matching the query text.
 */
async function hybridSearch(questionText, { appId, workspaceId, collectionId, crossWorkspaceIds, topK = TOP_K } = {}) {
  // Build filter clause — workspace-aware with cross-workspace support
  const conditions = ['dc.content LIKE ?'];
  const params = [`%${questionText}%`];

  if (crossWorkspaceIds && crossWorkspaceIds.length > 0) {
    // Cross-workspace search: search across all user's assigned workspaces
    const placeholders = crossWorkspaceIds.map(() => '?').join(', ');
    conditions.push(`dc.workspace_id IN (${placeholders})`);
    params.push(...crossWorkspaceIds);
  } else if (workspaceId) {
    conditions.push(`dc.workspace_id = ?`);
    params.push(workspaceId);
  } else if (appId) {
    conditions.push(`dc.app_id = ?`);
    params.push(appId);
  }

  if (collectionId) {
    conditions.push(`dc.collection_id = ?`);
    params.push(collectionId);
  }

  params.push(topK);

  const [rows] = await query(
    `SELECT
      dc.id,
      dc.source_id,
      dc.chunk_index,
      dc.content,
      dc.metadata,
      1.0 AS semantic_score,
      1.0 AS keyword_score,
      1.0 AS combined_score,
      ds.filename,
      ds.file_type
    FROM doc_chunks dc
    JOIN doc_sources ds ON dc.source_id = ds.id
    WHERE ${conditions.join(' AND ')}
    ORDER BY dc.id
    LIMIT ?`,
    params
  );
  return rows;
}

// ─── RAG Synthesis ───

/**
 * Given a user question and retrieved chunks, synthesise an answer using Claude.
 * Returns { answer, citations, confidence }.
 */
async function synthesiseAnswer(question, chunks, { conversationHistory = [] } = {}) {
  const Anthropic = require('@anthropic-ai/sdk');
  const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

  // Build context from chunks
  const contextBlocks = chunks.map((chunk, i) => {
    const source = chunk.filename || `Source ${chunk.source_id}`;
    return `[${i + 1}] ${source} (chunk ${chunk.chunk_index}, score: ${Number(chunk.combined_score).toFixed(3)})\n${chunk.content}`;
  }).join('\n\n---\n\n');

  const systemPrompt = `You are Data Ask, a helpful enterprise data assistant for Solix Technologies customers.
You answer questions based on the provided document context. Follow these rules:

1. Answer ONLY based on the provided context. If the context doesn't contain enough information, say so clearly.
2. Cite your sources using [1], [2], etc. corresponding to the chunk numbers.
3. Be concise but thorough. Use specific details from the documents.
4. If the question is ambiguous, note the ambiguity and answer the most likely interpretation.
5. Format answers with clear structure when appropriate (paragraphs, not bullet points unless listing items).
6. Never fabricate information not in the context.`;

  const userMessage = `Context documents:

${contextBlocks}

---

Question: ${question}

Provide a thorough answer with citations.`;

  // Build messages array with conversation history
  const messages = [];
  for (const msg of conversationHistory.slice(-6)) {  // Last 3 turns
    messages.push({ role: msg.role, content: msg.content });
  }
  messages.push({ role: 'user', content: userMessage });

  const response = await client.messages.create({
    model: 'claude-sonnet-4-20250514',
    max_tokens: 2048,
    system: systemPrompt,
    messages,
  });

  const answer = response.content[0].text;

  // Extract citation references from the answer
  const citationMatches = answer.match(/\[(\d+)\]/g) || [];
  const citedIndices = [...new Set(citationMatches.map(m => parseInt(m.replace(/[\[\]]/g, '')) - 1))];
  const citations = citedIndices
    .filter(i => i >= 0 && i < chunks.length)
    .map(i => ({
      chunkId: chunks[i].id,
      sourceId: chunks[i].source_id,
      filename: chunks[i].filename,
      chunkIndex: chunks[i].chunk_index,
      excerpt: chunks[i].content.substring(0, 200) + '...',
      score: Number(chunks[i].combined_score).toFixed(3),
    }));

  // Determine confidence based on top chunk scores
  const avgScore = chunks.length > 0
    ? chunks.reduce((sum, c) => sum + Number(c.combined_score), 0) / chunks.length
    : 0;
  let confidence = 'low';
  if (avgScore > 0.7) confidence = 'high';
  else if (avgScore > 0.5) confidence = 'medium';

  return {
    answer,
    citations,
    confidence,
    chunksUsed: chunks.length,
    tokenUsage: {
      input: response.usage.input_tokens,
      output: response.usage.output_tokens,
      model: 'claude-sonnet-4-20250514',
    },
  };
}

// ─── Full Unstructured Query ───

/**
 * End-to-end unstructured query: search → synthesise.
 */
async function answerFromDocuments(question, { appId, workspaceId, collectionId, conversationHistory = [], crossWorkspaceIds } = {}) {
  // 1. Hybrid search (workspace-aware, with optional cross-workspace)
  const chunks = await hybridSearch(question, { appId, workspaceId, collectionId, crossWorkspaceIds });

  if (chunks.length === 0) {
    const scope = crossWorkspaceIds ? 'any of your workspaces' : 'this workspace';
    return {
      answer: `I couldn't find any relevant information in the uploaded documents in ${scope} to answer your question. Please try rephrasing, or make sure relevant documents have been uploaded.`,
      citations: [],
      confidence: 'low',
      chunksUsed: 0,
      tokenUsage: null,
    };
  }

  // 2. Synthesise answer
  const result = await synthesiseAnswer(question, chunks, { conversationHistory });
  return result;
}

module.exports = {
  hybridSearch,
  synthesiseAnswer,
  answerFromDocuments,
};
