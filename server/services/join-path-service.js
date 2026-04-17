/**
 * Join Path Service — BFS shortest-path join planning from the knowledge graph.
 *
 * Builds an in-memory adjacency graph from app_relationships and uses BFS to find
 * the shortest join path between any two schema-linked tables. Automatically includes
 * intermediate/bridge tables and their join columns in the path.
 *
 * Why: Current 1-hop expansion misses multi-hop paths (e.g., AP_INVOICES →
 * AP_SUPPLIER_SITES → AP_SUPPLIERS). The LLM can't JOIN through tables it doesn't
 * know about. BFS guarantees the shortest path and includes all bridge tables.
 */

const { query } = require('../db');

/**
 * Build an adjacency graph from app_relationships for a given application.
 * Returns a Map where each table_id maps to an array of edges:
 *   { neighbor: table_id, fromCol: string, toCol: string, relType: string }
 *
 * The graph is bidirectional — if A→B exists, both A and B get edges.
 */
async function buildAdjacencyGraph(appId) {
  const result = await query(
    `SELECT ar.from_table_id, ar.from_column, ar.to_table_id, ar.to_column,
            ar.rel_type, ar.cardinality,
            ft.table_name AS from_table_name, tt.table_name AS to_table_name
     FROM app_relationships ar
     JOIN app_tables ft ON ar.from_table_id = ft.id
     JOIN app_tables tt ON ar.to_table_id = tt.id
     WHERE ar.app_id = $1`,
    [appId]
  );

  const graph = new Map();
  const tableNames = new Map(); // table_id → table_name

  for (const rel of result.rows) {
    tableNames.set(rel.from_table_id, rel.from_table_name);
    tableNames.set(rel.to_table_id, rel.to_table_name);

    // Add edge: from → to
    if (!graph.has(rel.from_table_id)) graph.set(rel.from_table_id, []);
    graph.get(rel.from_table_id).push({
      neighbor: rel.to_table_id,
      fromCol: rel.from_column,
      toCol: rel.to_column,
      relType: rel.rel_type,
      cardinality: rel.cardinality,
      fromTable: rel.from_table_name,
      toTable: rel.to_table_name,
    });

    // Add reverse edge: to → from (graph is bidirectional for path finding)
    if (!graph.has(rel.to_table_id)) graph.set(rel.to_table_id, []);
    graph.get(rel.to_table_id).push({
      neighbor: rel.from_table_id,
      fromCol: rel.to_column,       // reversed
      toCol: rel.from_column,        // reversed
      relType: rel.rel_type,
      cardinality: rel.cardinality,
      fromTable: rel.to_table_name,  // reversed
      toTable: rel.from_table_name,  // reversed
    });
  }

  return { graph, tableNames };
}

/**
 * BFS shortest path between two tables in the relationship graph.
 *
 * Returns an array of join steps:
 *   [{ fromTable, fromCol, toTable, toCol, relType }]
 *
 * Returns null if no path exists (tables are in disconnected subgraphs).
 * Max depth of 5 hops to prevent runaway traversal on large schemas.
 */
function bfsShortestPath(graph, startId, endId, maxDepth = 5) {
  if (startId === endId) return [];

  const visited = new Set([startId]);
  // Queue entries: [currentId, path so far]
  const queue = [[startId, []]];

  while (queue.length > 0) {
    const [currentId, path] = queue.shift();

    if (path.length >= maxDepth) continue;

    const edges = graph.get(currentId) || [];
    for (const edge of edges) {
      if (visited.has(edge.neighbor)) continue;

      const newPath = [...path, {
        fromTable: edge.fromTable,
        fromCol: edge.fromCol,
        toTable: edge.toTable,
        toCol: edge.toCol,
        relType: edge.relType,
        bridgeTableId: edge.neighbor,
      }];

      if (edge.neighbor === endId) {
        return newPath;
      }

      visited.add(edge.neighbor);
      queue.push([edge.neighbor, newPath]);
    }
  }

  return null; // No path found
}

/**
 * Find shortest join paths between all pairs of selected tables.
 *
 * Given a set of schema-linked table IDs, computes the BFS shortest path between
 * every pair and returns:
 *   - bridgeTableIds: additional table IDs needed for multi-hop joins
 *   - joinPaths: formatted join path descriptions for the LLM context
 *
 * Only includes paths that require intermediate tables (1+ hops beyond direct).
 */
async function findJoinPaths(appId, selectedTableIds) {
  if (!selectedTableIds || selectedTableIds.length < 2) {
    return { bridgeTableIds: [], joinPaths: [] };
  }

  const { graph, tableNames } = await buildAdjacencyGraph(appId);

  const bridgeTableIds = new Set();
  const joinPaths = [];
  const selectedSet = new Set(selectedTableIds);

  // Find paths between all pairs of selected tables
  for (let i = 0; i < selectedTableIds.length; i++) {
    for (let j = i + 1; j < selectedTableIds.length; j++) {
      const startId = selectedTableIds[i];
      const endId = selectedTableIds[j];
      const path = bfsShortestPath(graph, startId, endId);

      if (!path || path.length === 0) continue;

      // Check if path requires intermediate tables not already selected
      const intermediates = [];
      for (const step of path) {
        if (!selectedSet.has(step.bridgeTableId) && step.bridgeTableId !== startId && step.bridgeTableId !== endId) {
          bridgeTableIds.add(step.bridgeTableId);
          intermediates.push(step.bridgeTableId);
        }
      }

      // Format join path for LLM context
      const startName = tableNames.get(startId) || `table_${startId}`;
      const endName = tableNames.get(endId) || `table_${endId}`;

      if (path.length === 1) {
        // Direct relationship — already covered by RELATIONSHIPS section
        // Only include if useful as explicit guidance
        joinPaths.push({
          from: startName,
          to: endName,
          hops: 1,
          steps: path,
          description: `"${path[0].fromTable}"."${path[0].fromCol}" = "${path[0].toTable}"."${path[0].toCol}"`,
        });
      } else {
        // Multi-hop path — this is the key value add
        const stepDescriptions = path.map(s =>
          `"${s.fromTable}"."${s.fromCol}" = "${s.toTable}"."${s.toCol}"`
        );
        joinPaths.push({
          from: startName,
          to: endName,
          hops: path.length,
          steps: path,
          intermediates: intermediates.map(id => tableNames.get(id)),
          description: stepDescriptions.join(' → '),
        });
      }
    }
  }

  return {
    bridgeTableIds: [...bridgeTableIds],
    joinPaths: joinPaths.filter(p => p.hops > 0),
  };
}

/**
 * Format join paths as LLM context text.
 * Produces a JOIN_PATHS section that gives the LLM explicit step-by-step
 * instructions for multi-hop joins, so it doesn't have to figure out
 * intermediate tables from raw relationships.
 */
function formatJoinPathsContext(joinPaths) {
  if (!joinPaths || joinPaths.length === 0) return '';

  const lines = [];
  lines.push('COMPUTED JOIN PATHS (use these for multi-table queries):');
  lines.push('These are the shortest join paths between the selected tables.');
  lines.push('For multi-hop paths, you MUST include the intermediate tables in your JOINs.');
  lines.push('');

  // Prioritize multi-hop paths (those are the ones the LLM struggles with)
  const multiHop = joinPaths.filter(p => p.hops > 1);
  const directPaths = joinPaths.filter(p => p.hops === 1);

  if (multiHop.length > 0) {
    lines.push('Multi-hop join paths (IMPORTANT — requires intermediate tables):');
    for (const path of multiHop) {
      lines.push(`  ${path.from} → ${path.to} (${path.hops} hops via ${path.intermediates.join(', ')}):`);
      for (const step of path.steps) {
        lines.push(`    JOIN "${step.toTable}" ON "${step.fromTable}"."${step.fromCol}" = "${step.toTable}"."${step.toCol}"`);
      }
    }
    lines.push('');
  }

  // Only include direct paths if there aren't too many (avoid bloating the prompt)
  if (directPaths.length > 0 && directPaths.length <= 20) {
    lines.push('Direct join paths:');
    for (const path of directPaths) {
      lines.push(`  ${path.from} ↔ ${path.to}: ${path.description}`);
    }
    lines.push('');
  }

  return lines.join('\n');
}

module.exports = {
  buildAdjacencyGraph,
  bfsShortestPath,
  findJoinPaths,
  formatJoinPathsContext,
};
