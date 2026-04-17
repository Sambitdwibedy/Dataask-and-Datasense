const express = require('express');
const { query } = require('../db');

const router = express.Router();

// GET /api/quality/:appId/dashboard - comprehensive quality & trust metrics
router.get('/:appId/dashboard', async (req, res) => {
  try {
    const { appId } = req.params;

    // Column-level stats: totals, approval status, confidence tiers
    // All columns fully qualified — both app_columns and app_tables have enrichment_status, confidence_score, description
    const colStats = await query(
      `SELECT
        COUNT(*) as total_columns,
        COUNT(CASE WHEN ac.enrichment_status = 'approved' THEN 1 END) as approved,
        COUNT(CASE WHEN ac.enrichment_status = 'ai_enriched' THEN 1 END) as ai_enriched,
        COUNT(CASE WHEN ac.enrichment_status = 'rejected' THEN 1 END) as rejected,
        COUNT(CASE WHEN ac.enrichment_status = 'draft' THEN 1 END) as draft,
        COUNT(CASE WHEN ac.enrichment_status = 'needs_review' THEN 1 END) as needs_review,
        COALESCE(ROUND(AVG(ac.confidence_score)::numeric, 1), 0) as avg_confidence,
        COUNT(CASE WHEN ac.confidence_score >= 90 THEN 1 END) as high_conf,
        COUNT(CASE WHEN ac.confidence_score >= 60 AND ac.confidence_score < 90 THEN 1 END) as med_conf,
        COUNT(CASE WHEN ac.confidence_score > 0 AND ac.confidence_score < 60 THEN 1 END) as low_conf,
        COUNT(CASE WHEN ac.business_name IS NOT NULL AND ac.business_name != '' AND ac.business_name != ac.column_name THEN 1 END) as has_business_name,
        COUNT(CASE WHEN ac.description IS NOT NULL AND ac.description != '' THEN 1 END) as has_description
      FROM app_columns ac
      JOIN app_tables at ON ac.table_id = at.id
      WHERE at.app_id = $1`,
      [appId]
    );

    // Entity/table-level stats
    const entityStats = await query(
      `SELECT
        COUNT(*) as total_entities,
        COUNT(CASE WHEN enrichment_status = 'approved' THEN 1 END) as approved_entities,
        COUNT(CASE WHEN enrichment_status = 'ai_enriched' THEN 1 END) as enriched_entities,
        COUNT(CASE WHEN entity_metadata IS NOT NULL AND entity_metadata::text != '{}' AND entity_metadata::text != 'null' THEN 1 END) as has_metadata,
        COALESCE(ROUND(AVG(confidence_score)::numeric, 1), 0) as avg_entity_confidence
      FROM app_tables
      WHERE app_id = $1`,
      [appId]
    );

    // Relationship stats (defensive — table may not have enrichment columns yet)
    let relStats;
    try {
      relStats = await query(
        `SELECT
          COUNT(*) as total_relationships,
          COUNT(CASE WHEN enrichment_status = 'approved' THEN 1 END) as approved_relationships,
          COUNT(CASE WHEN enrichment_status = 'ai_enriched' THEN 1 END) as enriched_relationships
        FROM app_relationships
        WHERE app_id = $1`,
        [appId]
      );
    } catch (e) {
      // Fallback: just count relationships without enrichment status
      try {
        relStats = await query(
          `SELECT COUNT(*) as total_relationships, 0 as approved_relationships, 0 as enriched_relationships
           FROM app_relationships WHERE app_id = $1`,
          [appId]
        );
      } catch {
        relStats = { rows: [{ total_relationships: 0, approved_relationships: 0, enriched_relationships: 0 }] };
      }
    }

    // Domain breakdown from entity_metadata (defensive — JSONB may be null or missing)
    let domainBreakdown;
    try {
      domainBreakdown = await query(
        `SELECT
          COALESCE(
            NULLIF(NULLIF(NULLIF(
              at.entity_metadata->>'domain', 'General'), 'UNKNOWN'), ''),
            'Unclassified'
          ) as domain,
          COUNT(DISTINCT at.id) as table_count,
          COUNT(DISTINCT ac.id) as column_count,
          COUNT(DISTINCT CASE WHEN ac.enrichment_status = 'approved' THEN ac.id END) as approved_columns,
          COUNT(DISTINCT CASE WHEN ac.enrichment_status IN ('approved', 'ai_enriched') THEN ac.id END) as enriched_columns,
          COALESCE(ROUND(AVG(ac.confidence_score)::numeric, 1), 0) as avg_confidence
        FROM app_tables at
        LEFT JOIN app_columns ac ON at.id = ac.table_id
        WHERE at.app_id = $1
        GROUP BY domain
        ORDER BY column_count DESC`,
        [appId]
      );
    } catch {
      domainBreakdown = { rows: [] };
    }

    // Confidence distribution histogram (buckets of 10)
    const confDist = await query(
      `SELECT
        CASE
          WHEN ac.confidence_score >= 90 THEN '90-100'
          WHEN ac.confidence_score >= 80 THEN '80-89'
          WHEN ac.confidence_score >= 70 THEN '70-79'
          WHEN ac.confidence_score >= 60 THEN '60-69'
          WHEN ac.confidence_score >= 50 THEN '50-59'
          WHEN ac.confidence_score >= 40 THEN '40-49'
          WHEN ac.confidence_score >= 30 THEN '30-39'
          WHEN ac.confidence_score >= 20 THEN '20-29'
          WHEN ac.confidence_score >= 10 THEN '10-19'
          ELSE '0-9'
        END as bucket,
        COUNT(*) as count
      FROM app_columns ac
      JOIN app_tables at ON ac.table_id = at.id
      WHERE at.app_id = $1 AND ac.confidence_score > 0
      GROUP BY bucket
      ORDER BY bucket DESC`,
      [appId]
    );

    // Recent curation activity (last 10 approvals/rejections)
    const recentActivity = await query(
      `SELECT ac.column_name, at.table_name, ac.enrichment_status, ac.confidence_score, ac.enriched_at
      FROM app_columns ac
      JOIN app_tables at ON ac.table_id = at.id
      WHERE at.app_id = $1 AND ac.enrichment_status IN ('approved', 'rejected')
      ORDER BY ac.enriched_at DESC NULLS LAST
      LIMIT 10`,
      [appId]
    );

    // Pattern library count (defensive — table may not exist yet)
    let patternCount;
    try {
      patternCount = await query(
        `SELECT COUNT(*) as count FROM query_patterns WHERE app_id = $1`,
        [appId]
      );
    } catch {
      patternCount = { rows: [{ count: 0 }] };
    }

    // Synonym stats
    let synonymStats;
    try {
      synonymStats = await query(
        `SELECT
          COUNT(*) FILTER (WHERE status = 'active') as active,
          COUNT(*) FILTER (WHERE source = 'ai_generated' AND status = 'active') as ai_generated,
          COUNT(*) FILTER (WHERE source = 'builder_curated' AND status = 'active') as builder_curated,
          COUNT(*) FILTER (WHERE source IN ('solix_global', 'domain_pack') AND status = 'active') as global_pack,
          COUNT(DISTINCT column_id) FILTER (WHERE status = 'active') as columns_with_synonyms
        FROM app_synonyms WHERE app_id = $1`,
        [appId]
      );
    } catch {
      synonymStats = { rows: [{ active: 0, ai_generated: 0, builder_curated: 0, global_pack: 0, columns_with_synonyms: 0 }] };
    }

    // Compute Trust Score (weighted composite)
    const cs = colStats.rows[0];
    const es = entityStats.rows[0];
    const rs = relStats.rows[0];
    const totalCols = parseInt(cs.total_columns) || 1;
    const totalEntities = parseInt(es.total_entities) || 1;
    const totalRels = parseInt(rs.total_relationships) || 1;

    // Trust dimensions (0-100 each):
    const curationCoverage = Math.round((parseInt(cs.approved) / totalCols) * 100);  // % columns human-approved
    const enrichmentCoverage = Math.round(((parseInt(cs.approved) + parseInt(cs.ai_enriched) + parseInt(cs.needs_review)) / totalCols) * 100);  // % with any AI enrichment (including needs_review)
    const entityCoverage = Math.round((parseInt(es.approved_entities) / totalEntities) * 100);
    const relCoverage = totalRels > 0 ? Math.round((parseInt(rs.approved_relationships) / totalRels) * 100) : 0;
    const descCoverage = Math.round((parseInt(cs.has_description) / totalCols) * 100);
    const avgConf = parseFloat(cs.avg_confidence) || 0;

    // Weighted trust score: enrichment (30%) + curation bonus (15%) + confidence (25%) + description coverage (20%) + entity+rel coverage (10%)
    // enrichmentCoverage measures AI processing coverage; curationCoverage adds bonus for human approval
    const trustScore = Math.round(
      enrichmentCoverage * 0.30 +
      curationCoverage * 0.15 +
      avgConf * 0.25 +
      descCoverage * 0.20 +
      ((entityCoverage + relCoverage) / 2) * 0.10
    );

    res.json({
      trust_score: trustScore,
      columns: {
        total: parseInt(cs.total_columns),
        approved: parseInt(cs.approved),
        ai_enriched: parseInt(cs.ai_enriched),
        rejected: parseInt(cs.rejected),
        draft: parseInt(cs.draft),
        needs_review: parseInt(cs.needs_review),
        avg_confidence: parseFloat(cs.avg_confidence),
        high_conf: parseInt(cs.high_conf),
        med_conf: parseInt(cs.med_conf),
        low_conf: parseInt(cs.low_conf),
        has_business_name: parseInt(cs.has_business_name),
        has_description: parseInt(cs.has_description),
      },
      entities: {
        total: parseInt(es.total_entities),
        approved: parseInt(es.approved_entities),
        enriched: parseInt(es.enriched_entities),
        has_metadata: parseInt(es.has_metadata),
        avg_confidence: parseFloat(es.avg_entity_confidence),
      },
      relationships: {
        total: parseInt(rs.total_relationships),
        approved: parseInt(rs.approved_relationships),
        enriched: parseInt(rs.enriched_relationships),
      },
      patterns: parseInt(patternCount.rows[0].count),
      synonyms: {
        active: parseInt(synonymStats.rows[0].active) || 0,
        ai_generated: parseInt(synonymStats.rows[0].ai_generated) || 0,
        builder_curated: parseInt(synonymStats.rows[0].builder_curated) || 0,
        global_pack: parseInt(synonymStats.rows[0].global_pack) || 0,
        columns_with_synonyms: parseInt(synonymStats.rows[0].columns_with_synonyms) || 0,
      },
      trust_dimensions: {
        curation_coverage: curationCoverage,
        enrichment_coverage: enrichmentCoverage,
        entity_coverage: entityCoverage,
        relationship_coverage: relCoverage,
        description_coverage: descCoverage,
        avg_confidence: avgConf,
      },
      domains: domainBreakdown.rows,
      confidence_distribution: confDist.rows,
      recent_activity: recentActivity.rows,
    });
  } catch (err) {
    console.error('Get quality dashboard error:', err.message, err.stack);
    res.status(500).json({ error: 'Internal server error', detail: err.message });
  }
});

// GET /api/quality/:appId/confidence - confidence score distribution (detailed)
router.get('/:appId/confidence', async (req, res) => {
  try {
    const { appId } = req.params;

    const result = await query(
      `SELECT
        ac.id,
        ac.column_name,
        at.table_name,
        ac.confidence_score,
        ac.enrichment_status
      FROM app_columns ac
      JOIN app_tables at ON ac.table_id = at.id
      WHERE at.app_id = $1
      ORDER BY ac.confidence_score DESC`,
      [appId]
    );

    const distribution = {};
    result.rows.forEach((row) => {
      const score = parseFloat(row.confidence_score) || 0;
      const bucket = `${Math.floor(score / 10) * 10}-${Math.floor(score / 10) * 10 + 9}`;
      distribution[bucket] = (distribution[bucket] || 0) + 1;
    });

    res.json({ columns: result.rows, distribution });
  } catch (err) {
    console.error('Get confidence error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/quality/:appId/coverage - coverage by domain
router.get('/:appId/coverage', async (req, res) => {
  try {
    const { appId } = req.params;

    const result = await query(
      `SELECT
        COALESCE(
          NULLIF(NULLIF(NULLIF(
            at.entity_metadata->>'domain', 'General'), 'UNKNOWN'), ''),
          'Unclassified'
        ) as domain,
        at.id as table_id,
        at.table_name,
        at.entity_name,
        at.enrichment_status as entity_status,
        COUNT(DISTINCT ac.id) as total_columns,
        COUNT(DISTINCT CASE WHEN ac.enrichment_status = 'approved' THEN ac.id END) as approved_columns,
        COALESCE(ROUND(100.0 * COUNT(DISTINCT CASE WHEN ac.enrichment_status = 'approved' THEN ac.id END) / NULLIF(COUNT(DISTINCT ac.id), 0), 2), 0) as coverage_percentage
      FROM app_tables at
      LEFT JOIN app_columns ac ON at.id = ac.table_id
      WHERE at.app_id = $1
      GROUP BY at.id, at.table_name, at.entity_name, at.enrichment_status, at.entity_metadata
      ORDER BY domain, at.table_name`,
      [appId]
    );

    res.json({ coverage: result.rows });
  } catch (err) {
    console.error('Get coverage error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = router;
