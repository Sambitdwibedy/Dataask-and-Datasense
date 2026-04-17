-- ============================================================
-- REPORT: Regional Demographics Summary
-- Author: J. Novak, Analytics Dept.
-- Last Modified: 2018-03-14
-- Source System: Czech Statistical Office data feed
-- NOTE: District reference data loaded quarterly from CSO export
-- ============================================================

-- District population breakdown by municipality size bands
-- Used by: Marketing segmentation, Branch network planning
SELECT
    d.A2                    AS district_name,
    d.A3                    AS region,
    d.A4                    AS total_population,
    -- Municipality size distribution (CSO standard bands)
    d.A5                    AS munic_under_500,
    d.A6                    AS munic_500_to_1999,
    d.A7                    AS munic_2000_to_9999,
    d.A8                    AS munic_over_10000,
    -- Economic indicators
    d.A10                   AS urban_ratio_pct,
    d.A11                   AS avg_monthly_salary_czk,
    -- A12 and A13 are unemployment but I can never remember
    -- which year is which. One is 95 and one is 96.
    d.A12                   AS unemployment_rate_1,
    d.A13                   AS unemployment_rate_2
    -- Skipping A14-A16, not sure what they are. Crime stats maybe?
    -- A9 is garbage, don't use
FROM district d
WHERE d.A4 > 50000  -- Major districts only
ORDER BY d.A4 DESC;
