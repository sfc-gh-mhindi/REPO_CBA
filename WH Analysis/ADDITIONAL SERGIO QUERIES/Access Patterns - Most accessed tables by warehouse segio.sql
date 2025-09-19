-- FRAUMD Warehouse Analysis Queries
-- Filtered for specific FRAUMD warehouses
call pst.svcs.sp_set_account_context (50973,'AU');
USE DATABASE PST;
USE SCHEMA PST.SVCS;



-- Most accessed tables by warehouse
WITH WAREHOUSE_SIZE AS
(
     SELECT WAREHOUSE_SIZE, NODES
       FROM (
              SELECT 'X-SMALL' AS WAREHOUSE_SIZE, 1 AS NODES
              UNION ALL
              SELECT 'SMALL' AS WAREHOUSE_SIZE, 2 AS NODES
              UNION ALL
              SELECT 'MEDIUM' AS WAREHOUSE_SIZE, 4 AS NODES
              UNION ALL
              SELECT 'LARGE' AS WAREHOUSE_SIZE, 8 AS NODES
              UNION ALL
              SELECT 'X-LARGE' AS WAREHOUSE_SIZE, 16 AS NODES
              UNION ALL
              SELECT '2X-LARGE' AS WAREHOUSE_SIZE, 32 AS NODES
              UNION ALL
              SELECT '3X-LARGE' AS WAREHOUSE_SIZE, 64 AS NODES
              UNION ALL
              SELECT '4X-LARGE' AS WAREHOUSE_SIZE, 128 AS NODES
            )
),
QH AS
(
     SELECT QH.QUERY_ID
           ,QH.QUERY_TEXT
           ,QH.USER_NAME
           ,QH.ROLE_NAME
           ,QH.EXECUTION_TIME
           ,QH.WAREHOUSE_SIZE
           ,QH.WAREHOUSE_NAME
           ,QH.BYTES_SCANNED
           ,QH.ROWS_PRODUCED
           ,QH.PERCENTAGE_SCANNED_FROM_CACHE
           ,QH.PARTITIONS_SCANNED
           ,QH.PARTITIONS_TOTAL
      FROM QUERY_HISTORY QH
     WHERE warehouse_name IN ('WH_USR_PRD_P01_FRAUMD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_001', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_002', 'WH_USR_PRD_P01_FRAUMD_LABMLFRD_003') 
       AND START_TIME > DATEADD(month,-2,CURRENT_TIMESTAMP())
       AND QH.BYTES_SCANNED > 0
),
TABLE_ACCESS_DETAILS AS (
    SELECT
        f1.value:"objectName"::string AS table_name,
        f1.value:"objectDomain"::string AS object_domain,
        QH.WAREHOUSE_NAME,
        QH.BYTES_SCANNED,
        QH.ROWS_PRODUCED,
        QH.PERCENTAGE_SCANNED_FROM_CACHE,
        QH.PARTITIONS_SCANNED,
        QH.PARTITIONS_TOTAL,
        QH.EXECUTION_TIME,
        access_history.query_id
    FROM
        PST.SVCS.access_history,
        LATERAL FLATTEN(input => base_objects_accessed) f1,
        QH
    WHERE
        f1.value:"objectDomain"::string = 'Table'
        AND QH.QUERY_ID = access_history.query_id
        AND f1.value:"objectName"::string IS NOT NULL
)

-- =====================================================================
-- 1. HOT TABLES: Most accessed tables across all warehouses
-- =====================================================================
SELECT 
    '1. HOT TABLES ANALYSIS' AS analysis_type,
    table_name,
    COUNT(DISTINCT warehouse_name) AS warehouses_using_table,
    COUNT(*) AS total_access_count,
    LISTAGG(DISTINCT warehouse_name, ', ') AS warehouse_list,
    AVG(bytes_scanned) AS avg_bytes_scanned,
    AVG(rows_produced) AS avg_rows_produced,
    AVG(execution_time) AS avg_execution_time_ms,
    -- Classification
    CASE 
        WHEN COUNT(DISTINCT warehouse_name) >= 3 THEN 'CROSS-WAREHOUSE HOT TABLE'
        WHEN COUNT(*) > 100 THEN 'SINGLE-WAREHOUSE HOT TABLE'
        ELSE 'MODERATE ACCESS'
    END AS table_classification
FROM TABLE_ACCESS_DETAILS
GROUP BY table_name
HAVING COUNT(*) > 10  -- Only show tables with significant access
ORDER BY total_access_count DESC
LIMIT 20

UNION ALL

-- =====================================================================
-- 2. SCAN PATTERNS: Large table scans vs targeted queries
-- =====================================================================
SELECT 
    '2. SCAN PATTERNS ANALYSIS' AS analysis_type,
    table_name,
    warehouse_name,
    COUNT(*) AS query_count,
    '',  -- placeholder for warehouse_list
    AVG(bytes_scanned) AS avg_bytes_scanned,
    AVG(rows_produced) AS avg_rows_produced,
    AVG(execution_time) AS avg_execution_time_ms,
    -- Scan pattern classification
    CASE 
        WHEN AVG(bytes_scanned) > 10737418240 THEN 'LARGE SCAN (>10GB)'  -- 10GB
        WHEN AVG(bytes_scanned) > 1073741824 THEN 'MEDIUM SCAN (1-10GB)'  -- 1GB
        WHEN AVG(bytes_scanned) > 104857600 THEN 'SMALL SCAN (100MB-1GB)'  -- 100MB
        ELSE 'TARGETED QUERY (<100MB)'
    END AS scan_pattern
FROM TABLE_ACCESS_DETAILS
WHERE partitions_scanned IS NOT NULL
GROUP BY table_name, warehouse_name
HAVING COUNT(*) > 5
ORDER BY avg_bytes_scanned DESC
LIMIT 20

UNION ALL

-- =====================================================================
-- 3. CACHE EFFICIENCY: Tables with low cache hit rates
-- =====================================================================
SELECT 
    '3. CACHE EFFICIENCY ANALYSIS' AS analysis_type,
    table_name,
    warehouse_name,
    COUNT(*) AS query_count,
    '',  -- placeholder for warehouse_list
    AVG(bytes_scanned) AS avg_bytes_scanned,
    AVG(percentage_scanned_from_cache) AS avg_cache_hit_rate,
    AVG(execution_time) AS avg_execution_time_ms,
    -- Cache efficiency classification
    CASE 
        WHEN AVG(percentage_scanned_from_cache) < 10 THEN 'POOR CACHE (<10%)'
        WHEN AVG(percentage_scanned_from_cache) < 30 THEN 'LOW CACHE (10-30%)'
        WHEN AVG(percentage_scanned_from_cache) < 60 THEN 'MODERATE CACHE (30-60%)'
        ELSE 'GOOD CACHE (>60%)'
    END AS cache_efficiency
FROM TABLE_ACCESS_DETAILS
WHERE percentage_scanned_from_cache IS NOT NULL
GROUP BY table_name, warehouse_name
HAVING COUNT(*) > 5
ORDER BY avg_cache_hit_rate ASC
LIMIT 20

UNION ALL

-- =====================================================================
-- 4. CROSS-WAREHOUSE ACCESS: Same tables used by different warehouses
-- =====================================================================
SELECT 
    '4. CROSS-WAREHOUSE ACCESS' AS analysis_type,
    table_name,
    '' AS warehouse_name,  -- Will show summary
    SUM(query_count) AS total_queries,
    LISTAGG(warehouse_details, ' | ') AS warehouse_access_details,
    AVG(avg_bytes_scanned) AS overall_avg_bytes_scanned,
    AVG(avg_rows_produced) AS overall_avg_rows_produced,
    AVG(avg_execution_time) AS overall_avg_execution_time_ms,
    CASE 
        WHEN COUNT(DISTINCT wh_name) >= 4 THEN 'ALL WAREHOUSES ACCESS'
        WHEN COUNT(DISTINCT wh_name) >= 3 THEN 'MOST WAREHOUSES ACCESS'
        WHEN COUNT(DISTINCT wh_name) = 2 THEN 'DUAL WAREHOUSE ACCESS'
        ELSE 'SINGLE WAREHOUSE'
    END AS cross_warehouse_pattern
FROM (
    SELECT 
        table_name,
        warehouse_name AS wh_name,
        COUNT(*) AS query_count,
        AVG(bytes_scanned) AS avg_bytes_scanned,
        AVG(rows_produced) AS avg_rows_produced,
        AVG(execution_time) AS avg_execution_time,
        warehouse_name || '(' || COUNT(*) || ' queries)' AS warehouse_details
    FROM TABLE_ACCESS_DETAILS
    GROUP BY table_name, warehouse_name
) wh_summary
GROUP BY table_name
HAVING COUNT(DISTINCT wh_name) > 1  -- Only show tables accessed by multiple warehouses
ORDER BY COUNT(DISTINCT wh_name) DESC, SUM(query_count) DESC
LIMIT 15;
/*
==============================================================================
COMPREHENSIVE TABLE ACCESS PATTERN ANALYSIS FOR FRAUMD WAREHOUSES
==============================================================================

This query provides 4 distinct analyses in a single result set:

1. HOT TABLES ANALYSIS:
   - Shows most frequently accessed tables across all warehouses
   - Identifies cross-warehouse shared tables vs single-warehouse tables
   - Includes average scan sizes and performance metrics
   - Classifications: CROSS-WAREHOUSE HOT TABLE, SINGLE-WAREHOUSE HOT TABLE, MODERATE ACCESS

2. SCAN PATTERNS ANALYSIS:
   - Categorizes queries by data volume scanned
   - Identifies large table scans (>10GB) vs targeted queries (<100MB)
   - Shows average bytes scanned, rows produced, and execution time
   - Classifications: LARGE SCAN, MEDIUM SCAN, SMALL SCAN, TARGETED QUERY

3. CACHE EFFICIENCY ANALYSIS:
   - Identifies tables with poor cache hit rates
   - Shows percentage of data scanned from cache vs disk
   - Helps identify tables that may benefit from better clustering or scheduling
   - Classifications: POOR CACHE (<10%), LOW CACHE (10-30%), MODERATE CACHE (30-60%), GOOD CACHE (>60%)

4. CROSS-WAREHOUSE ACCESS ANALYSIS:
   - Shows tables accessed by multiple warehouses
   - Provides query counts per warehouse for each table
   - Identifies potential consolidation opportunities
   - Classifications: ALL WAREHOUSES ACCESS, MOST WAREHOUSES ACCESS, DUAL WAREHOUSE ACCESS

OPTIMIZATION INSIGHTS:
- Hot tables accessed by multiple warehouses may benefit from dedicated caching strategies
- Large scan patterns indicate potential for better partitioning/clustering
- Poor cache efficiency suggests suboptimal query timing or table design
- Cross-warehouse access patterns may indicate opportunities for workload consolidation

WAREHOUSE SIZING IMPLICATIONS:
- Tables with consistent large scans may need larger warehouses
- Poor cache efficiency may require schedule optimization to leverage cached data
- Cross-warehouse hot tables may benefit from multi-cluster auto-scaling
- Targeted queries on hot tables are good candidates for smaller warehouses
*/