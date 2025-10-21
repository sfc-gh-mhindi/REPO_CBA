# DMVA Performance Estimation - Quick Start

## Installation

Deploy the performance estimation views and functions:

```sql
-- 1. Create the performance estimate view
@snowflake/07_Views/dmva_performance_estimate_vw.sql

-- 2. Create the performance summary view
@snowflake/07_Views/dmva_performance_summary_vw.sql

-- 3. Create the performance estimation function
@snowflake/05_Functions/dmva_estimate_performance.sql
```

## Most Common Queries

### 1. Check All Tables for Issues

```sql
SELECT 
    object_name,
    table_size_gb,
    total_column_count,
    aggregates_per_query,
    estimated_total_time_mins,
    recommendation
FROM dmva_performance_estimate_vw
WHERE recommendation NOT LIKE '%✅%'
ORDER BY aggregates_per_query DESC;
```

### 2. Check Specific Table

```sql
SELECT * FROM TABLE(dmva_estimate_performance('YOUR_TABLE_NAME'));
```

### 3. System-Wide Summary

```sql
SELECT 
    system_name,
    num_tables,
    round(total_size_gb, 1) as size_gb,
    round(total_sequential_time_hours, 1) as seq_hours,
    round(estimated_time_16_workers_mins / 60, 1) as parallel_16w_hours,
    max_disk_space_gb as disk_gb,
    tables_exceeding_aggregate_limit as problem_tables
FROM dmva_performance_summary_vw
WHERE is_source = true;
```

### 4. Test Different max_select_columns

```sql
-- Test different values to find optimal setting
SELECT 
    test_value as max_select_columns,
    aggregates_per_query,
    measure_queries,
    total_time_mins,
    case 
        when aggregates_per_query > 77 then '❌ TOO HIGH'
        when aggregates_per_query > 64 then '⚠️  RISKY'
        else '✅ SAFE'
    end as status
FROM (
    SELECT 200 as test_value, * FROM TABLE(dmva_estimate_performance(null, 'YOUR_TABLE', 200))
    UNION ALL
    SELECT 154 as test_value, * FROM TABLE(dmva_estimate_performance(null, 'YOUR_TABLE', 154))
    UNION ALL
    SELECT 128 as test_value, * FROM TABLE(dmva_estimate_performance(null, 'YOUR_TABLE', 128))
    UNION ALL
    SELECT 90 as test_value, * FROM TABLE(dmva_estimate_performance(null, 'YOUR_TABLE', 90))
    UNION ALL
    SELECT 76 as test_value, * FROM TABLE(dmva_estimate_performance(null, 'YOUR_TABLE', 76))
)
ORDER BY test_value DESC;
```

### 5. Apply Recommended Fix

```sql
-- For tables exceeding aggregate limit
UPDATE dmva_object_info
SET overrides = object_construct('max_select_columns', 76)
WHERE object_id IN (
    SELECT object_id
    FROM dmva_performance_estimate_vw
    WHERE system_type = 'TERADATA'
      AND aggregates_per_query > 64
);
```

### 6. Compare Warehouse Sizes

```sql
WITH warehouse_comparison AS (
    SELECT 'XS' as wh, * FROM TABLE(dmva_estimate_performance(null, 'YOUR_TABLE', null, 'XS'))
    UNION ALL
    SELECT 'S', * FROM TABLE(dmva_estimate_performance(null, 'YOUR_TABLE', null, 'S'))
    UNION ALL
    SELECT 'M', * FROM TABLE(dmva_estimate_performance(null, 'YOUR_TABLE', null, 'M'))
    UNION ALL
    SELECT 'L', * FROM TABLE(dmva_estimate_performance(null, 'YOUR_TABLE', null, 'L'))
)
SELECT 
    wh as warehouse,
    total_time_mins,
    data_movement_time_mins,
    bottleneck
FROM warehouse_comparison
ORDER BY total_time_mins;
```

## Your 135-Column Table Example

```sql
-- 1. Check current configuration
SELECT 
    object_name,
    measure_column_count as columns,
    max_select_columns as current_max_select,
    columns_per_query,
    num_measure_queries,
    aggregates_per_query,
    recommendation
FROM dmva_performance_estimate_vw
WHERE object_name = 'YOUR_135_COLUMN_TABLE';

-- 2. If it shows aggregates_per_query > 77, test the fix:
SELECT * 
FROM TABLE(dmva_estimate_performance(
    filter_object_name => 'YOUR_135_COLUMN_TABLE',
    what_if_max_select_columns => 76
));

-- 3. Apply the fix:
UPDATE dmva_object_info
SET overrides = object_construct('max_select_columns', 76)
WHERE object_name = 'YOUR_135_COLUMN_TABLE';

-- 4. Verify:
SELECT 
    object_name,
    aggregates_per_query,
    num_measure_queries,
    recommendation
FROM dmva_performance_estimate_vw
WHERE object_name = 'YOUR_135_COLUMN_TABLE';
-- Should now show: aggregates_per_query ≤ 64, recommendation: ✅
```

## Migration Planning Query

```sql
-- Generate complete migration plan with priorities
SELECT 
    row_number() over (partition by database_name order by estimated_total_time_mins desc) as db_priority,
    system_name,
    database_name,
    schema_name,
    object_name,
    round(table_size_gb, 1) as gb,
    total_column_count as cols,
    num_measure_queries as meas_q,
    round(estimated_total_time_mins, 0) as mins,
    estimated_disk_space_gb as disk_gb,
    bottleneck,
    case 
        when recommendation like '%✅%' then 'Ready'
        when recommendation like '%⚠️%' then 'Needs Config'
        else 'Critical Issue'
    end as status
FROM dmva_performance_estimate_vw
WHERE is_source = true
  AND active = true
ORDER BY database_name, db_priority;
```

## Export Performance Report

```sql
-- Create a comprehensive report for stakeholders
SELECT 
    'Summary' as report_section,
    system_name,
    cast(num_tables as varchar) as metric_name,
    'tables' as unit
FROM dmva_performance_summary_vw
WHERE is_source = true

UNION ALL

SELECT 
    'Summary',
    system_name,
    cast(round(total_size_gb, 1) as varchar),
    'GB'
FROM dmva_performance_summary_vw
WHERE is_source = true

UNION ALL

SELECT 
    'Summary',
    system_name,
    cast(round(total_sequential_time_hours, 1) as varchar),
    'hours (sequential)'
FROM dmva_performance_summary_vw
WHERE is_source = true

UNION ALL

SELECT 
    'Summary',
    system_name,
    cast(round(estimated_time_16_workers_mins / 60, 1) as varchar),
    'hours (16 workers)'
FROM dmva_performance_summary_vw
WHERE is_source = true

UNION ALL

SELECT 
    'Issues',
    system_name,
    cast(tables_exceeding_aggregate_limit as varchar),
    'tables need config'
FROM dmva_performance_summary_vw
WHERE is_source = true

ORDER BY report_section, system_name;
```

## Monitoring During Migration

```sql
-- Compare estimates vs actuals (after tasks run)
SELECT 
    e.object_name,
    e.estimated_total_time_mins as estimated_mins,
    round(datediff('second', min(t.start_ts), max(t.finish_ts)) / 60, 1) as actual_mins,
    round(
        (datediff('second', min(t.start_ts), max(t.finish_ts)) / 60) / 
        nullif(e.estimated_total_time_mins, 0) * 100, 
        0
    ) as pct_of_estimate,
    case 
        when actual_mins <= estimated_mins then '✅ On Time'
        when actual_mins <= estimated_mins * 1.2 then '⚠️  Slightly Over'
        else '❌ Over Estimate'
    end as performance
FROM dmva_performance_estimate_vw e
JOIN dmva_object_info o ON e.object_id = o.object_id
JOIN dmva_tasks t ON t.request_payload:database_name = o.database_name
    AND t.request_payload:schema_name = o.schema_name
    AND t.request_payload:object_name = o.object_name
    AND t.status_cd = 'OK'
WHERE t.task_type IN ('measure_partition', 'unload_partition', 'load_partition')
GROUP BY e.object_name, e.estimated_total_time_mins, actual_mins
ORDER BY object_name;
```

## Key Metrics Reference

| Metric | Meaning | Action if High |
|--------|---------|---------------|
| `aggregates_per_query` | Number of aggregate functions per measure query | Reduce `max_select_columns` if > 64 |
| `num_measure_queries` | Number of queries to profile table | Normal - more queries = safer |
| `estimated_total_time_mins` | Total migration time | Consider larger warehouse or partitioning |
| `disk_space_gb` | VM disk space needed | Ensure VM has sufficient storage |
| `bottleneck` | Performance limiting factor | Optimize the bottleneck component |

## Quick Fixes

| Problem | Solution |
|---------|----------|
| `aggregates_per_query > 77` | Set `max_select_columns = 76` (or lower) |
| `aggregates_per_query > 64` | Set `max_select_columns = 90` (conservative) |
| Bottleneck: Snowflake Load | Use larger warehouse (M or L) |
| Bottleneck: Network Transfer | Enable compression or WRITE_NOS |
| Bottleneck: Teradata Unload | Increase partitioning |
| High disk space requirement | Process tables sequentially or expand disk |

---

**For detailed documentation**, see: [Performance_Estimation.md](./documentation/Performance_Estimation.md)
