# How to Collect Complete DMVA Performance Data

This guide shows how to gather all necessary parameters for accurate performance calibration.

## Required Data Points

For each table migration, collect these parameters:

### 1. Basic Table Metrics
- ✅ Already have: DB Name, Table Name, Nbr of Columns, Nbr of Rows, Size (GB)

### 2. Configuration Parameters (Currently Missing)

#### A. Number of Partitions

**Option 1: From DMVA metadata (after checksum generation)**
```sql
SELECT 
    o.object_name,
    count(distinct c.extract_group_id) as partition_count
FROM dmva_object_info o
JOIN dmva_checksums c ON o.object_id = c.object_id
WHERE o.object_name = 'YOUR_TABLE'
GROUP BY o.object_name;
```

**Option 2: From DMVA tasks**
```sql
SELECT 
    count(distinct extract_group_id) as partition_count
FROM dmva_tasks
WHERE task_type = 'unload_partition'
  AND request_payload:object_name = 'YOUR_TABLE';
```

#### B. Aggregation Settings

```sql
SELECT 
    o.object_name,
    coalesce(o.overrides:distinct_counts::boolean, 
             s.overrides:distinct_counts::boolean, 
             d.distinct_counts::boolean) as distinct_counts,
    coalesce(o.overrides:null_counts::boolean, 
             s.overrides:null_counts::boolean, 
             d.null_counts::boolean) as null_counts,
    coalesce(o.overrides:min_values::boolean, 
             s.overrides:min_values::boolean, 
             d.min_values::boolean) as min_values,
    coalesce(o.overrides:max_values::boolean, 
             s.overrides:max_values::boolean, 
             d.max_values::boolean) as max_values,
    coalesce(o.overrides:sum_values::boolean, 
             s.overrides:sum_values::boolean, 
             d.sum_values::boolean) as sum_values,
    coalesce(o.overrides:max_select_columns::int, 
             s.overrides:max_select_columns::int, 
             d.max_select_columns::int) as max_select_columns
FROM dmva_object_info o
JOIN dmva_systems s ON o.system_name = s.system_name
CROSS JOIN (
    SELECT 
        max(case when param_name = 'distinct_counts' then param_value::boolean end) as distinct_counts,
        max(case when param_name = 'null_counts' then param_value::boolean end) as null_counts,
        max(case when param_name = 'min_values' then param_value::boolean end) as min_values,
        max(case when param_name = 'max_values' then param_value::boolean end) as max_values,
        max(case when param_name = 'sum_values' then param_value::boolean end) as sum_values,
        max(case when param_name = 'max_select_columns' then param_value::int end) as max_select_columns
    FROM dmva_defaults
) d
WHERE o.object_name = 'YOUR_TABLE';
```

#### C. Warehouse Size

Check which warehouse was used for loading:

```sql
SELECT 
    distinct warehouse_name
FROM dmva_tasks
WHERE task_type = 'load_partition'
  AND request_payload:object_name = 'YOUR_TABLE';
```

Or check warehouse size:
```sql
SHOW WAREHOUSES LIKE 'YOUR_WAREHOUSE_NAME';
-- Look at the SIZE column
```

#### D. Number of Measure Queries Generated

```sql
SELECT 
    count(*) as measure_query_count
FROM dmva_tasks
WHERE task_type = 'measure_partition'
  AND request_payload:object_name = 'YOUR_TABLE'
  AND status_cd = 'OK';
```

### 3. Timing Data

Get actual execution times by phase:

```sql
WITH task_times AS (
    SELECT 
        object_name,
        task_type,
        sum(datediff('second', start_ts, finish_ts)) / 60.0 as duration_mins
    FROM (
        SELECT 
            request_payload:object_name::varchar as object_name,
            task_type,
            start_ts,
            finish_ts
        FROM dmva_tasks
        WHERE request_payload:object_name = 'YOUR_TABLE'
          AND status_cd = 'OK'
    )
    GROUP BY object_name, task_type
)
SELECT 
    object_name,
    round(max(case when task_type = 'measure_partition' then duration_mins end), 1) as td_checksum_measure_mins,
    round(max(case when task_type = 'unload_partition' then duration_mins end), 1) as td_unload_mins,
    round(max(case when task_type = 'load_partition' then duration_mins end), 1) as sf_load_mins,
    -- Store checksum is embedded in measure_partition on target side
    round(sum(case when task_type in ('measure_partition', 'get_checksums') then duration_mins end), 1) as total_validation_mins
FROM task_times
GROUP BY object_name;
```

## Complete Data Collection Script

Run this for each table:

```sql
-- Complete parameter collection for table migration
WITH table_config AS (
    SELECT 
        o.database_name,
        o.object_name,
        o.est_num_records as row_count,
        round(o.est_table_size_mb / 1024, 2) as size_gb,
        count(distinct c.column_id) as column_count,
        count(distinct p.extract_group_id) as partition_count,
        coalesce(o.overrides:distinct_counts::boolean, 
                 s.overrides:distinct_counts::boolean, 
                 true) as distinct_counts,
        coalesce(o.overrides:null_counts::boolean, 
                 s.overrides:null_counts::boolean, 
                 true) as null_counts,
        coalesce(o.overrides:min_values::boolean, 
                 s.overrides:min_values::boolean, 
                 true) as min_values,
        coalesce(o.overrides:max_values::boolean, 
                 s.overrides:max_values::boolean, 
                 true) as max_values,
        coalesce(o.overrides:sum_values::boolean, 
                 s.overrides:sum_values::boolean, 
                 true) as sum_values,
        coalesce(o.overrides:max_select_columns::int, 
                 s.overrides:max_select_columns::int, 
                 1000) as max_select_columns
    FROM dmva_object_info o
    JOIN dmva_systems s ON o.system_name = s.system_name
    LEFT JOIN dmva_column_info c ON o.object_id = c.object_id AND c.active
    LEFT JOIN dmva_checksums p ON o.object_id = p.object_id
    WHERE o.object_name = 'YOUR_TABLE'  -- CHANGE THIS
    GROUP BY 
        o.database_name, o.object_name, o.est_num_records, 
        o.est_table_size_mb, o.overrides, s.overrides
),
task_times AS (
    SELECT 
        request_payload:object_name::varchar as object_name,
        task_type,
        sum(datediff('second', start_ts, finish_ts)) / 60.0 as duration_mins,
        min(start_ts) as first_start,
        max(finish_ts) as last_finish
    FROM dmva_tasks
    WHERE request_payload:object_name = 'YOUR_TABLE'  -- CHANGE THIS
      AND status_cd = 'OK'
    GROUP BY request_payload:object_name::varchar, task_type
),
measure_query_count AS (
    SELECT count(*) as num_queries
    FROM dmva_tasks
    WHERE task_type = 'measure_partition'
      AND request_payload:object_name = 'YOUR_TABLE'  -- CHANGE THIS
      AND status_cd = 'OK'
)
SELECT 
    tc.database_name as "DB Name",
    tc.object_name as "Table Name",
    'Full' as "Type of Run",
    tc.column_count as "Nbr of Columns",
    tc.row_count as "Nbr of Rows",
    tc.size_gb as "Size on Source (in GB)",
    tc.partition_count as "Nbr of Partitions",
    tc.distinct_counts::varchar as "distinct_counts",
    tc.null_counts::varchar as "null_counts",
    tc.min_values::varchar as "min_values",
    tc.max_values::varchar as "max_values",
    tc.sum_values::varchar as "sum_values",
    tc.max_select_columns as "max_select_columns",
    'S' as "Snowflake WH Size",  -- UPDATE THIS
    mqc.num_queries as "Nbr of Measure Queries Generated",
    round(max(case when tt.task_type = 'measure_partition' then tt.duration_mins end), 0) as "TD - Calculating Partition Checksums and Validation Measures",
    round(max(case when tt.task_type = 'unload_partition' then tt.duration_mins end), 0) as "TD - Unloading",
    round(max(case when tt.task_type = 'load_partition' then tt.duration_mins end), 0) as "SF - Loading",
    round(max(case when tt.task_type = 'get_checksums' then tt.duration_mins end), 0) as "SF - Storing Partition Checksums and Validations",
    round(datediff('second', min(tt.first_start), max(tt.last_finish)) / 60.0, 0) as "Total Duration",
    '' as "Notes"
FROM table_config tc
CROSS JOIN task_times tt
CROSS JOIN measure_query_count mqc
GROUP BY 
    tc.database_name, tc.object_name, tc.column_count, tc.row_count, tc.size_gb,
    tc.partition_count, tc.distinct_counts, tc.null_counts, tc.min_values,
    tc.max_values, tc.sum_values, tc.max_select_columns, mqc.num_queries;
```

## Quick Checklist

Before adding a table to the CSV, verify you have:

- [x] Table size in GB
- [x] Row count
- [x] Column count
- [ ] **Number of partitions** (from dmva_checksums)
- [ ] **distinct_counts** (true/false)
- [ ] **null_counts** (true/false)
- [ ] **min_values** (true/false)
- [ ] **max_values** (true/false)
- [ ] **sum_values** (true/false)
- [ ] **max_select_columns** (numeric)
- [ ] **Snowflake warehouse size** (XS/S/M/L/XL)
- [ ] **Number of measure queries generated** (numeric)
- [x] TD checksum time (mins)
- [x] TD unload time (mins)
- [x] SF load time (mins)
- [x] SF store checksum time (mins)
- [x] Total time (mins)

## Why This Matters

**Example: ACCT_BASE Mystery**

Current data shows:
- 135 columns
- Only 2 minutes for checksum/measure
- Only 1 measure query??? (probably wrong)

If ACCT_BASE used default settings with all 5 aggregations enabled:
- measure_items_per_column = 6
- columns_per_query = 1000 / 6 = 166
- num_queries = ceil(135 / 166) = 1
- **aggregates_per_query = 135 × 5 = 675** ❌❌❌

This would **massively exceed** Teradata's limit of 77!

So either:
1. ACCT_BASE had aggregations disabled (need to confirm)
2. ACCT_BASE had a different max_select_columns setting
3. The timing data is incorrect
4. It actually failed and was re-run with different settings

**This is why we need the actual configuration parameters!**

## Target: 15-20 Fully Documented Tables

To build an accurate model, aim to collect data for:
- 5 small tables (< 50 GB)
- 5 medium tables (50-200 GB)
- 5 large tables (> 200 GB)
- Mix of narrow (< 50 cols) and wide tables (> 100 cols)
- Mix of different aggregation configurations

This will enable proper multi-variable regression and confidence intervals.
