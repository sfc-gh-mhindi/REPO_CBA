# DMVA Performance Calculator - Limitations and Recommendations

## Current Status: Insufficient Data for Accurate Calibration

### The Problem

With only **3 data points** and **10+ variables** to estimate, we cannot build an accurately calibrated model. The current approach is **over-parameterized** and produces unrealistic results (29.1% accuracy).

## What's Missing from the Data

To build an accurate calculator, we need these additional columns in `DMVA Exec Results.csv`:

### Critical Missing Information

| Column | Why It's Needed | Impact on Accuracy |
|--------|----------------|-------------------|
| **Number of Partitions** | Affects parallelism in unload/load phases | HIGH |
| **distinct_counts (true/false)** | Directly impacts checksum time & aggregate count | CRITICAL |
| **null_counts (true/false)** | Affects aggregate count | CRITICAL |
| **min_values (true/false)** | Affects aggregate count | CRITICAL |
| **max_values (true/false)** | Affects aggregate count | CRITICAL |
| **sum_values (true/false)** | Affects aggregate count | CRITICAL |
| **max_select_columns** | Determines query splitting | CRITICAL |
| **Snowflake Warehouse Size** | Affects load time significantly | HIGH |
| **Number of Measure Queries Generated** | Validates our calculations | MEDIUM |

### Current Assumptions (Probably Wrong)

| Table | Our Guess | Reality Check |
|-------|-----------|---------------|
| DERV_ACCT_PATY | All 5 aggregations enabled | ❓ Unknown - could be different |
| DERV_ACCT_PATY | 10 partitions | ❓ Unknown - could be 1 or 100 |
| ACCT_BASE | All 5 aggregations enabled | ❓ Unknown - This has 135 columns, likely has issues! |
| ACCT_BASE | 10 partitions | ❓ Unknown |
| PLAN_BALN_SEGM_MSTR | 5 partitions | ❓ Unknown |

## The Statistics Problem

### Why We Can't Build an Accurate Model Yet

```
Number of unknown parameters: 10+
Number of data points: 3
Result: Severely underdetermined system
```

**Statistical Requirements:**
- Minimum: Need 10-20 data points to fit 10 parameters
- Ideal: Need 30-50 data points for reliable estimates
- Current: Have 3 data points (30% of minimum requirement)

### Attempted Model

```python
# What we're trying to estimate:
checksum_time = f(base, per_query, per_column, num_queries, num_aggregations, columns)
unload_time = f(base, per_gb, size, partitions, parallelism)
load_time = f(base, per_gb, size, warehouse_size)
store_time = f(base, per_partition, partitions)
total_time = f(checksum, unload, load, store, pipeline_efficiency)

# Total unknowns: 10 coefficients
# Total equations: 3 tables × 5 phases = 15 equations
# But many are interdependent, effectively < 10 independent constraints
```

## Two Paths Forward

### Option 1: Collect More Data (Recommended)

**Expand the CSV to include:**

```csv
DB Name,Table Name,Type of Run,Nbr of Columns,Nbr of Rows,Size on Source (in GB),
Nbr of Partitions,distinct_counts,null_counts,min_values,max_values,sum_values,
max_select_columns,Snowflake WH Size,Nbr of Measure Queries Generated,
TD - Calculating Partition Checksums and Validation Measures,TD - Unloading,
SF - Loading,SF - Storing Partition Checksums and Validations,Total Duration,Notes
```

**Minimum needed:**
- 10-15 more table migrations with full parameter tracking
- Mix of table sizes (small, medium, large)
- Mix of column counts (narrow, medium, wide)
- Different aggregation combinations

**This would enable:**
- Multi-variable regression analysis
- Confidence intervals on predictions
- Identification of which factors matter most
- Detection of non-linear relationships

### Option 2: Simplified Rule-Based Model (Pragmatic)

Since we can't do proper calibration, create simple rules based on the data we have:

```python
class SimpleRuleBasedEstimator:
    """
    Simple estimator based on observable patterns in the 3 data points.
    Not statistically rigorous, but pragmatic given limited data.
    """
    
    def estimate_checksum_time(self, columns, num_aggregations, max_select_columns):
        """
        Observed pattern:
        - PLAN_BALN_SEGM_MSTR: 29 cols, 1 agg → 20 mins
        - ACCT_BASE: 135 cols, 5 aggs → 2 mins  ← Anomaly?
        - DERV_ACCT_PATY: 11 cols, 5 aggs → 5 mins
        
        Hypothesis: Time depends heavily on whether queries hit aggregate limit
        """
        measure_items = 1 + num_aggregations
        columns_per_query = max(int(max_select_columns / measure_items), 1)
        num_queries = int(np.ceil(columns / columns_per_query))
        aggregates_per_query = min(columns, columns_per_query) * num_aggregations
        
        # Base time per query
        if aggregates_per_query > 64:
            # Complex queries hitting limits
            time_per_query = 10.0  # High cost
        elif aggregates_per_query > 30:
            # Moderate complexity
            time_per_query = 3.0
        else:
            # Simple queries
            time_per_query = 1.5
        
        return num_queries * time_per_query + 1.0  # Base overhead
    
    def estimate_unload_time(self, size_gb, estimated_partitions):
        """
        Observed:
        - 119 GB → 2 mins (DERV_ACCT_PATY)
        - 116 GB → 1 min (ACCT_BASE)  ← Very fast
        - 43 GB → 6 mins (PLAN_BALN_SEGM_MSTR)  ← Very slow
        
        Wide variance suggests other factors at play (Teradata load, partitioning)
        """
        # Conservative: 20 GB/min base rate
        base_mins = size_gb / 20.0
        # Adjust for parallelism
        if estimated_partitions > 1:
            base_mins = base_mins / np.sqrt(min(estimated_partitions, 4))
        return max(base_mins, 0.5)  # Minimum 30 seconds
    
    def estimate_load_time(self, size_gb, warehouse_size='S'):
        """
        Observed:
        - 119 GB → 2 mins (DERV_ACCT_PATY)
        - 116 GB → 2 mins (ACCT_BASE)
        - 43 GB → 10 mins (PLAN_BALN_SEGM_MSTR)  ← Anomaly?
        
        PLAN pattern suggests possibly smaller warehouse or other constraints
        """
        rates = {
            'XS': 10,   # GB/min
            'S': 30,    # GB/min
            'M': 60,
            'L': 120,
            'XL': 240
        }
        return size_gb / rates.get(warehouse_size, 30) + 0.5
```

## Recommendations for Improved Data Collection

### For Next Migrations

**Before running DMVA migration, capture:**

1. **Configuration Query:**
```sql
SELECT 
    o.object_name,
    o.est_table_size_mb / 1024 as size_gb,
    o.est_num_records as row_count,
    count(c.column_id) as column_count,
    count(distinct p.extract_group_id) as partition_count,
    coalesce(o.overrides:max_select_columns::int, 
             s.overrides:max_select_columns::int, 1000) as max_select_columns,
    coalesce(o.overrides:distinct_counts::boolean, 
             s.overrides:distinct_counts::boolean, true) as distinct_counts,
    coalesce(o.overrides:null_counts::boolean, 
             s.overrides:null_counts::boolean, true) as null_counts,
    coalesce(o.overrides:min_values::boolean, 
             s.overrides:min_values::boolean, true) as min_values,
    coalesce(o.overrides:max_values::boolean, 
             s.overrides:max_values::boolean, true) as max_values,
    coalesce(o.overrides:sum_values::boolean, 
             s.overrides:sum_values::boolean, true) as sum_values
FROM dmva_object_info o
JOIN dmva_systems s ON o.system_name = s.system_name
LEFT JOIN dmva_column_info c ON o.object_id = c.object_id
LEFT JOIN dmva_checksums p ON o.object_id = p.object_id
WHERE o.object_name = 'YOUR_TABLE'
GROUP BY o.object_name, o.est_table_size_mb, o.est_num_records, 
         o.overrides, s.overrides;
```

2. **During Migration:**
- Note the Snowflake warehouse size used
- Record start/end timestamps for each phase from dmva_tasks

3. **After Migration:**
- Count actual measure queries generated:
```sql
SELECT count(*)
FROM dmva_tasks
WHERE task_type = 'measure_partition'
  AND request_payload:object_name = 'YOUR_TABLE';
```

### Enhanced CSV Template

```csv
DB_Name,Table_Name,Size_GB,Rows,Columns,Partitions,
distinct_counts,null_counts,min_values,max_values,sum_values,
max_select_columns,Warehouse_Size,Measure_Queries_Generated,
TD_Checksum_Mins,TD_Unload_Mins,SF_Load_Mins,SF_Store_Mins,Total_Mins,
Notes
```

## Current Calculator Status

### What It CAN Do (Reliably)

✅ **Detect Teradata aggregate limit violations**
- Calculates aggregates per query based on DMVA logic
- Warns if exceeds 77 (will fail) or 64 (risky)
- This is based on code logic, not statistics - highly reliable

✅ **Estimate relative complexity**
- Can compare tables (A will take longer than B)
- Can identify which tables need configuration changes

✅ **Configuration recommendations**
- Recommends specific `max_select_columns` values
- Based on Teradata limits, not regression

### What It CANNOT Do (Reliably)

❌ **Accurate absolute time predictions**
- 29-91% accuracy depending on assumptions
- Too much variance in the limited data
- Missing critical parameters

❌ **Phase-specific breakdowns**
- Can't accurately predict TD vs SF bottlenecks
- Unload/load times vary 3-10x in our data

❌ **Generalize to new tables**
- Coefficients are unstable with 3 data points
- High risk of extrapolation errors

## Conclusion

**The calculator is currently most valuable for:**
1. ✅ Detecting configuration issues (aggregate limits)
2. ✅ Recommending fixes (`max_select_columns` settings)
3. ✅ Relative complexity comparison

**To make it valuable for time estimation:**
- Need 10-15 more fully-documented table migrations
- OR accept simple rule-based estimates with wide error bars

**Recommendation:** Use the calculator primarily as a **configuration validator** rather than a **time estimator** until more calibration data is available.
