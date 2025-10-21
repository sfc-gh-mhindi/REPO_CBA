# DMVA Delta/Incremental Loading for Iceberg Tables

**Data Migration Validation Accelerator (DMVA) - Delta Loading Guide**

---

## Overview

DMVA supports **incremental/delta loading patterns** from Teradata to Snowflake Iceberg tables through a sophisticated **checkpointing and partitioning mechanism**. This enables efficient synchronization of only changed data rather than full table reloads.

---

## Key Concepts

### 1. **Checksum-Based Checkpointing**

DMVA uses the `dmva_checksums` table as the **checkpoint registry** to track what data has been loaded:

```sql
-- Core checkpoint structure
CREATE TABLE dmva_checksums (
    checksum_id NUMBER NOT NULL,
    object_id NUMBER NOT NULL,              -- Links to source table
    extract_group_id VARCHAR NOT NULL,      -- Checkpoint identifier (partition)
    checksum_method VARIANT,                -- How data is partitioned
    source_filter VARCHAR,                  -- WHERE clause for source
    target_filter VARCHAR,                  -- WHERE clause for target
    partition_checksum VARCHAR,             -- Data signature
    partition_row_count NUMBER,             -- Rows in this partition
    created_ts TIMESTAMP_NTZ(9),           -- When checkpoint created
    updated_ts TIMESTAMP_NTZ(9)            -- When checkpoint updated
);
```

**Key Fields:**
- **`extract_group_id`**: Acts as the checkpoint - identifies which data partition has been loaded
- **`partition_checksum`**: Hash of the data in this partition - detects changes
- **`source_filter` / `target_filter`**: SQL WHERE clauses defining the partition boundaries

---

### 2. **Checksum Methods** (Partitioning Strategies)

DMVA offers **4 checksum methods** to partition data and enable incremental loads:

| Method | Use Case | Checkpoint Logic |
|--------|----------|------------------|
| **WHOLE TABLE** | Small tables, one-time full loads | No partitioning, entire table = 1 checkpoint |
| **by_integer** | Integer column (ID, sequence) | Partition by ranges: `ID >= i * modulus AND ID < (i+1) * modulus` |
| **by_date** | Date/timestamp column | Partition by time periods: `DATE >= '2024-01-01' AND DATE < '2024-02-01'` |
| **by_substr** | String column (account ID prefix) | Partition by string prefix: `SUBSTR(COLUMN, 1, 6) = 'ABC123'` |

---

## Delta Loading Workflow - Executive Overview

**For executive and high-level audiences**

DMVA's delta loading capability enables **smart, efficient data synchronization** from Teradata to Snowflake Iceberg tables by loading only what has changed, rather than reprocessing entire tables repeatedly. Think of it as an intelligent "sync" mechanism, similar to how cloud storage services sync only modified files rather than re-uploading everything.

### How It Works (High-Level)

**1. Initial Setup - Taking a Snapshot**
- DMVA divides your data into logical chunks (like chapters in a book)
- Each chunk gets a unique "fingerprint" (checksum) that identifies its contents
- These fingerprints are stored as checkpoints in a tracking registry

**2. Ongoing Synchronization - Smart Updates**
- On subsequent runs, DMVA checks only the chunks you're interested in (based on your lookback period)
- For each chunk, it compares the current fingerprint with the stored checkpoint
- **If fingerprints match**: Data hasn't changed → Skip (saves time and cost)
- **If fingerprints differ**: Data has changed → Reload that chunk only
- **If new chunks exist**: New data → Load these chunks

**3. Efficiency Gains**
- **Time Savings**: Process hours or days of data changes instead of months/years of historical data
- **Cost Reduction**: Use less compute power and storage bandwidth by moving only changed data
- **Flexibility**: Handle late-arriving data by reprocessing recent time periods with a lookback window

### Business Benefits

| Benefit | Impact |
|---------|--------|
| **Reduced Processing Time** | Daily updates complete in minutes instead of hours |
| **Lower Cloud Costs** | Pay only for processing changed data, not entire tables |
| **Near Real-Time Data** | More frequent updates possible due to reduced load times |
| **Automatic Change Detection** | System identifies changes without manual intervention |
| **Audit Trail** | Complete logging of what was loaded, when, and why |

### Real-World Example

**Scenario:** A retail company with 500 million historical transactions, growing by 5 million new transactions daily.

**Without Delta Loading (Full Reload):**
- Process all 500M rows daily
- 2-3 hours per run
- High compute costs

**With DMVA Delta Loading:**
- Process only changed partitions (typically 1-7 days of data)
- 5-15 minutes per run
- 90% cost reduction
- Can run multiple times per day if needed

### Key Capabilities

✅ **Incremental Loading**: Only process data from a specific point forward (e.g., "last 7 days")  
✅ **Change Detection**: Automatically identify which data partitions have changed  
✅ **Late-Arriving Data**: Detect and reload historical partitions if late data arrives  
✅ **Parallel Processing**: Multiple data chunks processed simultaneously for speed  
✅ **Full Monitoring**: Complete visibility into what's being loaded and its status

### When to Use Delta Loading

| Scenario | Recommendation |
|----------|----------------|
| Large tables (100M+ rows) with regular updates | **Strongly Recommended** |
| Daily/hourly incremental data feeds | **Ideal Use Case** |
| Historical tables with occasional backfills | **Beneficial** |
| Small tables (<10M rows) | Full reload may be simpler |
| One-time migrations | Full load sufficient |

---

## Delta Loading Workflow - Technical Details

**For technical and implementation teams**

The following sections provide detailed technical implementation guidance for data engineers and developers.

---

### Phase 1: Initial Full Load

```
┌──────────────────────────────────────────────────────────────┐
│ STEP 1: Discover Table Partitions                           │
├──────────────────────────────────────────────────────────────┤
│ Teradata:                                                    │
│   SELECT FLOOR(transaction_id / 1000000) AS extract_group_id│
│        , HASH_AGG(*) AS partition_checksum                   │
│        , COUNT(*) AS partition_row_count                     │
│   FROM source_table                                          │
│   GROUP BY extract_group_id                                  │
│                                                              │
│ Result: Discovers partitions 0, 1, 2, ..., N                │
└──────────────────────────────────────────────────────────────┘
              ↓
┌──────────────────────────────────────────────────────────────┐
│ STEP 2: Store Checkpoints                                   │
├──────────────────────────────────────────────────────────────┤
│ INSERT INTO dmva_checksums:                                  │
│   extract_group_id: '0'                                      │
│   source_filter: 'transaction_id >= 0 AND < 1000000'        │
│   target_filter: 'transaction_id >= 0 AND < 1000000'        │
│   partition_checksum: 'ABC123...'                            │
│   partition_row_count: 999500                                │
│                                                              │
│   extract_group_id: '1'                                      │
│   source_filter: 'transaction_id >= 1000000 AND < 2000000'  │
│   ...                                                        │
└──────────────────────────────────────────────────────────────┘
              ↓
┌──────────────────────────────────────────────────────────────┐
│ STEP 3: Extract & Load Each Partition                       │
├──────────────────────────────────────────────────────────────┤
│ For EACH partition:                                          │
│   1. Unload from Teradata (using source_filter)             │
│   2. Upload to Snowflake stage                               │
│   3. Load into Iceberg table (using target_filter)           │
│   4. Validate checksums match                                │
└──────────────────────────────────────────────────────────────┘
```

---

### Phase 2: Incremental/Delta Load

#### **With Lookback Parameter** (Recommended)

The **`lookback`** parameter enables incremental loading by defining a starting checkpoint:

**Example Configuration:**
```json
{
    "column_name": "transaction_date",
    "type": "by_date",
    "period": "month",
    "lookback": "20241201"
}
```

**Incremental Load Logic:**

```sql
-- DMVA generates extract_group_ids ONLY from the lookback point forward
WITH partitions AS (
    SELECT DATE_TRUNC('month', transaction_date) AS extract_group_id
         , HASH_AGG(*) AS partition_checksum
         , COUNT(*) AS partition_row_count
    FROM source_table
    WHERE transaction_date >= TO_DATE('20241201', 'YYYYMMDD')  -- ← LOOKBACK FILTER
    GROUP BY extract_group_id
)
SELECT TO_CHAR(extract_group_id, 'YYYYMMDD') AS extract_group_id
     , ...
FROM partitions
```

**Checkpoint Behavior:**
1. **New Partitions** (not in `dmva_checksums`): Loaded as NEW data
2. **Existing Partitions** (in `dmva_checksums`): 
   - Compare `partition_checksum` (hash)
   - If **checksums differ** → Data changed → RELOAD partition
   - If **checksums match** → Skip (already synchronized)

**Delta Load Workflow:**

```
┌──────────────────────────────────────────────────────────────┐
│ STEP 1: Query Teradata for Partitions Since Lookback        │
├──────────────────────────────────────────────────────────────┤
│ Teradata discovers partitions: Dec-2024, Jan-2025, Feb-2025 │
└──────────────────────────────────────────────────────────────┘
              ↓
┌──────────────────────────────────────────────────────────────┐
│ STEP 2: Compare with Existing Checkpoints                   │
├──────────────────────────────────────────────────────────────┤
│ Dec-2024: EXISTS in dmva_checksums                           │
│   Old checksum: 'ABC123'                                     │
│   New checksum: 'ABC123' → MATCH → SKIP                     │
│                                                              │
│ Jan-2025: EXISTS in dmva_checksums                           │
│   Old checksum: 'DEF456'                                     │
│   New checksum: 'DEF789' → CHANGED → RELOAD                 │
│                                                              │
│ Feb-2025: NOT EXISTS in dmva_checksums → NEW → LOAD         │
└──────────────────────────────────────────────────────────────┘
              ↓
┌──────────────────────────────────────────────────────────────┐
│ STEP 3: Process Only Changed/New Partitions                 │
├──────────────────────────────────────────────────────────────┤
│ Jan-2025: DELETE + INSERT (REPLACE pattern)                 │
│ Feb-2025: INSERT (APPEND pattern)                            │
└──────────────────────────────────────────────────────────────┘
```

---

## Load Patterns Supported

DMVA supports multiple load patterns through its **load process**:

### 1. **REPLACE (Truncate & Load)**

**Pattern:** Delete existing partition data, then insert new data

**Implementation:**
```python
# From system_snowflake.py
def get_delete_sql(self, object_identifier, target_filter):
    return f'delete from {object_identifier} where {target_filter}'

def get_copy_sql(self, object_identifier, ...):
    return f'copy into {object_identifier} from (...)'
```

**Execution Flow:**
```sql
-- Step 1: Delete old partition data
DELETE FROM iceberg_table 
WHERE transaction_date >= '2025-01-01' 
  AND transaction_date < '2025-02-01';

-- Step 2: Load new partition data
COPY INTO iceberg_table
FROM @stage/partition_0001/
...;
```

**When Used:**
- Data in source partition has changed (checksum mismatch)
- Ensures Iceberg table is **exactly synchronized** with source

---

### 2. **APPEND (Insert Only)**

**Pattern:** Insert new data without deleting existing data

**When Used:**
- New partition (extract_group_id not in dmva_checksums)
- Source data is append-only (no updates/deletes)

**Execution Flow:**
```sql
-- Only load step (no delete)
COPY INTO iceberg_table
FROM @stage/partition_new/
...;
```

---

### 3. **MERGE/SCD2** (NOT Directly Supported)

**Current State:** DMVA does **not natively support MERGE** or **SCD Type 2** patterns.

**Workaround Options:**

#### **Option A: Post-Load MERGE (Recommended)**

Use DMVA for data extraction, then apply custom MERGE logic:

```sql
-- DMVA loads into staging table
-- Your custom procedure:
MERGE INTO target_iceberg_table t
USING staging_table s
ON t.primary_key = s.primary_key
WHEN MATCHED AND t.checksum != s.checksum THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

#### **Option B: External Tool**

Use dbt or custom SQL for SCD2 transformations after DMVA load.

---

## Logging & Monitoring

DMVA provides **comprehensive logging** at multiple levels:

### 1. **Task-Level Logging**

**Table: `dmva_tasks` / `dmva_ingested_all`**

Tracks every task (checksum, unload, upload, load, measure):

```sql
SELECT 
    task_id,
    task_type,              -- 'checksum_partition', 'load_partition', etc.
    status_cd,              -- 'OK', 'ERROR', 'RUNNING'
    start_ts,
    finish_ts,
    DATEDIFF('second', start_ts, finish_ts) AS duration_sec,
    source_object_id,
    target_object_id,
    extract_group_id,       -- Which partition was processed
    result_payload          -- Detailed results (rows loaded, errors, etc.)
FROM dmva_tasks
WHERE task_type = 'load_partition'
ORDER BY start_ts DESC;
```

**Example Output:**
```
task_id | task_type       | status_cd | duration | extract_group_id | result_payload
--------|-----------------|-----------|----------|------------------|----------------
12345   | load_partition  | OK        | 45s      | 202501          | {"rows": 1.2M}
12346   | load_partition  | OK        | 38s      | 202502          | {"rows": 1.1M}
12347   | load_partition  | ERROR     | 12s      | 202503          | {"error": "..."}
```

---

### 2. **Status Change Log**

**Table: `dmva_status_change_log`**

Tracks status transitions for auditability:

```sql
SELECT 
    object_id,
    extract_group_id,
    old_status,
    new_status,
    changed_ts,
    changed_by
FROM dmva_status_change_log
WHERE extract_group_id = '202501'
ORDER BY changed_ts;
```

---

### 3. **Checkpoint History**

**Table: `dmva_checksums`**

Every checkpoint update is logged with timestamps:

```sql
SELECT 
    extract_group_id,
    partition_checksum,
    partition_row_count,
    created_ts,             -- When first loaded
    updated_ts              -- When last synchronized
FROM dmva_checksums
WHERE object_id = (SELECT object_id FROM dmva_object_info WHERE object_name = 'MY_TABLE')
ORDER BY extract_group_id;
```

**Identify Changed Partitions:**
```sql
-- Find partitions that were reloaded (updated_ts != created_ts)
SELECT 
    extract_group_id,
    partition_row_count,
    created_ts,
    updated_ts,
    DATEDIFF('day', created_ts, updated_ts) AS days_since_first_load
FROM dmva_checksums
WHERE updated_ts > created_ts
ORDER BY updated_ts DESC;
```

---

### 4. **Monitoring Views**

DMVA provides pre-built views for monitoring:

```sql
-- Overall migration status
SELECT * FROM dmva_status_results_total;

-- Per-table status
SELECT * FROM dmva_status_results_detail
WHERE source_schema_name = 'TERADATA_DB'
  AND source_object_name = 'MY_TABLE';

-- Active tasks
SELECT * FROM dmva_tasks_active;

-- Task statistics
SELECT * FROM dmva_task_stats_by_date;
```

---

## Complete Delta Load Example

### Scenario: Daily Incremental Load from Teradata to Snowflake Iceberg

**Source Table:**
- `teradata_db.transactions` (500M rows, growing by ~5M/day)
- Date column: `transaction_date`

**Target Table:**
- `snowflake_db.iceberg_schema.transactions` (Iceberg table)

---

### Step 1: Configure Checksum Method

```sql
-- Set incremental checksum method with lookback
UPDATE dmva_object_info
SET checksum_method = PARSE_JSON('{
    "column_name": "transaction_date",
    "type": "by_date",
    "period": "day",
    "lookback": "20250101"
}')
WHERE system_name = 'teradata_source'
  AND schema_name = 'teradata_db'
  AND object_name = 'transactions';
```

**Explanation:**
- **`type: by_date`**: Partition by date ranges
- **`period: day`**: Each partition = 1 day of data
- **`lookback: 20250101`**: Only process data from Jan 1, 2025 forward

---

### Step 2: Initial Load (Day 1)

```sql
-- Execute checksum/load tasks
CALL dmva_get_checksum_tasks('teradata_source', PARSE_JSON('{"teradata_db": ["transactions"]}'));
```

**What Happens:**
1. **Teradata** discovers partitions: `20250101`, `20250102`, ..., `20250108` (8 days)
2. Each partition becomes a checkpoint in `dmva_checksums`
3. Data for each day is unloaded, uploaded, and loaded into Iceberg table
4. Checksums stored for validation

**Result:**
```sql
SELECT extract_group_id, partition_row_count, created_ts
FROM dmva_checksums
WHERE object_id = (SELECT object_id FROM dmva_object_info WHERE object_name = 'transactions')
ORDER BY extract_group_id;

-- Output:
-- 20250101 | 5,100,000 | 2025-01-08 10:00:00
-- 20250102 | 5,050,000 | 2025-01-08 10:12:00
-- 20250103 | 5,200,000 | 2025-01-08 10:24:00
-- ...
-- 20250108 | 5,150,000 | 2025-01-08 11:30:00
```

---

### Step 3: Delta Load (Day 2 - Next Day)

```sql
-- Update lookback to yesterday (or keep same lookback for a sliding window)
UPDATE dmva_object_info
SET checksum_method = PARSE_JSON('{
    "column_name": "transaction_date",
    "type": "by_date",
    "period": "day",
    "lookback": "20250108"
}')
WHERE object_name = 'transactions';

-- Execute delta load
CALL dmva_get_checksum_tasks('teradata_source', PARSE_JSON('{"teradata_db": ["transactions"]}'));
```

**What Happens:**

1. **Teradata** discovers partitions from `lookback` forward: `20250108`, `20250109` (today)

2. **Compare Checksums:**
   ```
   Partition 20250108:
     - Exists in dmva_checksums: YES
     - Old checksum: 'ABC123'
     - New checksum: 'ABC456' (late-arriving data added!)
     - Action: RELOAD (DELETE + INSERT)
   
   Partition 20250109:
     - Exists in dmva_checksums: NO
     - Action: LOAD (INSERT only)
   ```

3. **Execution:**
   ```sql
   -- Partition 20250108: DELETE + INSERT (REPLACE pattern)
   DELETE FROM iceberg_schema.transactions
   WHERE transaction_date >= '2025-01-08' AND transaction_date < '2025-01-09';
   
   COPY INTO iceberg_schema.transactions FROM @stage/20250108/...;
   
   -- Partition 20250109: INSERT (APPEND pattern)
   COPY INTO iceberg_schema.transactions FROM @stage/20250109/...;
   ```

4. **Update Checkpoints:**
   ```sql
   -- Partition 20250108: Update updated_ts
   UPDATE dmva_checksums
   SET partition_checksum = 'ABC456',
       partition_row_count = 5,155,000,  -- Now includes late data
       updated_ts = CURRENT_TIMESTAMP()
   WHERE extract_group_id = '20250108';
   
   -- Partition 20250109: Insert new checkpoint
   INSERT INTO dmva_checksums (extract_group_id, partition_checksum, ...)
   VALUES ('20250109', 'DEF789', ...);
   ```

---

### Step 4: Monitor Delta Load

```sql
-- Check which partitions were processed
SELECT 
    t.extract_group_id,
    t.task_type,
    t.status_cd,
    t.start_ts,
    t.finish_ts,
    t.result_payload:rows_affected::NUMBER AS rows_loaded
FROM dmva_tasks t
WHERE t.source_object_id = (SELECT object_id FROM dmva_object_info WHERE object_name = 'transactions')
  AND t.start_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY t.start_ts DESC;

-- Output:
-- 20250109 | load_partition | OK | ... | 5,120,000 rows
-- 20250108 | load_partition | OK | ... | 5,155,000 rows (reloaded)
```

---

## Best Practices

### 1. **Choose the Right Checksum Method**

| Data Pattern | Recommended Method |
|--------------|-------------------|
| Append-only with timestamps | `by_date` (day/month) |
| Sequential IDs (auto-increment) | `by_integer` (modulus = 1M-10M) |
| Partition key in source | Match source partitioning |
| Small tables (<10M rows) | `WHOLE TABLE` |

---

### 2. **Set Appropriate Lookback Windows**

**Sliding Window (Recommended):**
```json
{
    "lookback": "LAST_7_DAYS"
}
```
- Reprocesses last 7 days to catch late-arriving data
- Balances data freshness vs. processing cost

**Fixed Checkpoint:**
```json
{
    "lookback": "20250101"
}
```
- Loads all data from a specific date forward
- Use for initial loads or backfills

---

### 3. **Partition Size Guidelines**

Target: **1M-10M rows per partition**

**Too Large:**
- Slow processing
- Memory issues
- Difficult to parallelize

**Too Small:**
- Too many tasks
- Overhead dominates
- Poor performance

**Adjust modulus/period:**
```json
// If partitions too large (>100M rows):
{ "modulus": 100000 }  // Smaller modulus = more partitions

// If partitions too small (<100K rows):
{ "modulus": 10000000 }  // Larger modulus = fewer partitions
```

---

### 4. **Monitor & Alert**

```sql
-- Create alert for failed loads
CREATE ALERT delta_load_failures
WAREHOUSE = dmva_wh
SCHEDULE = '1 HOUR'
IF (EXISTS (
    SELECT 1 
    FROM dmva_tasks
    WHERE status_cd = 'ERROR'
      AND task_type = 'load_partition'
      AND start_ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
THEN
    CALL send_notification_sp('Delta load failures detected!');
```

---

## Limitations & Workarounds

### ❌ **NOT Supported**

1. **Native MERGE/UPSERT**
   - Workaround: Use REPLACE pattern + post-load MERGE

2. **SCD Type 2 (Slowly Changing Dimensions)**
   - Workaround: Load to staging, apply SCD2 logic separately

3. **Change Data Capture (CDC)**
   - Workaround: Use checksum method to detect changes at partition level

4. **Cross-Partition Updates**
   - Limitation: DMVA works at partition granularity
   - Workaround: Set partition size to match update patterns

---

### ✅ **Supported**

1. ✅ **REPLACE** (DELETE + INSERT per partition)
2. ✅ **APPEND** (INSERT only for new partitions)
3. ✅ **Late-Arriving Data** (via lookback and checksum comparison)
4. ✅ **Incremental Loads** (via checkpointing)
5. ✅ **Parallel Processing** (multiple partitions loaded simultaneously)
6. ✅ **Data Validation** (checksum verification)
7. ✅ **Comprehensive Logging** (task tracking, status changes, audit trail)

---

## Summary

DMVA provides **robust delta/incremental loading** capabilities through:

1. **Checkpoint Mechanism**: `dmva_checksums` table tracks loaded partitions
2. **Partitioning Strategies**: 4 checksum methods for different data patterns
3. **Lookback Support**: Enables incremental loads from a specific point forward
4. **Change Detection**: Checksum comparison identifies changed partitions
5. **Load Patterns**: REPLACE (update) and APPEND (insert) patterns
6. **Comprehensive Logging**: Multi-level tracking for monitoring and troubleshooting

**For Teradata → Snowflake Iceberg migrations:**
- ✅ Efficient delta loads without full table scans
- ✅ Automatic detection of changed data
- ✅ Parallel processing for performance
- ✅ Full audit trail of all loads
- ⚠️ Post-processing required for MERGE/SCD2 patterns

---

**Next Steps:**
- Configure appropriate `checksum_method` for your tables
- Set `lookback` parameter for incremental loads
- Schedule periodic delta loads (daily/hourly)
- Monitor via `dmva_tasks` and status views
- Apply custom MERGE logic if needed for SCD patterns


