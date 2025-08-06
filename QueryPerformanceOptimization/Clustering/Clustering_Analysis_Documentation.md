# Clustering Effectiveness Analysis - Documentation

This document explains each SQL statement in the `Clustering Effectiveness.sql` file and what insights they provide for optimizing table clustering in Snowflake.

## Overview
The script analyzes clustering effectiveness for three large tables:
- `FDP_CUSTOMER_TRANSACTIONS_SEND_FLAT` (594GB, 6000+ columns)
- `FDP_DIGT_NMON_TRAN_SBST` 
- `FDP_DIGITAL_MONETARY_TRANSACTIONS_SEND_FLAT` (290GB, 6800+ columns)

---

## SECTION 1: CHECK EXISTING CLUSTERING KEYS

### Statement 1: Check Clustering Keys via INFORMATION_SCHEMA
```sql
SELECT 'FDP_CUSTOMER_TRANSACTIONS_SEND_FLAT' AS TABLE_NAME,
       CLUSTERING_KEY, IS_PRIMARY_KEY, IS_UNIQUE_KEY
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
WHERE TABLE_SCHEMA = 'LCL' 
  AND TABLE_NAME = 'FDP_CUSTOMER_TRANSACTIONS_SEND_FLAT'
  AND CONSTRAINT_TYPE = 'CLUSTERING';
```
**Purpose:** Identifies existing clustering keys defined as table constraints  
**Returns:** Current clustering key configuration, if any exists  
**Why Important:** Determines if tables are already clustered before making changes

### Statement 2: Alternative Method Using SHOW Command
```sql
SHOW TABLES LIKE 'FDP_CUSTOMER_TRANSACTIONS_SEND_FLAT' IN SCHEMA LCL;
```
**Purpose:** Alternative way to see table metadata including clustering information  
**Returns:** Comprehensive table details including clustering keys in the output  
**Why Important:** SHOW commands sometimes reveal clustering info not visible in INFORMATION_SCHEMA

---

## SECTION 2: DETAILED TABLE INFORMATION INCLUDING CLUSTERING

### Statement 3: Comprehensive Table Metadata
```sql
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE,
       CLUSTERING_KEY, ROW_COUNT, BYTES, RETENTION_TIME,
       CREATED, LAST_ALTERED, AUTO_CLUSTERING_ON, COMMENT
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'LCL' 
  AND TABLE_NAME IN ('FDP_CUSTOMER_TRANSACTIONS_SEND_FLAT', ...);
```
**Purpose:** Gets complete table information including size, clustering status, and auto-clustering settings  
**Returns:** Table size (BYTES), row counts, clustering keys, auto-clustering status  
**Why Important:** Provides baseline metrics to measure improvement and understand current state

---

## SECTION 3: CLUSTERING EFFECTIVENESS ANALYSIS

### Statement 4: Account Usage Clustering Metrics
```sql
SELECT TABLE_NAME, CLUSTERING_KEY, TOTAL_PARTITION_COUNT,
       TOTAL_CONSTANT_PARTITION_COUNT, AVERAGE_OVERLAPS,
       AVERAGE_DEPTH, PARTITION_DEPTH_HISTOGRAM
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES 
WHERE TABLE_SCHEMA = 'LCL' AND TABLE_NAME IN (...);
```
**Purpose:** Retrieves detailed clustering effectiveness metrics from Snowflake's account usage views  
**Returns:** 
- `TOTAL_PARTITION_COUNT`: Number of micro-partitions
- `AVERAGE_OVERLAPS`: How much data overlaps between partitions (lower = better)
- `AVERAGE_DEPTH`: Clustering quality metric (closer to 1.0 = better)
- `PARTITION_DEPTH_HISTOGRAM`: Distribution of clustering depth

**Why Important:** These metrics quantify clustering effectiveness and identify poorly clustered tables

---

## SECTION 4: CLUSTERING DEPTH AND EFFECTIVENESS METRICS

### Statement 5: System Function Analysis
```sql
SELECT 'FDP_CUSTOMER_TRANSACTIONS_SEND_FLAT' AS TABLE_NAME,
       SYSTEM$CLUSTERING_DEPTH('LCL.FDP_CUSTOMER_TRANSACTIONS_SEND_FLAT') AS CLUSTERING_DEPTH,
       SYSTEM$CLUSTERING_INFORMATION('LCL.FDP_CUSTOMER_TRANSACTIONS_SEND_FLAT') AS CLUSTERING_INFO;
```
**Purpose:** Uses Snowflake system functions to get real-time clustering metrics  
**Returns:** 
- `CLUSTERING_DEPTH`: Current clustering quality (target: < 2.0)
- `CLUSTERING_INFORMATION`: Detailed JSON with clustering statistics

**Why Important:** Provides current clustering effectiveness without waiting for account usage updates

---

## SECTION 5: ANALYZE QUERY PERFORMANCE IMPACT

### Statement 6: Partition Pruning Test
```sql
EXPLAIN 
SELECT COUNT(*) 
FROM LCL.FDP_CUSTOMER_TRANSACTIONS_SEND_FLAT 
WHERE TTS_TRAN_DATE_ALT >= TO_CHAR(CURRENT_DATE - 30, 'YYYYMMDD')
  AND CHL_ID_OB_USERID IN ('TEST_USER_1', 'TEST_USER_2');
```
**Purpose:** Tests how effectively Snowflake can prune micro-partitions with current clustering  
**Returns:** Query execution plan showing partitions scanned vs. total partitions  
**Why Important:** 
- Shows real-world impact of clustering on query performance
- Simulates the exact filtering pattern from the problematic query
- Good pruning: < 10% partitions scanned, Poor pruning: > 50% scanned

---

## SECTION 6: CLUSTERING RECOMMENDATIONS

### Statement 7: Recommended Clustering Commands (Comments)
```sql
ALTER TABLE LCL.FDP_CUSTOMER_TRANSACTIONS_SEND_FLAT 
CLUSTER BY (TTS_TRAN_DATE_ALT, CHL_ID_OB_USERID);
```
**Purpose:** Provides ready-to-execute commands for optimal clustering  
**Logic:** 
- **Primary key**: Date columns (most selective filter in queries)
- **Secondary key**: User ID columns (second most common filter)

**Why These Keys:** Based on query analysis showing 30-day date filtering and user-specific joins

### Statement 8: Auto-Clustering Enablement
```sql
ALTER TABLE LCL.FDP_CUSTOMER_TRANSACTIONS_SEND_FLAT RESUME RECLUSTER;
```
**Purpose:** Enables automatic clustering maintenance  
**Why Important:** Large tables with frequent updates need auto-clustering to maintain effectiveness

---

## SECTION 7: MONITORING CLUSTERING EFFECTIVENESS OVER TIME

### Statement 9: Ongoing Monitoring Query
```sql
SELECT TABLE_NAME, CLUSTERING_KEY, AVERAGE_DEPTH,
       AVERAGE_OVERLAPS, TOTAL_PARTITION_COUNT, NOTES
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES 
WHERE TABLE_SCHEMA = 'LCL' AND TABLE_NAME IN (...);
```
**Purpose:** Provides a monitoring query to track clustering effectiveness over time  
**Returns:** Historical clustering metrics to identify degradation  
**Why Important:** Clustering effectiveness can degrade as data grows; regular monitoring prevents performance regression

### Statement 10: Auto-Clustering Status Check
```sql
SELECT TABLE_NAME, CLUSTERING_KEY, AUTO_CLUSTERING_ON, LAST_ALTERED
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'LCL' AND TABLE_NAME IN (...);
```
**Purpose:** Verifies auto-clustering is enabled and functioning  
**Returns:** Current auto-clustering status and last maintenance date  
**Why Important:** Ensures clustering maintenance is active for large, frequently updated tables

---

## SECTION 8: CLUSTERING EFFECTIVENESS INTERPRETATION

### Key Metrics Interpretation:

| Metric | Good Value | Poor Value | Impact |
|--------|------------|------------|---------|
| **CLUSTERING_DEPTH** | < 2.0 | > 4.0 | Query performance |
| **AVERAGE_OVERLAPS** | < 10 | > 50 | Partition pruning efficiency |
| **Partitions Scanned** | < 10% | > 50% | I/O and compute costs |

### Decision Matrix:

| Clustering Depth | Average Overlaps | Action Required |
|------------------|------------------|-----------------|
| < 2.0 | < 10 | ✅ Good - Monitor only |
| 2.0-4.0 | 10-30 | ⚠️ Consider reclustering |
| > 4.0 | > 30 | 🔴 Immediate clustering needed |

---

## Execution Workflow

1. **Run Sections 1-4** to understand current state
2. **Analyze Section 5** results to quantify performance impact
3. **If clustering is poor** (depth > 2.0), implement Section 6 recommendations
4. **Enable monitoring** with Section 7 queries
5. **Re-run analysis** after changes to measure improvement

## Expected Outcomes

- **Before Clustering:** 594GB + 290GB scanned, 70-75% remote I/O
- **After Optimal Clustering:** 95%+ reduction in data scanned, < 10% I/O
- **Query Performance:** 70-90% execution time reduction
- **Cost Impact:** Significant reduction in compute and storage I/O costs

This analysis provides the foundation for determining whether clustering improvements should be prioritized alongside the query rewrite optimization strategy. 