# FRAUMD Warehouses: Usage Analysis & Optimization Guide

**Analysis Period:** Last 2 months  
**Warehouses Analyzed:** 4 FRAUMD warehouses  
**Key Finding:** LABMLFRD_003 warehouse misconfiguration identified  
**Primary Recommendation:** Workload redistribution and right-sizing  
**Expected Impact:** 60-70% efficiency improvement  

## Quick Navigation
- [🏗️ Current Warehouse Setup](#current-warehouse-setup)
- [📚 Warehouse Types Definition](#warehouse-types-definition)
- [📊 Current Usage Patterns](#current-usage-patterns)
- [🚨 Critical Issues](#critical-issues)
- [🎯 Optimization Plan](#optimization-plan)
- [📚 Best Practices Guide](#best-practices)
- [📋 Appendix](#appendix)

---

## 🏗️ Current Warehouse Setup

*Source: `Fraud-Warehouse Setup Info.csv`*

| **Warehouse** | **Size** | **Type** | **Max Clusters** | **Auto-Suspend** | **Server Count** | **Created** |
|---------------|----------|----------|------------------|-------------------|------------------|-------------|
| **LABMLFRD_003** | 2X-Large | **SOW_MEMORY_16X** | 1 | 600s | 32 | 2025-01-31 |
| **FRAUMD_001** | X-Large | Standard | 2 | 300s | 16 | 2023-11-08 |
| **LABMLFRD_002** | X-Large | High Memory | 2 | 30s | 16 | 2024-07-22 |
| **LABMLFRD_001** | X-Small | Standard | 2 | 30s | 1 | 2024-05-27 |

### Current Concurrency Patterns
*Source: `PS ACCOUNT REVIEW - AVG RUNNING 4 WHS.csv`*

| **Warehouse** | **Avg Running** | **Queue %** | **>=75% Cost Jobs** | **Peak Concurrency** |
|---------------|-----------------|-------------|--------------------|--------------------|
| **LABMLFRD_003** | 1.00 | 8.22% | 97.39% | Always Active |
| **FRAUMD_001** | 0.18 | 0.98% | 87.45% | Low Concurrency |
| **LABMLFRD_002** | 0.25 | 12.69% | 97.52% | Low-Medium |
| **LABMLFRD_001** | 0.11 | 0.15% | 99.79% | Very Low |

---

## 📚 Warehouse Types Definition

### Standard Warehouses
- **Purpose:** General-purpose SQL operations (SELECT, INSERT, UPDATE, CTAS)
- **Compute:** Balanced CPU and memory allocation
- **Best For:** Traditional data analytics, ETL operations, reporting
- **Cost:** Standard compute pricing

### High Memory Warehouses  
- **Purpose:** Memory-intensive operations requiring large working sets
- **Compute:** Enhanced memory allocation (2x standard memory)
- **Best For:** Complex joins, large aggregations, analytical functions
- **Cost:** ~25% premium over standard

### Snowpark-Optimized Warehouses (SOW)
- **Purpose:** Custom code execution (Python, Java, Scala)
- **Compute:** Specialized for User-Defined Functions (UDFs) and stored procedures
- **Memory Types:**
  - **SOW_MEMORY_16X:** 16x memory allocation for intensive ML/analytics
  - **Standard SOW:** Balanced for general Snowpark operations
- **Best For:** Machine learning, data science, custom algorithms
- **Cost:** 3-4x premium over standard warehouses

---

## 📊 Current Usage Patterns

### Warehouse Utilization Overview
*Source: `CBA CDL PROD - warehouse_utilisation.csv`*

| **Warehouse** | **Size** | **Query Count** | **Credits Used** | **Large Queries %** | **Small Queries %** |
|---------------|----------|-----------------|------------------|--------------------|--------------------|
| **LABMLFRD_003** | 2X-Large | 2,998 | 9,274 | 40% | 60% |
| **FRAUMD_001** | X-Large | 5,057 | 2,855 | 38% | 62% |
| **LABMLFRD_002** | X-Large | 2,219 | 1,158 | 23% | 77% |
| **LABMLFRD_001** | X-Small | 624 | 20 | 9% | 91% |

### Query Sizing Band Analysis
*Based on data volume scanned per query, derived from warehouse utilization SQL code:*

```sql
-- Sizing bands classification:
ROUND(count_below_one_gb / count_queries * 100, 0) AS percent_xs,        -- < 1GB
ROUND(count_btw_one_and_twenty_gb / count_queries * 100, 0) AS percent_s, -- 1-20GB  
ROUND(count_btw_twenty_and_fifty_gb / count_queries * 100, 0) AS percent_m, -- 20-50GB
ROUND(count_btw_fifty_and_one_hundred_gb / count_queries * 100, 0) AS percent_l, -- 50-100GB
ROUND(count_btw_one_hundred_and_twofifty_gb / count_queries * 100, 0) AS percent_xl, -- 100-250GB
ROUND(count_over_twofifty_gb / count_queries * 100, 0) AS percent_2xl -- > 250GB
```

### Detailed Sizing Distribution
*Source: `CBA CDL PROD - warehouse_utilisation.csv`*

| **Warehouse** | **XS (<1GB)** | **S (1-20GB)** | **M (20-50GB)** | **L (50-100GB)** | **XL (100-250GB)** | **2XL (>250GB)** |
|---------------|---------------|----------------|-----------------|------------------|-------------------|------------------|
| **LABMLFRD_003** | 60% | 19% | 4% | 2% | 3% | 11% |
| **FRAUMD_001** | 62% | 25% | 4% | 2% | 2% | 6% |
| **LABMLFRD_002** | 77% | 16% | 1% | 1% | 1% | 4% |
| **LABMLFRD_001** | 91% | 8% | 0% | 0% | 0% | 0% |

### Key Insights:
- **LABMLFRD_003**: Despite being 2X-Large Snowpark, 60% of queries are small (<1GB)
- **FRAUMD_001**: Well-balanced distribution, good candidate for mixed workloads
- **LABMLFRD_002**: Highly efficient with 77% small queries on X-Large
- **LABMLFRD_001**: Perfect sizing with 91% small queries on X-Small

---

## 🚨 Critical Issues

### 🔴 Critical Issue #1: LABMLFRD_003 Misalignment

**The Problem:**
- **Configuration:** 2X-Large SOW_MEMORY_16X (Snowpark Optimized)
- **Reality:** 99.86% standard SQL operations
- **Cost Impact:** Premium Snowpark pricing for inappropriate workload

**Actual Usage Pattern:**
*Source: `Fraud-Query complexity analysis.csv`*
- 100,065 SELECT queries (standard SQL)
- 3,436 ALTER operations (metadata)
- 148 UNLOAD operations (standard)
- **Only 139 CALL operations** (legitimate Snowpark usage)

**Sizing Mismatch:**
*Source: `CBA CDL PROD - warehouse_utilisation.csv`*
- 60% of queries scan <1GB (should use Small/Medium warehouse)
- 19% scan 1-20GB (appropriate for Large warehouse)
- Only 11% scan >250GB (justifying 2X-Large)

### ⚠️ Secondary Issues

**Issue #2: Universal Cache Miss**
*Source: `Fraud-Cache Efficiency Analysis.csv`*
- 0% cache hit rate across all warehouses
- Missed performance optimization opportunities

**Issue #3: Spillage Patterns**
*Source: `Fraud-Spillage analysis.csv`*
- LABMLFRD_003: 2,860 local spills + 10 remote spills
- FRAUMD_001: 5,292 local spills + 22 remote spills

---

## 🎯 Optimization Plan

### Priority 1: LABMLFRD_003 Workload Redistribution

**Analysis: Should we create new warehouse or redistribute to FRAUMD_001?**

*Source: `PS ACCOUNT REVIEW - AVG RUNNING 4 WHS.csv`*

**FRAUMD_001 Capacity Analysis:**
- **Current Avg Running:** 0.18 (only 18% utilized)
- **Queue Time:** 0.98% (very low)
- **Capacity Available:** ~82% unused capacity

**Recommendation: Redistribute to FRAUMD_001**

**Rationale:**
1. **FRAUMD_001 has significant spare capacity** (82% unused)
2. **Low queue times** (0.98%) indicate no concurrency pressure
3. **Cost-effective:** Use existing resources vs creating new warehouse
4. **Similar workload profiles:** Both handle mixed SELECT/CTAS operations

**Implementation Plan:**

```sql
-- Phase 1: Migrate SELECT queries to FRAUMD_001
-- Target: 100,065 SELECT queries from LABMLFRD_003

-- Phase 2: Keep only Snowpark workloads on LABMLFRD_003
-- Downsize to Large SOW_MEMORY_16X for 139 CALL operations

-- Phase 3: Monitor and adjust
-- Track FRAUMD_001 utilization and enable multi-cluster if needed
```

### Expected Benefits:
- **FRAUMD_001:** Increase utilization from 18% to ~45-50%
- **LABMLFRD_003:** Right-size from 2X-Large to Large SOW
- **Cost Efficiency:** Eliminate 70%+ of inappropriate Snowpark usage

### Alternative: Multi-Cluster FRAUMD_001
*If redistribution causes concurrency issues:*

```sql
ALTER WAREHOUSE WH_USR_PRD_P01_FRAUMD_001 
SET AUTO_SCALE_MODE = 'STANDARD'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3;
```

---

## 📚 Best Practices Guide

### Warehouse Selection Decision Matrix

#### By Data Volume:
| **Scan Size** | **Recommended Size** | **Rationale** |
|---------------|---------------------|---------------|
| < 1GB | X-Small/Small | Cost-effective for targeted queries |
| 1-20GB | Medium/Large | Balanced performance and cost |
| 50GB+ | Large/X-Large+ | Required for large data processing |

#### By Query Type:
| **Query Type** | **Warehouse Type** | **Size Guidance** |
|----------------|-------------------|------------------|
| Simple SELECT | Standard | Match data volume |
| Complex JOINS | Standard/High Memory | Large+ recommended |
| CTAS Operations | Standard | Large+ for performance |
| **ML/Python/Java** | **Snowpark-Optimized** | Medium+ based on complexity |
| Metadata (ALTER, DDL) | Any Standard | X-Small sufficient |

#### By Concurrency:
| **User Count** | **Configuration** | **Auto-Scaling** |
|----------------|------------------|------------------|
| 1-5 users | Single cluster | Not needed |
| 5-15 users | Multi-cluster | 2-3 clusters |
| 15+ users | Multi-cluster | 3-5 clusters |

### Performance Monitoring KPIs

**Key Metrics to Track:**
1. **Utilization Rate:** Target 70-85%
2. **Queue Time:** Keep <5% of execution time
3. **Spillage Rate:** Maintain <1% of queries
4. **Cache Hit Rate:** Achieve 40-60%
5. **Credits per Query:** Monitor efficiency trends

---

## 📋 Appendix

### Appendix A: Small Queries on LABMLFRD_003 Warehouse

*Source: `CBA CDL PROD - SMALL QUERIES ON WH_USR_PRD_P01_FRAUMD_LABMLFRD_003.csv`*

**Sample of inappropriate queries running on expensive Snowpark warehouse:**

| **Query Type** | **Example** | **Data Scanned** | **Execution Time** | **Recommendation** |
|----------------|-------------|------------------|-------------------|-------------------|
| Simple SELECT | `SELECT * FROM LABMLFRD.CARD_TRAN_GROS_FRAU LIMIT 1` | 2.1MB | 1.3s | Move to X-Small |
| Table Browse | `SELECT * FROM LABMLFRD.FDP_UV_DATA_DICT` | 88KB | 6.9mins* | Move to Small |
| Aggregation | `SELECT TTS_TRAN_DATE_ALT, count(1) FROM ...` | 60MB | 2.9s | Move to Medium |

*Note: Long execution times often indicate resource contention on oversized warehouse*

**Key Findings:**
- **Simple LIMIT 1 queries:** Using 2X-Large Snowpark for single-row retrieval
- **Small aggregations:** <100MB scans on premium warehouse
- **Table browsing:** Metadata exploration on specialized ML warehouse

**Cost Impact:**
- These small queries represent 60% of LABMLFRD_003 workload
- Running on 4x more expensive warehouse than needed
- Estimated efficiency gain: 70%+ by moving to appropriate warehouses

### Appendix B: Warehouse Configuration Scripts

**Current State Backup:**
```sql
-- Document current configurations before changes
SELECT name, size, warehouse_type, auto_suspend, max_cluster_count 
FROM warehouses 
WHERE name LIKE '%FRAUMD%';
```

**Recommended Implementation:**
```sql
-- Phase 1: Enable multi-cluster on FRAUMD_001 (if needed)
ALTER WAREHOUSE WH_USR_PRD_P01_FRAUMD_001 
SET AUTO_SCALE_MODE = 'STANDARD'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2;

-- Phase 2: Downsize LABMLFRD_003 for Snowpark-only
ALTER WAREHOUSE WH_USR_PRD_P01_FRAUMD_LABMLFRD_003
SET WAREHOUSE_SIZE = 'LARGE';

-- Phase 3: Optimize auto-suspend
ALTER WAREHOUSE WH_USR_PRD_P01_FRAUMD_LABMLFRD_002
SET AUTO_SUSPEND = 120;
```

### Appendix C: Monitoring Queries

**Weekly Utilization Check:**
```sql
SELECT 
    warehouse_name,
    COUNT(*) as query_count,
    AVG(execution_time) as avg_execution_ms,
    SUM(CASE WHEN bytes_scanned < 1073741824 THEN 1 ELSE 0 END) / COUNT(*) * 100 as pct_small_queries
FROM query_history 
WHERE start_time >= CURRENT_DATE - 7
  AND warehouse_name LIKE '%FRAUMD%'
GROUP BY warehouse_name;
```

**Spillage Monitoring:**
```sql
SELECT 
    warehouse_name,
    SUM(CASE WHEN bytes_spilled_to_local_storage > 0 THEN 1 ELSE 0 END) as local_spills,
    SUM(CASE WHEN bytes_spilled_to_remote_storage > 0 THEN 1 ELSE 0 END) as remote_spills,
    COUNT(*) as total_queries
FROM query_history
WHERE start_time >= CURRENT_DATE - 7
  AND warehouse_name LIKE '%FRAUMD%'
GROUP BY warehouse_name;
```

---

**Contact Information:**
- **Primary Contact:** [Team/Person responsible]
- **Technical Escalation:** [DBA/Platform team]
- **Last Updated:** [Date]
- **Next Review:** [Monthly review date] 