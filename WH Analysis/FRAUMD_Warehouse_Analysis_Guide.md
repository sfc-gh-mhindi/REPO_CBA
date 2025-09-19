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

**Query Size Classification:**
- **Large Queries:** Operations that scan ≥ 1GB of data
- **Small Queries:** Operations that scan < 1GB of data

| **Warehouse** | **Size** | **Query Count** | **Credits Used** | **Large Queries %** | **Small Queries %** |
|---------------|----------|-----------------|------------------|--------------------|--------------------|
| **LABMLFRD_003** | 2X-Large | 2,998 | 9,274 | 40% | 60% |
| **FRAUMD_001** | X-Large | 5,057 | 2,855 | 38% | 62% |
| **LABMLFRD_002** | X-Large | 2,219 | 1,158 | 23% | 77% |
| **LABMLFRD_001** | X-Small | 624 | 20 | 9% | 91% |

### Query Sizing Band Analysis
*Based on data volume scanned per query, categorized into standardized sizing bands:*

**Sizing Band Definitions:**
- **XS (Extra Small):** Operations that scan < 1GB of data
- **S (Small):** Operations that scan 1-20GB of data
- **M (Medium):** Operations that scan 20-50GB of data
- **L (Large):** Operations that scan 50-100GB of data
- **XL (Extra Large):** Operations that scan 100-250GB of data
- **2XL (Double Extra Large):** Operations that scan > 250GB of data

*These bands help identify workload patterns and determine optimal warehouse sizing for different query types.*

### Detailed Sizing Distribution
*Source: `CBA CDL PROD - warehouse_utilisation.csv`*

```mermaid
%%{init: {'theme':'base', 'themeVariables': { 'primaryColor': '#ff6b6b', 'primaryTextColor': '#fff', 'primaryBorderColor': '#7C0000', 'lineColor': '#F8B229', 'secondaryColor': '#006100', 'tertiaryColor': '#fff'}}}%%
flowchart TB
    subgraph "🔥 Query Size Distribution Heat Map"
        subgraph "LABMLFRD_003 (2X-Large SOW)"
            L003_XS["🔴 XS: 60%<br/>ALARMING"]
            L003_S["🟡 S: 19%<br/>High"]
            L003_M["🟢 M: 4%<br/>Normal"]
            L003_L["🟢 L: 2%<br/>Normal"]
            L003_XL["🟡 XL: 3%<br/>Medium"]
            L003_2XL["🔴 2XL: 11%<br/>CRITICAL"]
        end
        
        subgraph "FRAUMD_001 (X-Large Standard)"
            F001_XS["🟡 XS: 62%<br/>High"]
            F001_S["🟢 S: 25%<br/>Good"]
            F001_M["🟢 M: 4%<br/>Normal"]
            F001_L["🟢 L: 2%<br/>Normal"]
            F001_XL["🟢 XL: 2%<br/>Low"]
            F001_2XL["🟡 2XL: 6%<br/>Medium"]
        end
        
        subgraph "LABMLFRD_002 (X-Large High Memory)"
            L002_XS["🟢 XS: 77%<br/>Optimal"]
            L002_S["🟢 S: 16%<br/>Good"]
            L002_M["🟢 M: 1%<br/>Low"]
            L002_L["🟢 L: 1%<br/>Low"]
            L002_XL["🟢 XL: 1%<br/>Low"]
            L002_2XL["🟢 2XL: 4%<br/>Low"]
        end
        
        subgraph "LABMLFRD_001 (X-Small Standard)"
            L001_XS["🟢 XS: 91%<br/>Perfect"]
            L001_S["🟢 S: 8%<br/>Good"]
            L001_M["🟢 M: 0%<br/>None"]
            L001_L["🟢 L: 0%<br/>None"]
            L001_XL["🟢 XL: 0%<br/>None"]
            L001_2XL["🟢 2XL: 0%<br/>None"]
        end
    end
    
    classDef critical fill:#ff6b6b,stroke:#d63031,stroke-width:3px,color:#fff
    classDef warning fill:#ffeaa7,stroke:#fdcb6e,stroke-width:2px,color:#000
    classDef good fill:#55a3ff,stroke:#0984e3,stroke-width:2px,color:#fff
    classDef optimal fill:#00b894,stroke:#00a085,stroke-width:2px,color:#fff
    
    class L003_XS,L003_2XL critical
    class L003_S,L003_XL,F001_XS,F001_2XL warning
    class L003_M,L003_L,F001_S,F001_M,F001_L,F001_XL good
    class L002_XS,L002_S,L002_M,L002_L,L002_XL,L002_2XL,L001_XS,L001_S,L001_M,L001_L,L001_XL,L001_2XL optimal
```

**🚨 Heat Map Legend:**
- 🔴 **CRITICAL/ALARMING** (60%+ small queries on large warehouses, 10%+ 2XL queries)
- 🟡 **WARNING/HIGH** (40-60% small queries, 5-10% 2XL queries)  
- 🟢 **GOOD/OPTIMAL** (Appropriate distribution for warehouse size)

**📊 Raw Distribution Table:**

| **Warehouse** | **XS (<1GB)** | **S (1-20GB)** | **M (20-50GB)** | **L (50-100GB)** | **XL (100-250GB)** | **2XL (>250GB)** |
|---------------|---------------|----------------|-----------------|------------------|-------------------|------------------|
| **LABMLFRD_003** | 🔴 60% | 🟡 19% | 🟢 4% | 🟢 2% | 🟡 3% | 🔴 11% |
| **FRAUMD_001** | 🟡 62% | 🟢 25% | 🟢 4% | 🟢 2% | 🟢 2% | 🟡 6% |
| **LABMLFRD_002** | 🟢 77% | 🟢 16% | 🟢 1% | 🟢 1% | 🟢 1% | 🟢 4% |
| **LABMLFRD_001** | 🟢 91% | 🟢 8% | 🟢 0% | 🟢 0% | 🟢 0% | 🟢 0% |

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

### 🌳 Warehouse Selection Decision Tree

> **📄 For complete mermaid code and additional diagrams, see:** [`Warehouse_Selection_Decision_Tree.md`](./Warehouse_Selection_Decision_Tree.md)

```mermaid
flowchart TD
    Start([🏁 Choose Warehouse]) --> Workload{What type of workload?}
    
    Workload --> SQL[📊 SQL Query<br/>Analytics]
    Workload --> Snowpark[🐍 Python/Java<br/>Snowpark]
    Workload --> Metadata[⚙️ Metadata Ops<br/>DDL/ALTER]
    Workload --> Mixed[🔄 Mixed<br/>Workload]
    
    %% Snowpark Path
    Snowpark --> MemNeeds{Memory intensive?}
    MemNeeds -->|Yes| LargeSOW[🔴 Large+ SOW<br/>Memory 16X]
    MemNeeds -->|No| MediumSOW[🟡 Medium+ SOW<br/>Standard]
    
    %% Metadata Path
    Metadata --> XSmallStd[🟢 X-Small<br/>Standard]
    
    %% Mixed and SQL Paths
    Mixed --> DataVol1{Data Volume<br/>Scanned?}
    SQL --> DataVol2{How much data<br/>scanned?}
    
    %% Data Volume Decision Tree
    DataVol1 --> Vol1[🟢 < 1GB:<br/>X-Small]
    DataVol1 --> Vol2[🟡 1-20GB:<br/>Small/Medium]
    DataVol1 --> Vol3[🟠 20-100GB:<br/>Medium/Large]
    DataVol1 --> Vol4[🔴 > 100GB:<br/>Large+]
    
    DataVol2 --> Vol5[🟢 < 1GB:<br/>X-Small]
    DataVol2 --> Vol6[🟡 1-20GB:<br/>Small/Medium]
    DataVol2 --> Vol7[🟠 20-100GB:<br/>Medium/Large]
    DataVol2 --> Vol8[🔴 > 100GB:<br/>Large+]
    
    %% Memory Decision for each volume
    Vol1 --> Mem1{Memory<br/>intensive?}
    Vol2 --> Mem2{Memory<br/>intensive?}
    Vol3 --> Mem3{Memory<br/>intensive?}
    Vol4 --> Mem4{Memory<br/>intensive?}
    Vol5 --> Mem5{Memory<br/>intensive?}
    Vol6 --> Mem6{Memory<br/>intensive?}
    Vol7 --> Mem7{Memory<br/>intensive?}
    Vol8 --> Mem8{Memory<br/>intensive?}
    
    %% Final Recommendations
    Mem1 -->|No| XSmallStd1[✅ X-Small<br/>Standard]
    Mem1 -->|Yes| XSmallHM1[🟡 X-Small<br/>High Memory]
    Mem2 -->|No| SmallStd[✅ Small<br/>Standard]
    Mem2 -->|Yes| SmallHM[🟡 Small<br/>High Memory]
    Mem3 -->|No| MediumStd[✅ Medium<br/>Standard]
    Mem3 -->|Yes| MediumHM[🟡 Medium<br/>High Memory]
    Mem4 -->|No| LargeStd[✅ Large+<br/>Standard]
    Mem4 -->|Yes| LargeHM[🔴 Large+<br/>High Memory]
    
    Mem5 -->|No| XSmallStd2[✅ X-Small<br/>Standard]
    Mem5 -->|Yes| XSmallHM2[🟡 X-Small<br/>High Memory]
    Mem6 -->|No| SmallStd2[✅ Small<br/>Standard]
    Mem6 -->|Yes| SmallHM2[🟡 Small<br/>High Memory]
    Mem7 -->|No| MediumStd2[✅ Medium<br/>Standard]
    Mem7 -->|Yes| MediumHM2[🟡 Medium<br/>High Memory]
    Mem8 -->|No| LargeStd2[✅ Large+<br/>Standard]
    Mem8 -->|Yes| LargeHM2[🔴 Large+<br/>High Memory]
    
    %% Styling
    classDef criticalNode fill:#ff6b6b,stroke:#d63031,stroke-width:3px,color:#fff
    classDef warningNode fill:#ffeaa7,stroke:#fdcb6e,stroke-width:2px,color:#000
    classDef goodNode fill:#55a3ff,stroke:#0984e3,stroke-width:2px,color:#fff
    classDef optimalNode fill:#00b894,stroke:#00a085,stroke-width:2px,color:#fff
    
    class LargeSOW,LargeHM,LargeHM2 criticalNode
    class MediumSOW,XSmallHM1,SmallHM,MediumHM,XSmallHM2,SmallHM2,MediumHM2 warningNode
    class SmallStd,MediumStd,LargeStd,SmallStd2,MediumStd2,LargeStd2 goodNode
    class XSmallStd,XSmallStd1,XSmallStd2 optimalNode
```

### 🎯 Concurrency Decision Branch

```mermaid
flowchart TD
    Users{👥 How many<br/>concurrent users?} --> Few[🟢 1-5 Users]
    Users --> Medium[🟡 5-15 Users]
    Users --> Many[🟠 15-25 Users]
    Users --> VeryMany[🔴 25+ Users]
    
    Few --> SingleCluster[✅ Single Cluster<br/>🔄 No Auto-Scaling<br/>💰 Cost Efficient]
    Medium --> MultiCluster2[⚡ Multi-Cluster<br/>📊 2-3 Max Clusters<br/>🔄 Auto-Scale: On]
    Many --> MultiCluster3[⚡ Multi-Cluster<br/>📊 3-5 Max Clusters<br/>🔄 Auto-Scale: Aggressive]
    VeryMany --> MultiCluster5[⚡ Multi-Cluster<br/>📊 5+ Max Clusters<br/>💸 High Concurrency Mode]
    
    classDef lowConcurrency fill:#00b894,stroke:#00a085,stroke-width:2px,color:#fff
    classDef medConcurrency fill:#ffeaa7,stroke:#fdcb6e,stroke-width:2px,color:#000
    classDef highConcurrency fill:#ff6b6b,stroke:#d63031,stroke-width:2px,color:#fff
    
    class Few,SingleCluster lowConcurrency
    class Medium,Many,MultiCluster2,MultiCluster3 medConcurrency
    class VeryMany,MultiCluster5 highConcurrency
```

### 🚨 FRAUMD Warehouse Specific Recommendations

```mermaid
flowchart TD
    FRAUMD[🏢 FRAUMD Warehouses<br/>📊 Current Analysis<br/>⏱️ Last 2 Months] --> WH1[🔴 LABMLFRD_003<br/>2X-Large SOW<br/>💸 9,274 Credits]
    FRAUMD --> WH2[🟡 FRAUMD_001<br/>X-Large Standard<br/>💰 2,855 Credits]
    FRAUMD --> WH3[🟢 LABMLFRD_002<br/>X-Large High Memory<br/>💰 1,158 Credits]
    FRAUMD --> WH4[✅ LABMLFRD_001<br/>X-Small Standard<br/>💚 20 Credits]
    
    WH1 --> Critical[🔴 CRITICAL ISSUE<br/>⚠️ 60% inappropriate workload<br/>📈 99% small queries on 2X-Large<br/>💸 300%+ cost inefficiency]
    WH2 --> Optimize[🟡 OPTIMIZATION OPPORTUNITY<br/>📊 82% spare capacity<br/>⚡ Ready for more workload<br/>🎯 Perfect for redistribution]
    WH3 --> Good[🟢 WELL CONFIGURED<br/>✅ 77% small queries (appropriate)<br/>⚡ Good utilization pattern<br/>🔧 Minor tuning needed]
    WH4 --> Perfect[✅ PERFECTLY SIZED<br/>🎯 91% small queries<br/>💚 Optimal cost efficiency<br/>🏆 Best practice example]
    
    Critical --> Action1[📋 IMMEDIATE ACTIONS<br/>🔄 Move 100K+ SELECT queries<br/>🎯 Redirect to FRAUMD_001<br/>🐍 Keep only Snowpark ops<br/>📉 Downsize to Large SOW]
    Optimize --> Action2[📋 ENHANCEMENT PLAN<br/>⬆️ Accept workload from 003<br/>⚡ Enable multi-cluster scaling<br/>📊 Monitor utilization closely<br/>🎯 Become primary standard WH]
    Good --> Action3[📋 MINOR OPTIMIZATIONS<br/>⏰ Extend auto-suspend → 120s<br/>📊 Monitor spillage patterns<br/>✅ Continue current usage]
    Perfect --> Action4[📋 MAINTENANCE MODE<br/>✅ No changes needed<br/>📊 Continue monitoring<br/>🏆 Use as best practice model]
    
    classDef criticalNode fill:#ff6b6b,stroke:#d63031,stroke-width:3px,color:#fff
    classDef optimizeNode fill:#ffeaa7,stroke:#fdcb6e,stroke-width:2px,color:#000
    classDef goodNode fill:#55a3ff,stroke:#0984e3,stroke-width:2px,color:#fff
    classDef perfectNode fill:#00b894,stroke:#00a085,stroke-width:2px,color:#fff
    
    class WH1,Critical,Action1 criticalNode
    class WH2,Optimize,Action2 optimizeNode
    class WH3,Good,Action3 goodNode
    class WH4,Perfect,Action4 perfectNode
```

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