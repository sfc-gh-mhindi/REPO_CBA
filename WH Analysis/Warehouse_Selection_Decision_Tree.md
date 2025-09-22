# Snowflake Warehouse Selection Decision Tree

## Visual Decision Tree for Warehouse Selection

### Mermaid Diagram Code
```mermaid
flowchart TD
    Start([Choose Warehouse]) --> Workload{What type of workload?}
    
    Workload --> SQL[SQL Query Analytics]
    Workload --> Snowpark[Python/Java Snowpark]
    Workload --> Metadata[Metadata Ops DDL/ALTER]
    Workload --> Mixed[Mixed Workload]
    
    %% Snowpark Path
    Snowpark --> MemNeeds{Memory intensive?}
    MemNeeds -->|Yes| LargeSOW[Large+ SOW Memory]
    MemNeeds -->|No| MediumSOW[Medium+ SOW Standard]
    
    %% Metadata Path
    Metadata --> XSmallStd[X-Small Standard]
    
    %% Mixed and SQL Paths
    Mixed --> DataVol1{Data Volume?}
    SQL --> DataVol2{How much data scanned?}
    
    %% Data Volume Decision Tree
    DataVol1 --> Vol1[< 1GB: X-Small]
    DataVol1 --> Vol2[1-20GB: Small/Medium]
    DataVol1 --> Vol3[20-100GB: Medium/Large]
    DataVol1 --> Vol4[> 100GB: Large+]
    
    DataVol2 --> Vol5[< 1GB: X-Small]
    DataVol2 --> Vol6[1-20GB: Small/Medium]
    DataVol2 --> Vol7[20-100GB: Medium/Large]
    DataVol2 --> Vol8[> 100GB: Large+]
    
    %% Memory Decision for each volume
    Vol1 --> Mem1{Memory intensive?}
    Vol2 --> Mem2{Memory intensive?}
    Vol3 --> Mem3{Memory intensive?}
    Vol4 --> Mem4{Memory intensive?}
    Vol5 --> Mem5{Memory intensive?}
    Vol6 --> Mem6{Memory intensive?}
    Vol7 --> Mem7{Memory intensive?}
    Vol8 --> Mem8{Memory intensive?}
    
    %% Final Recommendations
    Mem1 -->|No| XSmallStd1[X-Small Standard]
    Mem1 -->|Yes| XSmallHM1[X-Small High Memory]
    Mem2 -->|No| SmallStd[Small Standard]
    Mem2 -->|Yes| SmallHM[Small High Memory]
    Mem3 -->|No| MediumStd[Medium Standard]
    Mem3 -->|Yes| MediumHM[Medium High Memory]
    Mem4 -->|No| LargeStd[Large+ Standard]
    Mem4 -->|Yes| LargeHM[Large+ High Memory]
    
    Mem5 -->|No| XSmallStd2[X-Small Standard]
    Mem5 -->|Yes| XSmallHM2[X-Small High Memory]
    Mem6 -->|No| SmallStd2[Small Standard]
    Mem6 -->|Yes| SmallHM2[Small High Memory]
    Mem7 -->|No| MediumStd2[Medium Standard]
    Mem7 -->|Yes| MediumHM2[Medium High Memory]
    Mem8 -->|No| LargeStd2[Large+ Standard]
    Mem8 -->|Yes| LargeHM2[Large+ High Memory]
    
    %% Styling
    classDef criticalNode fill:#ff6b6b,stroke:#d63031,stroke-width:3px,color:#fff
    classDef optimizeNode fill:#ffeaa7,stroke:#fdcb6e,stroke-width:2px
    classDef goodNode fill:#55a3ff,stroke:#0984e3,stroke-width:2px
    classDef perfectNode fill:#00b894,stroke:#00a085,stroke-width:2px
    
    class LargeSOW,MediumSOW criticalNode
    class SmallStd,MediumStd optimizeNode
    class LargeStd goodNode
    class XSmallStd perfectNode
```

### Concurrency Decision Tree
```mermaid
flowchart TD
    Users{How many concurrent users?} --> Few[1-5 Users]
    Users --> Medium[5-15 Users]
    Users --> Many[15-25 Users]
    Users --> VeryMany[25+ Users]
    
    Few --> SingleCluster[Single Cluster<br/>No Auto-Scaling]
    Medium --> MultiCluster2[Multi-Cluster<br/>2-3 Max Clusters]
    Many --> MultiCluster3[Multi-Cluster<br/>3-5 Max Clusters]
    VeryMany --> MultiCluster5[Multi-Cluster<br/>5+ Max Clusters]
    
    classDef lowConcurrency fill:#00b894,stroke:#00a085,stroke-width:2px
    classDef medConcurrency fill:#ffeaa7,stroke:#fdcb6e,stroke-width:2px
    classDef highConcurrency fill:#ff6b6b,stroke:#d63031,stroke-width:2px
    
    class SingleCluster lowConcurrency
    class MultiCluster2,MultiCluster3 medConcurrency
    class MultiCluster5 highConcurrency
```

### FRAUMD Specific Recommendations
```mermaid
flowchart TD
    FRAUMD[FRAUMD Warehouses<br/>Current Analysis] --> WH1[LABMLFRD_003<br/>2X-Large SOW]
    FRAUMD --> WH2[FRAUMD_001<br/>X-Large Standard]
    FRAUMD --> WH3[LABMLFRD_002<br/>X-Large SOW]
    FRAUMD --> WH4[LABMLFRD_001<br/>X-Small Standard]
    
    WH1 --> Critical[🔴 CRITICAL ISSUE<br/>99% inappropriate workload]
    WH2 --> Optimize[✅ OPTIMIZE<br/>82% spare capacity]
    WH3 --> Good[✅ GOOD<br/>Well configured]
    WH4 --> Perfect[✅ PERFECT<br/>Optimal sizing]
    
    Critical --> Action1[Move 100K+ SELECT queries<br/>to FRAUMD_001<br/>Keep only Snowpark ops<br/>Downsize to Large SOW]
    Optimize --> Action2[Accept workload from 003<br/>Enable multi-cluster<br/>Monitor utilization]
    Good --> Action3[Extend auto-suspend<br/>to 120 seconds<br/>Minor optimization]
    Perfect --> Action4[No changes needed<br/>Continue monitoring]
    
    classDef criticalNode fill:#ff6b6b,stroke:#d63031,stroke-width:3px,color:#fff
    classDef optimizeNode fill:#ffeaa7,stroke:#fdcb6e,stroke-width:2px
    classDef goodNode fill:#55a3ff,stroke:#0984e3,stroke-width:2px
    classDef perfectNode fill:#00b894,stroke:#00a085,stroke-width:2px
    
    class WH1,Critical,Action1 criticalNode
    class WH2,Optimize,Action2 optimizeNode
    class WH3,Good,Action3 goodNode
    class WH4,Perfect,Action4 perfectNode
```

## Decision Tree as Pseudocode

```
function selectWarehouse(workloadType, dataVolume, memoryIntensive, concurrentUsers) {
    
    // Step 1: Determine warehouse type
    if (workloadType === "SNOWPARK") {
        warehouseType = "SNOWPARK_OPTIMIZED"
        if (memoryIntensive) {
            size = "LARGE_OR_ABOVE"
            memory = "SOW_MEMORY_16X"
        } else {
            size = "MEDIUM_OR_ABOVE"
            memory = "SOW_STANDARD"
        }
    } else if (workloadType === "METADATA") {
        warehouseType = "STANDARD"
        size = "XSMALL"
        memory = "STANDARD"
    } else {
        warehouseType = "STANDARD"
        
        // Step 2: Determine size based on data volume
        if (dataVolume < "1GB") {
            size = "XSMALL"
        } else if (dataVolume < "20GB") {
            size = "SMALL"
        } else if (dataVolume < "100GB") {
            size = "MEDIUM"
        } else {
            size = "LARGE_OR_ABOVE"
        }
        
        // Step 3: Determine memory type
        if (memoryIntensive) {
            memory = "HIGH_MEMORY"
        } else {
            memory = "STANDARD"
        }
    }
    
    // Step 4: Determine clustering
    if (concurrentUsers <= 5) {
        clusters = "SINGLE_CLUSTER"
    } else if (concurrentUsers <= 15) {
        clusters = "MULTI_CLUSTER_2_3"
    } else if (concurrentUsers <= 25) {
        clusters = "MULTI_CLUSTER_3_5"
    } else {
        clusters = "MULTI_CLUSTER_5_PLUS"
    }
    
    return {
        type: warehouseType,
        size: size,
        memory: memory,
        clustering: clusters
    }
}
```

## Quick Reference Matrix

| **Data Volume** | **Memory Intensive** | **Standard** | **High Memory** | **Snowpark** |
|-----------------|---------------------|--------------|-----------------|--------------|
| **< 1GB**       | No                  | X-Small      | X-Small HM      | Not recommended |
| **< 1GB**       | Yes                 | X-Small HM   | X-Small HM      | Not recommended |
| **1-20GB**      | No                  | Small        | Small HM        | Medium SOW   |
| **1-20GB**      | Yes                 | Small HM     | Small HM        | Medium SOW Memory |
| **20-100GB**    | No                  | Medium       | Medium HM       | Large SOW    |
| **20-100GB**    | Yes                 | Medium HM    | Medium HM       | Large SOW Memory |
| **> 100GB**     | No                  | Large+       | Large+ HM       | Large+ SOW   |
| **> 100GB**     | Yes                 | Large+ HM    | Large+ HM       | Large+ SOW Memory |

## Implementation Examples

### Example 1: Data Exploration (FRAUMD Use Case)
```
Input: 
- Workload: SQL Analytics
- Data Volume: < 1GB (60% of LABMLFRD_003 queries)
- Memory Intensive: No
- Concurrent Users: 5-10

Output: Small Standard Warehouse
Recommendation: Move to FRAUMD_001 or create Small dedicated warehouse
```

### Example 2: ML Training (Legitimate Snowpark)
```
Input:
- Workload: Snowpark (Python ML)
- Data Volume: > 50GB
- Memory Intensive: Yes
- Concurrent Users: 2-3

Output: Large SOW_MEMORY_16X
Recommendation: Right-size LABMLFRD_003 for this specific use case
```

### Example 3: ETL Processing (Batch Operations)
```
Input:
- Workload: Mixed (CTAS + INSERT)
- Data Volume: 10-50GB
- Memory Intensive: No
- Concurrent Users: 1-3

Output: Medium Standard, Single Cluster
Recommendation: Use LABMLFRD_002 or dedicated Medium warehouse
```

## Visual Tools for Creating Diagrams

1. **Mermaid**: Copy the mermaid code above into any mermaid renderer
2. **Draw.io**: Use the text-based decision tree as a template
3. **Lucidchart**: Professional diagramming with decision tree templates
4. **Confluence**: Native mermaid support for wiki documentation
5. **PowerPoint/Google Slides**: Manual creation using shapes and connectors

## Usage Instructions

1. **For Documentation**: Copy the mermaid code into your documentation platform
2. **For Presentations**: Use the visual ASCII tree or create diagrams from the pseudocode
3. **For Training**: Use the quick reference matrix for hands-on workshops
4. **For Implementation**: Use the pseudocode as a basis for automated warehouse selection logic 