# CSEL & CCODS DBT Projects - Snowflake Deployment Guide

## Overview

This repository contains two complementary data pipeline projects implemented using DBT (Data Build Tool) and deployed within Snowflake:

- **CSEL (Commonwealth Bank Service Layer)**: Processes customer service data, appointments, products, and department information through 18 sequential transformations
- **CCODS (Commonwealth Bank Operations Data System)**: Processes BCFINSG data and populates the PLAN_BALN_SEGM_MSTR table through 2 sequential model groups

Both projects share the same Snowflake infrastructure and deployment framework while serving different business domains.

---

## 🏗️ **CSEL Project**

### Project Structure

```
CSEL/
├── DBT Project/                        # Shared DBT project directory (same as CCODS)
│   ├── models/
│   │   ├── cse_dataload/               # Core data loading models
│   │   │   ├── 02processkey/           # Process key generation
│   │   │   ├── 04MappingLookupSets/    # Mapping and lookup tables
│   │   │   ├── 08extraction/           # Data extraction models
│   │   │   ├── 12MappingTransformation/ # Data transformations
│   │   │   ├── 14loadtotemp/           # Temporary table loading
│   │   │   ├── 16transformdelta/       # Delta transformations
│   │   │   ├── 18loadtogdw/           # Final GDW loading
│   │   │   └── 24processmetadata/      # Process metadata management
│   │   └── appt_pdct/                  # Appointment product models
│   ├── dbt_project.yml                # DBT project configuration
│   └── profiles.yml                   # Connection profiles
├── NPW DBT - Pre-Installation Scripts/ # Deployment scripts
│   ├── CSEL01-Pre-Installation Script.sql      # Database setup and table creation
│   ├── CSEL02-Installing NPW DBT - Control Data.sql # Control data population
│   ├── CSEL04-Execute_DBT_Procedure.sql # Main execution procedure
│   └── CSEL05-Task_Execute_DBT_CSEL.sql # Orchestration task
└── README.md                          # This file
```

### CSEL Execution Framework

**Procedure**: `NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CSEL()`  
**Task**: `NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CSEL`  
**Schedule**: Daily at 3:00 AM Australia/Sydney

#### CSEL Execution Flow (18 Steps)

> **Note**: If Mermaid diagrams don't render in your Git viewer, see alternative formats in the [`diagrams/`](diagrams/) folder:
> - [PlantUML version](diagrams/execution_flow.puml) 
> - [ASCII art version](diagrams/infrastructure_ascii.md)

```mermaid
flowchart LR
    START([Start CSEL]) --> PHASE1[Phase 1: Control Setup<br/>Steps 1-5<br/>🔧 Stream Status, ISAC Check<br/>Process Keys, Lookups]
    
    PHASE1 --> PHASE2[Phase 2: Data Processing<br/>Steps 6-12<br/>📥 Extract Apps, Transform<br/>Load Temp, Delta, Update/Insert]
    
    PHASE2 --> PHASE3[Phase 3: Product Processing<br/>Steps 13-16<br/>📦 Load Product Temp<br/>Delta, Update/Insert Products]
    
    PHASE3 --> PHASE4[Phase 4: Final Loading<br/>Steps 17-18<br/>🎯 Update/Insert<br/>Department Appointments]
    
    PHASE4 --> SUCCESS[✅ SUCCESS<br/>All 18 Steps Complete<br/>JSON Result]
    
    PHASE1 -.-> ERROR[❌ FAILED<br/>Log Error Details<br/>Return Failure]
    PHASE2 -.-> ERROR
    PHASE3 -.-> ERROR
    PHASE4 -.-> ERROR
    
    style START fill:#e1f5fe
    style PHASE1 fill:#f3e5f5
    style PHASE2 fill:#fff3e0
    style PHASE3 fill:#e8f5e8
    style PHASE4 fill:#fce4ec
    style SUCCESS fill:#c8e6c9
    style ERROR fill:#ffcdd2
```

---

## 🏗️ **CCODS Project**

### Project Structure

```
CCODS/
├── DBT Project/                       # Shared DBT project directory (same as CSEL)
│   ├── models/
│   │   ├── ccods/                      # CCODS-specific models
│   │   │   ├── 40_transform/           # Data transformation layer
│   │   │   │   └── xfmplanbalnsegmmstrfrombcfinsg/ # BCFINSG transformations
│   │   │   └── 60_load_gdw/           # GDW loading layer
│   │   │       └── ldbcfinsgplanbalnsegmmstr/      # Final table loading
│   │   ├── cse_dataload/              # Shared CSE data loading models
│   │   └── appt_pdct/                 # Shared appointment product models
│   ├── dbt_project.yml               # DBT project configuration
│   └── profiles.yml                  # Connection profiles
├── Pre-Installation Scripts/          # Deployment scripts
│   ├── CCODS02-Execute_DBT_CCODS_Procedure.sql # Main execution procedure
│   └── CCODS03-Task_Execute_DBT_CCODS.sql      # Orchestration task
├── Analysis & Debug Files/           # Troubleshooting resources
│   ├── Debug_Data_Flow_Tracing.sql  # Data lineage tracing
│   ├── Process_Key_Fix_Analysis.sql # Process key debugging
│   └── Corrected_DEPT_APPT_MERGE.sql # MERGE statement fixes
└── README.md                        # This file
```

### CCODS Execution Framework

**Procedure**: `NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CCODS()`  
**Task**: `NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CCODS`  
**Schedule**: Daily at 4:00 AM Australia/Sydney (1 hour after CSEL)

#### CCODS Execution Flow (2 Steps)

```mermaid
flowchart LR
    START([Start CCODS<br/>4:00 AM]) --> STEP1[Step 1: Transform<br/>🔄 BCFINSG Data<br/>40_transform models]
    
    STEP1 --> CHECK1{Success?}
    CHECK1 -->|✅ Yes| STEP2[Step 2: Load GDW<br/>📤 Final Loading<br/>PLAN_BALN_SEGM_MSTR]
    CHECK1 -->|❌ No| FAIL1[❌ Transform Failed<br/>Log Error & Stop]
    
    STEP2 --> CHECK2{Success?}
    CHECK2 -->|✅ Yes| SUCCESS[✅ CCODS Complete<br/>2 Steps Successful<br/>JSON Result]
    CHECK2 -->|❌ No| FAIL2[❌ Load Failed<br/>Log Error & Stop]
    
    style START fill:#e1f5fe
    style STEP1 fill:#f3e5f5
    style STEP2 fill:#e8f5e8
    style SUCCESS fill:#c8e6c9
    style FAIL1 fill:#ffcdd2
    style FAIL2 fill:#ffcdd2
    style CHECK1 fill:#fff3e0
    style CHECK2 fill:#fff3e0
```

#### CCODS Model Details

**Step 1: Data Transformation (40_transform)**
- **Path**: `models/ccods/40_transform/xfmplanbalnsegmmstrfrombcfinsg/`
- **Purpose**: Transform raw BCFINSG data into structured format
- **Output**: Prepared data for plan balance segment master processing
- **Key Models**: 
  - `bcfinsg__xfmplanbalnsegmmstrfrombcfinsg.sql` (root data extraction)
  - Data validation and cleansing transformations

**Step 2: GDW Loading (60_load_gdw)**
- **Path**: `models/ccods/60_load_gdw/ldbcfinsgplanbalnsegmmstr/`
- **Purpose**: Load transformed data into final GDW table
- **Target Table**: `PLAN_BALN_SEGM_MSTR`
- **Output**: Populated production table ready for consumption

---

## 🏛️ **Shared Infrastructure**

### Snowflake Database Structure

```mermaid
graph LR
    subgraph "⏰ Scheduling"
        CRON_CSEL[3:00 AM<br/>CSEL]
        CRON_CCODS[4:00 AM<br/>CCODS]
    end
    
    subgraph "🏗️ Execution Layer"
        TASK_CSEL[T_EXECUTE_DBT_CSEL<br/>📅 Daily Task]
        TASK_CCODS[T_EXECUTE_DBT_CCODS<br/>📅 Daily Task]
        PROC_CSEL[P_EXECUTE_DBT_CSEL<br/>🔄 18 Steps]
        PROC_CCODS[P_EXECUTE_DBT_CCODS<br/>🔄 2 Steps]
    end
    
    subgraph "📦 Shared Resources"
        DBT[GDW1_DBT<br/>Shared Project]
        WORKSPACE[DBT Workspace]
        WAREHOUSE[Compute Warehouse]
    end
    
    subgraph "📊 Outputs & Audit"
        LOG[DCF_T_EXEC_LOG<br/>Unified Audit]
        CSEL_OUT[📋 CSEL Models<br/>Appointments, Products]
        CCODS_OUT[📈 CCODS Models<br/>PLAN_BALN_SEGM_MSTR]
    end
    
    CRON_CSEL --> TASK_CSEL --> PROC_CSEL
    CRON_CCODS --> TASK_CCODS --> PROC_CCODS
    PROC_CSEL --> DBT
    PROC_CCODS --> DBT
    DBT --> WORKSPACE
    DBT --> WAREHOUSE
    PROC_CSEL --> LOG
    PROC_CCODS --> LOG
    DBT --> CSEL_OUT
    DBT --> CCODS_OUT
    
    style PROC_CSEL fill:#e1f5fe
    style PROC_CCODS fill:#f3e5f5
    style DBT fill:#fce4ec
    style LOG fill:#c8e6c9
```

### Deployment Configuration

- **Primary Database**: `NPD_D12_DMN_GDWMIG`
- **Schema**: `TMP`
- **Shared DBT Project**: `GDW1_DBT`
- **Models Database**: `NPD_D12_DMN_GDWMIG_IBRG_V`
- **Compute Warehouse**: `wh_usr_npd_d12_gdwmig_001`

---

## 📋 **Pre-Installation Scripts**

### CSEL Pre-Installation

1. **CSEL01-Pre-Installation Script.sql**
   - Creates required database objects and tables
   - Sets up views for APPT_DEPT, APPT_PDCT, and other core entities
   - Creates UTIL_PROS_ISAC and control tables
   - Initializes sample data structures for testing

2. **CSEL02-Installing NPW DBT - Control Data.sql**
   - Populates control tables with required reference data
   - Inserts stream template entries for CSE processes
   - Sets up ETL processing dates and stream configurations
   - Creates sample step occurrence data for validation

3. **CSEL04-Execute_DBT_Procedure.sql**
   - Creates `P_EXECUTE_DBT_CSEL()` stored procedure
   - Implements 18-step sequential execution logic
   - Comprehensive error handling and logging

4. **CSEL05-Task_Execute_DBT_CSEL.sql**
   - Creates `T_EXECUTE_DBT_CSEL` scheduled task
   - Daily execution at 3:00 AM Australia/Sydney
   - Prevents overlapping executions

### CCODS Pre-Installation

1. **CCODS02-Execute_DBT_CCODS_Procedure.sql**
   - Creates `P_EXECUTE_DBT_CCODS()` stored procedure
   - Implements 2-step sequential execution logic
   - Focused on BCFINSG data processing

2. **CCODS03-Task_Execute_DBT_CCODS.sql**
   - Creates `T_EXECUTE_DBT_CCODS` scheduled task
   - Daily execution at 4:00 AM Australia/Sydney (after CSEL)
   - Prevents overlapping executions

### Installation Order

```mermaid
flowchart LR
    ADMIN([Database Admin]) --> PHASE1[Phase 1: CSEL Setup<br/>🔧 Database, Control Data<br/>Procedure, Task]
    
    PHASE1 --> PHASE2[Phase 2: CCODS Setup<br/>🔧 Procedure, Task<br/>Dependencies]
    
    PHASE2 --> PHASE3[Phase 3: DBT Deploy<br/>📦 Upload Shared Project<br/>GDW1_DBT Workspace]
    
    PHASE3 --> PHASE4[Phase 4: Activation<br/>▶️ Resume Both Tasks<br/>Operational Status]
    
    PHASE4 --> COMPLETE[✅ Installation Complete<br/>Both Projects Scheduled<br/>Ready for Production]
    
    style ADMIN fill:#e3f2fd
    style PHASE1 fill:#e1f5fe
    style PHASE2 fill:#f3e5f5
    style PHASE3 fill:#e8f5e8
    style PHASE4 fill:#fff3e0
    style COMPLETE fill:#c8e6c9
```

---

## 📊 **Monitoring and Troubleshooting**

### Unified Monitoring Methods

```mermaid
flowchart LR
    EXEC[🔄 Process Execution<br/>CSEL + CCODS] --> METHOD1[📊 DBT Workspace<br/>SHOW TASKS<br/>Real-time Status]
    
    EXEC --> METHOD2[📈 Query History<br/>TASK_HISTORY()<br/>RESULT_SCAN()]
    
    EXEC --> METHOD3[📋 Unified Audit<br/>DCF_T_EXEC_LOG<br/>Multi-Process Queries]
    
    METHOD1 --> OUT1[⚡ Real-time<br/>Current Status<br/>Both Projects]
    
    METHOD2 --> OUT2[📊 Historical<br/>Execution Trends<br/>Comparative Analysis]
    
    METHOD3 --> OUT3[🔍 Detailed<br/>Step-by-Step Logs<br/>Process-Specific]
    
    style EXEC fill:#e3f2fd
    style METHOD1 fill:#e8f5e8
    style METHOD2 fill:#fff3e0
    style METHOD3 fill:#fce4ec
    style OUT1 fill:#c8e6c9
    style OUT2 fill:#c8e6c9
    style OUT3 fill:#c8e6c9
```

### Monitoring Queries

#### A. Task Status Monitoring
```sql
-- Monitor both CSEL and CCODS tasks
SHOW TASKS LIKE '%EXECUTE_DBT%' IN SCHEMA NPD_D12_DMN_GDWMIG.TMP;
```

#### B. Execution History Monitoring
```sql
-- Monitor both task execution histories
SELECT 
    TASK_NAME,
    QUERY_ID, 
    STATE, 
    QUERY_START_TIME, 
    QUERY_END_TIME,
    TOTAL_ELAPSED_TIME,
    ERROR_CODE,
    ERROR_MESSAGE
FROM TABLE(NPD_D12_DMN_GDWMIG.INFORMATION_SCHEMA.TASK_HISTORY()) A
WHERE TASK_NAME IN ('T_EXECUTE_DBT_CSEL', 'T_EXECUTE_DBT_CCODS')
  AND SCHEMA_NAME = 'TMP'
ORDER BY A.QUERY_START_TIME DESC;
```

#### C. Unified Audit Monitoring
```sql
-- Monitor recent executions for both processes
SELECT 
    PRCS_NAME,
    STRM_NAME,
    STEP_STATUS,
    MESSAGE_TYPE,
    MESSAGE_TEXT,
    CREATED_TS,
    SESSION_ID,
    WAREHOUSE_NAME
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG
WHERE PRCS_NAME IN ('P_EXECUTE_DBT_CSEL', 'P_EXECUTE_DBT_CCODS')
ORDER BY CREATED_TS DESC;

-- Monitor failed executions for both processes
SELECT 
    PRCS_NAME,
    STRM_NAME,
    STEP_STATUS,
    MESSAGE_TEXT,
    CREATED_TS
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG
WHERE PRCS_NAME IN ('P_EXECUTE_DBT_CSEL', 'P_EXECUTE_DBT_CCODS')
  AND STEP_STATUS = 'FAILED'
ORDER BY CREATED_TS DESC;
```

#### D. Process Comparison Dashboard
```sql
-- Compare execution patterns between CSEL and CCODS
SELECT 
    PRCS_NAME,
    DATE(CREATED_TS) as EXECUTION_DATE,
    COUNT(*) as TOTAL_STEPS,
    SUM(CASE WHEN STEP_STATUS = 'COMPLETED' THEN 1 ELSE 0 END) as SUCCESSFUL_STEPS,
    SUM(CASE WHEN STEP_STATUS = 'FAILED' THEN 1 ELSE 0 END) as FAILED_STEPS,
    MIN(CREATED_TS) as START_TIME,
    MAX(CREATED_TS) as END_TIME,
    DATEDIFF('minute', MIN(CREATED_TS), MAX(CREATED_TS)) as DURATION_MINUTES
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG
WHERE PRCS_NAME IN ('P_EXECUTE_DBT_CSEL', 'P_EXECUTE_DBT_CCODS')
  AND CREATED_TS >= CURRENT_DATE - 7  -- Last 7 days
GROUP BY PRCS_NAME, DATE(CREATED_TS)
ORDER BY EXECUTION_DATE DESC, PRCS_NAME;
```

---

## 🔧 **Maintenance and Operations**

### Manual Execution

#### CSEL Manual Execution
```sql
CALL NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CSEL();
```

#### CCODS Manual Execution
```sql
CALL NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CCODS();
```

### Task Management

#### Suspend Tasks
```sql
-- Suspend CSEL task
ALTER TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CSEL SUSPEND;

-- Suspend CCODS task
ALTER TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CCODS SUSPEND;
```

#### Resume Tasks
```sql
-- Resume CSEL task
ALTER TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CSEL RESUME;

-- Resume CCODS task
ALTER TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CCODS RESUME;
```

#### Modify Schedules
```sql
-- Modify CSEL schedule
ALTER TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CSEL 
SET SCHEDULE = 'USING CRON 0 2 * * * Australia/Sydney';

-- Modify CCODS schedule  
ALTER TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CCODS 
SET SCHEDULE = 'USING CRON 0 5 * * * Australia/Sydney';
```

---

## 🚨 **Troubleshooting Guide**

### Common Issues

#### CSEL-Specific Issues
1. **Step Failure in 18-step process**: Check `DCF_T_EXEC_LOG` for specific step details
2. **CSE_CPL_BUS_APP data issues**: Validate source data quality and stream status
3. **Appointment/Department data inconsistencies**: Review transformation logic in steps 7-18

#### CCODS-Specific Issues
1. **BCFINSG transformation failure**: Check Step 1 logs for data validation errors
2. **PLAN_BALN_SEGM_MSTR load failure**: Review Step 2 for target table issues
3. **Data reconciliation problems**: Use provided debug scripts for analysis

#### Shared Infrastructure Issues
1. **Task Not Running**: Verify task status and warehouse availability
2. **DBT Model Errors**: Review individual model logs in DBT workspace
3. **Permission Issues**: Ensure proper database and schema access rights
4. **Resource Contention**: Monitor warehouse usage during peak execution times

### Debug Resources

The CCODS project includes specialized debugging tools:
- `Debug_Data_Flow_Tracing.sql`: Trace data lineage through transformations
- `Process_Key_Fix_Analysis.sql`: Analyze and fix process key generation issues
- `Corrected_DEPT_APPT_MERGE.sql`: Reference for proper temporal table MERGE logic

---

## 📈 **Performance Optimization**

### Execution Timing
- **CSEL**: 3:00 AM (18 steps)
- **CCODS**: 4:00 AM (2 steps)
- **Gap**: 1-hour buffer prevents resource conflicts

### Resource Management
- **Shared Warehouse**: `wh_usr_npd_d12_gdwmig_001`
- **Overlap Prevention**: Both tasks configured with `ALLOW_OVERLAPPING_EXECUTION = FALSE`
- **Sequential Execution**: CCODS starts after CSEL completion window


