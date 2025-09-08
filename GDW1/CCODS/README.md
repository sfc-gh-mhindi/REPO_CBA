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

```mermaid
flowchart TD
    START_CSEL([Start P_EXECUTE_DBT_CSEL]) --> CTRL_UPDATE_CSEL[Update Control Tables<br/>RUN_STRM_ABRT_F = 'N'<br/>RUN_STRM_ACTV_F = 'I']
    
    CTRL_UPDATE_CSEL --> STEP1[Step 1: processrunstreamstatuscheck<br/>Validate stream processing status]
    STEP1 --> STEP2[Step 2: utilprosisacprevloadcheck<br/>Check previous load completion]
    STEP2 --> STEP3[Step 3: loadgdwproskeyseq<br/>Load GDW process key sequences]
    STEP3 --> STEP4[Step 4: ldmap_cse_pack_pdct_pllkp<br/>Load product mapping lookups]
    STEP4 --> STEP5[Step 5: processrunstreamfinishingpoint<br/>Set processing checkpoints]
    
    STEP5 --> STEP6[Step 6: processrunstreamstatuscheck<br/>with CSE_CPL_BUS_APP variable]
    STEP6 --> STEP7[Step 7: extpl_app<br/>Extract application data]
    STEP7 --> STEP8[Step 8: xfmpl_appfrmext<br/>Transform application data]
    STEP8 --> STEP9[Step 9: ldtmp_appt_deptrmxfm<br/>Load appointment department temp data]
    
    STEP9 --> STEP10[Step 10: dltappt_deptfrmtmp_appt_dept<br/>Delta processing for appointment departments]
    STEP10 --> STEP11[Step 11: ldapptdeptupd<br/>Update appointment department data]
    STEP11 --> STEP12[Step 12: ldapptdeptins<br/>Insert new appointment department data]
    
    STEP12 --> STEP13[Step 13: ldtmp_appt_pdctfrmxfm<br/>Load appointment product temp data]
    STEP13 --> STEP14[Step 14: dltappt_pdctfrmtmp_appt_pdct<br/>Delta processing for appointment products]
    STEP14 --> STEP15[Step 15: ldapptpdctupd<br/>Update appointment product data]
    STEP15 --> STEP16[Step 16: ldapptpdctins<br/>Insert new appointment product data]
    
    STEP16 --> STEP17[Step 17: ldapptdeptupd<br/>with dept_appt target table]
    STEP17 --> STEP18[Step 18: ldapptdeptins<br/>with dept_appt target table]
    
    STEP18 --> SUCCESS_CSEL[SUCCESS<br/>Log completion<br/>Return JSON result]
    
    STEP1 --> ERROR_CSEL{Step Failed?}
    STEP2 --> ERROR_CSEL
    STEP3 --> ERROR_CSEL
    STEP4 --> ERROR_CSEL
    STEP5 --> ERROR_CSEL
    STEP6 --> ERROR_CSEL
    STEP7 --> ERROR_CSEL
    STEP8 --> ERROR_CSEL
    STEP9 --> ERROR_CSEL
    STEP10 --> ERROR_CSEL
    STEP11 --> ERROR_CSEL
    STEP12 --> ERROR_CSEL
    STEP13 --> ERROR_CSEL
    STEP14 --> ERROR_CSEL
    STEP15 --> ERROR_CSEL
    STEP16 --> ERROR_CSEL
    STEP17 --> ERROR_CSEL
    STEP18 --> ERROR_CSEL
    
    ERROR_CSEL --> FAIL_CSEL[FAILED<br/>Log error details<br/>Return failure JSON]
    
    style START_CSEL fill:#e1f5fe
    style SUCCESS_CSEL fill:#c8e6c9
    style FAIL_CSEL fill:#ffcdd2
    style ERROR_CSEL fill:#fff3e0
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
flowchart TD
    START_CCODS([Start P_EXECUTE_DBT_CCODS]) --> LOG_START[Log Process Start<br/>CCODS DBT Execution<br/>2 Sequential Model Groups]
    
    LOG_START --> STEP1_CCODS[Step 1: 40_transform<br/>xfmplanbalnsegmmstrfrombcfinsg<br/>🔄 BCFINSG Data Transformation]
    
    STEP1_CCODS --> VALIDATE1[Validate Transformation<br/>✅ Check Success Flag<br/>📊 Log Results]
    
    VALIDATE1 --> STEP1_SUCCESS{Step 1<br/>Success?}
    
    STEP1_SUCCESS -->|✅ Yes| LOG_TRANSFORM[Log Transformation Success<br/>All BCFINSG models executed<br/>plan_baln_segm_mstr_i ready]
    
    STEP1_SUCCESS -->|❌ No| FAIL1[FAILED at Step 1<br/>🚨 Transformation Failed<br/>Log Error & Return]
    
    LOG_TRANSFORM --> STEP2_CCODS[Step 2: 60_load_gdw<br/>ldbcfinsgplanbalnsegmmstr<br/>📤 Final GDW Loading]
    
    STEP2_CCODS --> VALIDATE2[Validate Load<br/>✅ Check Success Flag<br/>📊 Log Results]
    
    VALIDATE2 --> STEP2_SUCCESS{Step 2<br/>Success?}
    
    STEP2_SUCCESS -->|✅ Yes| LOG_LOAD[Log Load Success<br/>PLAN_BALN_SEGM_MSTR<br/>Population Complete]
    
    STEP2_SUCCESS -->|❌ No| FAIL2[FAILED at Step 2<br/>🚨 Load Failed<br/>Log Error & Return]
    
    LOG_LOAD --> SUCCESS_CCODS[SUCCESS<br/>📋 Complete Process Log<br/>🎯 Return JSON Result<br/>Total Steps: 2]
    
    style START_CCODS fill:#e1f5fe
    style STEP1_CCODS fill:#f3e5f5
    style STEP2_CCODS fill:#e8f5e8
    style SUCCESS_CCODS fill:#c8e6c9
    style FAIL1 fill:#ffcdd2
    style FAIL2 fill:#ffcdd2
    style STEP1_SUCCESS fill:#fff3e0
    style STEP2_SUCCESS fill:#fff3e0
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
graph TB
    subgraph "Snowflake Environment"
        subgraph "NPD_D12_DMN_GDWMIG Database"
            subgraph "TMP Schema"
                PROC_CSEL[P_EXECUTE_DBT_CSEL<br/>🔄 CSEL Procedure<br/>18 Steps]
                TASK_CSEL[T_EXECUTE_DBT_CSEL<br/>⏰ 3:00 AM Daily]
                PROC_CCODS[P_EXECUTE_DBT_CCODS<br/>🔄 CCODS Procedure<br/>2 Steps]
                TASK_CCODS[T_EXECUTE_DBT_CCODS<br/>⏰ 4:00 AM Daily]
                DBT[GDW1_DBT<br/>📦 Shared DBT Project]
            end
        end
        
        subgraph "NPD_D12_DMN_GDWMIG_IBRG_V Database"
            subgraph "P_V_OUT_001_STD_0 Schema"
                LOG[DCF_T_EXEC_LOG<br/>📊 Unified Audit Table]
                CTRL[RUN_STRM_TMPL<br/>⚙️ Control Table]
            end
            
            subgraph "CSEL Model Outputs"
                CSEL_VIEWS[📋 CSE Data Models<br/>Appointments, Products, Departments]
                CSEL_TABLES[📊 Service Layer Tables]
            end
            
            subgraph "CCODS Model Outputs"
                CCODS_VIEWS[📈 BCFINSG Transformations]
                CCODS_TABLES[🎯 PLAN_BALN_SEGM_MSTR]
            end
        end
    end
    
    subgraph "External Systems"
        WORKSPACE[Snowflake<br/>DBT Workspace]
        WAREHOUSE[wh_usr_npd_d12_gdwmig_001<br/>Compute Warehouse]
    end
    
    subgraph "Scheduling"
        CRON_CSEL[3:00 AM<br/>Australia/Sydney]
        CRON_CCODS[4:00 AM<br/>Australia/Sydney]
    end
    
    CRON_CSEL --> TASK_CSEL
    CRON_CCODS --> TASK_CCODS
    TASK_CSEL --> PROC_CSEL
    TASK_CCODS --> PROC_CCODS
    PROC_CSEL --> DBT
    PROC_CCODS --> DBT
    DBT --> WORKSPACE
    DBT --> WAREHOUSE
    PROC_CSEL --> LOG
    PROC_CCODS --> LOG
    PROC_CSEL --> CTRL
    DBT --> CSEL_VIEWS
    DBT --> CSEL_TABLES
    DBT --> CCODS_VIEWS
    DBT --> CCODS_TABLES
    
    style PROC_CSEL fill:#e1f5fe
    style PROC_CCODS fill:#f3e5f5
    style TASK_CSEL fill:#e8f5e8
    style TASK_CCODS fill:#fff3e0
    style DBT fill:#fce4ec
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
sequenceDiagram
    participant Admin as Database Admin
    participant CSEL as CSEL Installation
    participant CCODS as CCODS Installation
    participant DBT as DBT Workspace
    
    Admin->>CSEL: 1. Deploy CSEL Database Setup
    CSEL->>Admin: ✅ Tables & Views Created
    
    Admin->>CSEL: 2. Deploy CSEL Control Data
    CSEL->>Admin: ✅ Reference Data Populated
    
    Admin->>CSEL: 3. Deploy CSEL Procedure
    CSEL->>Admin: ✅ P_EXECUTE_DBT_CSEL Created
    
    Admin->>CSEL: 4. Deploy CSEL Task
    CSEL->>Admin: ✅ T_EXECUTE_DBT_CSEL Created
    
    Admin->>CCODS: 5. Deploy CCODS Procedure
    CCODS->>Admin: ✅ P_EXECUTE_DBT_CCODS Created
    
    Admin->>CCODS: 6. Deploy CCODS Task
    CCODS->>Admin: ✅ T_EXECUTE_DBT_CCODS Created
    
    Admin->>DBT: 7. Upload Shared DBT Project
    DBT->>Admin: ✅ GDW1_DBT Workspace Ready
    
    Admin->>CSEL: 8. Resume CSEL Task
    Admin->>CCODS: 9. Resume CCODS Task
    
    Note over Admin, DBT: Both projects now scheduled and operational
```

---

## 📊 **Monitoring and Troubleshooting**

### Unified Monitoring Methods

```mermaid
graph LR
    subgraph "Execution Monitoring"
        CSEL_EXEC[CSEL Execution<br/>P_EXECUTE_DBT_CSEL]
        CCODS_EXEC[CCODS Execution<br/>P_EXECUTE_DBT_CCODS]
    end
    
    subgraph "Monitoring Methods"
        subgraph "1. DBT Workspace Monitoring"
            WS[Snowflake DBT<br/>Workspace]
            WS_CMD[SHOW TASKS<br/>Commands]
        end
        
        subgraph "2. Query History Monitoring"
            QH[Query History<br/>Analysis]
            TASK_HIST[TASK_HISTORY<br/>Function]
            RESULT_SCAN[RESULT_SCAN<br/>Function]
        end
        
        subgraph "3. Unified Audit Monitoring"
            AUDIT[DCF_T_EXEC_LOG<br/>Shared Audit Table]
            LOG_QUERY[Multi-Process Queries<br/>CSEL + CCODS Status]
        end
    end
    
    subgraph "Monitoring Outputs"
        REAL_TIME[Real-time Status<br/>Both Projects]
        HISTORY[Execution History<br/>Comparative Analysis]
        DETAILED[Detailed Logs<br/>Process-Specific Status]
    end
    
    CSEL_EXEC --> WS
    CCODS_EXEC --> WS
    CSEL_EXEC --> QH
    CCODS_EXEC --> QH
    CSEL_EXEC --> AUDIT
    CCODS_EXEC --> AUDIT
    
    WS --> WS_CMD
    WS_CMD --> REAL_TIME
    
    QH --> TASK_HIST
    QH --> RESULT_SCAN
    TASK_HIST --> HISTORY
    RESULT_SCAN --> HISTORY
    
    AUDIT --> LOG_QUERY
    LOG_QUERY --> DETAILED
    
    style CSEL_EXEC fill:#e1f5fe
    style CCODS_EXEC fill:#f3e5f5
    style WS fill:#e8f5e8
    style QH fill:#fff3e0
    style AUDIT fill:#fce4ec
    style REAL_TIME fill:#c8e6c9
    style HISTORY fill:#c8e6c9
    style DETAILED fill:#c8e6c9
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


