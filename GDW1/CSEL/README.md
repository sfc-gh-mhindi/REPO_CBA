# CSEL DBT Project - Snowflake Deployment Guide

## Overview

The CSEL (Commonwealth Bank Service Layer) project is a data pipeline implemented using DBT (Data Build Tool) and deployed within Snowflake. This project processes customer service data, appointments, products, and department information through a series of sequential transformations.

## Project Structure

```
CSEL/
├── NPW DBT Project - Modified/          # Main DBT project directory
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
│   ├── NPW04-Execute_DBT_Procedure.sql # Main execution procedure
│   └── NPW05-Task_Execute_DBT_CSEL.sql # Orchestration task
└── README.md                          # This file
```

## Deployment Architecture

### 1. Snowflake Database Structure

```mermaid
graph TB
    subgraph "Snowflake Environment"
        subgraph "NPD_D12_DMN_GDWMIG Database"
            subgraph "TMP Schema"
                PROC[P_EXECUTE_DBT_CSEL<br/>Stored Procedure]
                TASK[T_EXECUTE_DBT_CSEL<br/>Scheduled Task]
                DBT[GDW1_DBT<br/>DBT Project]
            end
        end
        
        subgraph "NPD_D12_DMN_GDWMIG_IBRG_V Database"
            subgraph "P_V_OUT_001_STD_0 Schema"
                LOG[DCF_T_EXEC_LOG<br/>Audit Table]
                CTRL[RUN_STRM_TMPL<br/>Control Table]
            end
            
            subgraph "DBT Models Output"
                VIEWS[Materialized Views]
                TABLES[Data Tables]
            end
        end
    end
    
    subgraph "External Systems"
        WORKSPACE[Snowflake<br/>DBT Workspace]
        WAREHOUSE[wh_usr_npd_d12_gdwmig_001<br/>Compute Warehouse]
    end
    
    subgraph "Scheduling"
        CRON[Daily 3 AM<br/>Australia/Sydney]
    end
    
    CRON --> TASK
    TASK --> PROC
    PROC --> DBT
    DBT --> WORKSPACE
    DBT --> WAREHOUSE
    PROC --> LOG
    PROC --> CTRL
    DBT --> VIEWS
    DBT --> TABLES
```

- **Primary Database**: `NPD_D12_DMN_GDWMIG`
- **Schema**: `TMP`
- **DBT Project Name**: `GDW1_DBT`
- **Models Database**: `NPD_D12_DMN_GDWMIG_IBRG_V` (materialized views and tables)

### 2. DBT Project Deployment

The DBT project is deployed to Snowflake Workspaces using the following configuration:

```yaml
# From dbt_project.yml
name: 'np_projects_commbank_sf_dbt'
profile: 'np_projects_commbank_sf_dbt'
models:
  np_projects_commbank_sf_dbt:
    +materialized: view
    +database: NPD_D12_DMN_GDWMIG_IBRG_V
```

#### Deployment Steps:
1. Upload the `NPW DBT Project - Modified` directory to Snowflake Workspaces
2. Configure the DBT project to use the `NPD_D12_DMN_GDWMIG.TMP.GDW1_DBT` workspace
3. Deploy the pre-installation scripts to create the execution infrastructure

## Execution Framework

### 3. Main Execution Procedure

**Location**: `NPW04-Execute_DBT_Procedure.sql`  
**Procedure Name**: `NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CSEL()`

This stored procedure orchestrates the execution of 18 sequential DBT model steps:

```mermaid
flowchart TD
    START([Start P_EXECUTE_DBT_CSEL]) --> CTRL_UPDATE[Update Control Tables<br/>RUN_STRM_ABRT_F = 'N'<br/>RUN_STRM_ACTV_F = 'I']
    
    CTRL_UPDATE --> STEP1[Step 1: processrunstreamstatuscheck<br/>Validate stream processing status]
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
    
    STEP18 --> SUCCESS[SUCCESS<br/>Log completion<br/>Return JSON result]
    
    STEP1 --> ERROR{Step Failed?}
    STEP2 --> ERROR
    STEP3 --> ERROR
    STEP4 --> ERROR
    STEP5 --> ERROR
    STEP6 --> ERROR
    STEP7 --> ERROR
    STEP8 --> ERROR
    STEP9 --> ERROR
    STEP10 --> ERROR
    STEP11 --> ERROR
    STEP12 --> ERROR
    STEP13 --> ERROR
    STEP14 --> ERROR
    STEP15 --> ERROR
    STEP16 --> ERROR
    STEP17 --> ERROR
    STEP18 --> ERROR
    
    ERROR --> FAIL[FAILED<br/>Log error details<br/>Return failure JSON]
    
    style START fill:#e1f5fe
    style SUCCESS fill:#c8e6c9
    style FAIL fill:#ffcdd2
    style ERROR fill:#fff3e0
```

#### Execution Steps:
1. **processrunstreamstatuscheck** - Validates stream processing status
2. **utilprosisacprevloadcheck** - Checks previous load completion
3. **loadgdwproskeyseq** - Loads GDW process key sequences
4. **ldmap_cse_pack_pdct_pllkp** - Loads product mapping lookups
5. **processrunstreamfinishingpoint** - Sets processing checkpoints
6. **processrunstreamstatuscheck** (with CSE_CPL_BUS_APP variable)
7. **extpl_app** - Extracts application data
8. **xfmpl_appfrmext** - Transforms application data from extraction
9. **ldtmp_appt_deptrmxfm** - Loads appointment department temp data
10. **dltappt_deptfrmtmp_appt_dept** - Delta processing for appointment departments
11. **ldapptdeptupd** - Updates appointment department data
12. **ldapptdeptins** - Inserts new appointment department data
13. **ldtmp_appt_pdctfrmxfm** - Loads appointment product temp data
14. **dltappt_pdctfrmtmp_appt_pdct** - Delta processing for appointment products
15. **ldapptpdctupd** - Updates appointment product data
16. **ldapptpdctins** - Inserts new appointment product data
17. **ldapptdeptupd** (with dept_appt target table)
18. **ldapptdeptins** (with dept_appt target table)

#### Key Features:
- **Error Handling**: Each step includes comprehensive error handling with detailed logging
- **Progress Tracking**: JSON-based result tracking with step-by-step status
- **Rollback Capability**: Failed steps prevent further execution and log detailed error information
- **Flexible Execution**: Some steps include dynamic variables for different target tables

### 4. Task Orchestration

**Location**: `NPW05-Task_Execute_DBT_CSEL.sql`  
**Task Name**: `NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CSEL`

#### Task Configuration:
```sql
CREATE OR REPLACE TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CSEL
    WAREHOUSE = wh_usr_npd_d12_gdwmig_001
    SCHEDULE = 'USING CRON 0 3 * * * Australia/Sydney'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
AS
    CALL NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CSEL();
```

- **Schedule**: Daily execution at 3:00 AM Australia/Sydney timezone
- **Warehouse**: Dedicated warehouse for consistent resource allocation
- **Overlap Prevention**: Ensures only one instance runs at a time

## Monitoring and Troubleshooting

### 5. Monitoring Methods

```mermaid
graph LR
    subgraph "CSEL DBT Execution Monitoring"
        EXEC[P_EXECUTE_DBT_CSEL<br/>Execution]
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
        
        subgraph "3. Audit Table Monitoring"
            AUDIT[DCF_T_EXEC_LOG<br/>Audit Table]
            LOG_QUERY[Status Queries<br/>Recent/Failed Executions]
        end
    end
    
    subgraph "Monitoring Outputs"
        REAL_TIME[Real-time Status<br/>Active Executions]
        HISTORY[Execution History<br/>Performance Metrics]
        DETAILED[Detailed Logs<br/>Step-by-step Status]
    end
    
    EXEC --> WS
    EXEC --> QH
    EXEC --> AUDIT
    
    WS --> WS_CMD
    WS_CMD --> REAL_TIME
    
    QH --> TASK_HIST
    QH --> RESULT_SCAN
    TASK_HIST --> HISTORY
    RESULT_SCAN --> HISTORY
    
    AUDIT --> LOG_QUERY
    LOG_QUERY --> DETAILED
    
    style EXEC fill:#e3f2fd
    style WS fill:#f3e5f5
    style QH fill:#e8f5e8
    style AUDIT fill:#fff3e0
    style REAL_TIME fill:#c8e6c9
    style HISTORY fill:#c8e6c9
    style DETAILED fill:#c8e6c9
```

#### A. DBT Project Monitoring
Monitor execution directly from the Snowflake DBT Workspace:
```sql
-- View active DBT executions
SHOW TASKS IN SCHEMA NPD_D12_DMN_GDWMIG.TMP;
```

#### B. Query History Monitoring
```sql
-- Monitor task execution history
SELECT QUERY_ID, STATE, QUERY_START_TIME, *
FROM TABLE(NPD_D12_DMN_GDWMIG.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'T_EXECUTE_DBT_CSEL'
)) A
WHERE SCHEMA_NAME = 'TMP'
ORDER BY A.QUERY_START_TIME DESC;

-- View specific execution results
SELECT * FROM TABLE(RESULT_SCAN('<QUERY_ID>'));
```

#### C. Audit Table Monitoring
**Primary Logging Table**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG`

```sql
-- Monitor recent executions
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
WHERE PRCS_NAME = 'P_EXECUTE_DBT_CSEL'
ORDER BY CREATED_TS DESC;

-- Monitor failed executions
SELECT *
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.DCF_T_EXEC_LOG
WHERE PRCS_NAME = 'P_EXECUTE_DBT_CSEL'
  AND STEP_STATUS = 'FAILED'
ORDER BY CREATED_TS DESC;
```

### 6. Execution Results

The procedure returns a JSON object containing:
- `total_steps`: Number of completed steps
- `final_status`: SUCCESS, FAILED, or EXCEPTION
- `completed_at`: Timestamp of completion (for successful runs)
- `failed_at_step`: Step number where failure occurred (for failed runs)
- `steps`: Array of individual step results with timestamps and status

#### Example Success Result:
```json
{
  "total_steps": 18,
  "final_status": "SUCCESS",
  "completed_at": "2024-01-15 03:45:32.123",
  "steps": [...]
}
```

#### Example Failure Result:
```json
{
  "total_steps": 7,
  "final_status": "FAILED",
  "failed_at_step": 7,
  "steps": [...]
}
```

## Maintenance and Operations

### 7. Manual Execution
To manually execute the procedure:
```sql
CALL NPD_D12_DMN_GDWMIG.TMP.P_EXECUTE_DBT_CSEL();
```

### 8. Task Management
```sql
-- Suspend the task
ALTER TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CSEL SUSPEND;

-- Resume the task
ALTER TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CSEL RESUME;

-- Modify task schedule
ALTER TASK NPD_D12_DMN_GDWMIG.TMP.T_EXECUTE_DBT_CSEL 
SET SCHEDULE = 'USING CRON 0 4 * * * Australia/Sydney';
```

### 9. Control Tables
The procedure updates control tables to manage stream processing:
```sql
-- Stream control update (executed at procedure start)
UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_TMPL 
SET RUN_STRM_ABRT_F = 'N', RUN_STRM_ACTV_F = 'I' 
WHERE RUN_STRM_C IN ('CSE_L4_PRE_PROC','CSE_CPL_BUS_APP') 
  AND SYST_C = 'CSEL4';
```

## Database Structure & Model Relationships

The following diagram shows the database structure and relationships between different components:

```mermaid
graph TB
    subgraph "Database Structure"
        subgraph "NPD_D12_DMN_GDWMIG"
            subgraph "TMP Schema"
                PROC2[P_EXECUTE_DBT_CSEL<br/>Stored Procedure]
                TASK2[T_EXECUTE_DBT_CSEL<br/>Scheduled Task]
                DBT2[GDW1_DBT<br/>DBT Project]
            end
        end
        
        subgraph "NPD_D12_DMN_GDWMIG_IBRG_V"
            subgraph "P_V_OUT_001_STD_0"
                LOG2[DCF_T_EXEC_LOG<br/>📊 Audit Logs]
                CTRL2[RUN_STRM_TMPL<br/>⚙️ Control Table]
            end
            
            subgraph "Model Outputs"
                PROC_KEY[02processkey<br/>🔑 Process Keys]
                MAPPING[04MappingLookupSets<br/>🗺️ Lookups]
                EXTRACT[08extraction<br/>📥 Data Extraction]
                TRANSFORM[12MappingTransformation<br/>🔄 Transformations]
                TEMP[14loadtotemp<br/>📝 Temp Tables]
                DELTA[16transformdelta<br/>🔺 Delta Processing]
                LOAD[18loadtogdw<br/>📤 Final Load]
                META[24processmetadata<br/>📋 Metadata]
                APPT[appt_pdct<br/>📅 Appointments]
            end
        end
    end
    
    TASK2 --> PROC2
    PROC2 --> DBT2
    PROC2 --> LOG2
    PROC2 --> CTRL2
    
    DBT2 --> PROC_KEY
    DBT2 --> MAPPING
    DBT2 --> EXTRACT
    DBT2 --> TRANSFORM
    DBT2 --> TEMP
    DBT2 --> DELTA
    DBT2 --> LOAD
    DBT2 --> META
    DBT2 --> APPT
    
    style PROC2 fill:#e1f5fe
    style TASK2 fill:#f3e5f5
    style DBT2 fill:#e8f5e8
    style LOG2 fill:#fff3e0
    style CTRL2 fill:#fce4ec
```

## Troubleshooting Guide

### Common Issues:
1. **Step Failure**: Check the `DCF_T_EXEC_LOG` table for detailed error messages
2. **Task Not Running**: Verify task status with `SHOW TASKS` and check warehouse availability
3. **DBT Model Errors**: Review individual model logs in the DBT workspace
4. **Permission Issues**: Ensure proper database and schema access rights

### Support Contacts:
- **Database Team**: For Snowflake infrastructure issues
- **ETL Team**: For data processing and transformation issues
- **Business Team**: For data validation and business logic questions

---

*Last Updated: [Current Date]*
*Version: 1.0*

