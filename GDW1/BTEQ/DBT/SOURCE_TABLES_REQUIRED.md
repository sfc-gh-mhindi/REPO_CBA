# GDW1 BTEQ to DBT Migration - Required Source Tables

## Overview

This document lists all source tables required for the GDW1 BTEQ to DBT migration project to function properly. Tables are organized by database/schema based on the variable mappings in `dbt_project.yml`.

## Database/Schema Variable Mappings

From `dbt_project.yml` (Current Active Configuration):
- **CAD_PROD_DATA**: `NPD_D12_DMN_GDWMIG_IBRG.P_D_GDW_001_STD_0`
- **CAD_PROD_MACRO**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0`
- **DDSTG**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_V_STG_001_STD_0` 
- **VTECH**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_V_TECH_001_STD_0`
- **VCBODS**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_V_CBODS_001_STD_0`
- **VEXTR**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_V_EXTR_001_STD_0`
- **STARMACRDB**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0`

### Database Structure
- **Production Database**: `NPD_D12_DMN_GDWMIG_IBRG` (no _V suffix)
- **View/Processing Database**: `NPD_D12_DMN_GDWMIG_IBRG_V` (with _V suffix)

## Required Source Tables by Schema

### 1. Production Data Schema (CAD_PROD_DATA)
**Database**: `NPD_D12_DMN_GDWMIG_IBRG.P_D_GDW_001_STD_0`

| Table Name | Purpose | Used In |
|------------|---------|---------|
| `ACCT_BALN_BKDT` | Account balance backdate main table | Account balance cleanup, processing |
| `PROCESS_CONTROL` | Process control and key management | Process key generation |
| `BATCH_CONTROL` | Batch control and key management | Batch key generation |

### 2. Staging Schema (DDSTG)  
**Database**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_V_STG_001_STD_0`

| Table Name | Purpose | Used In |
|------------|---------|---------|
| `ACCT_BALN_BKDT_STG1` | Account balance staging - records to be adjusted | Cleanup processing |
| `ACCT_BALN_BKDT_STG2` | Account balance staging - final processed records | Insert processing |
| `ACCT_BALN_BKDT_ADJ_RULE` | Account balance adjustment rules | Staging data filtering |
| `DERV_PRTF_ACCT_STAG` | Derived portfolio account staging | Portfolio setup |
| `DERV_PRTF_PATY_STAG` | Derived portfolio party staging | Portfolio setup |

### 3. Technical Schema (VTECH)
**Database**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_V_TECH_001_STD_0`

| Table Name | Purpose | Used In |
|------------|---------|---------|
| `ACCT_PDCT` | Account product information | SAP EDO reconciliation, multiple processes |
| `ACCT_BASE` | Account base information | SAP EDO reconciliation |
| `ACCT_OFFR` | Account offer information | SAP EDO reconciliation |
| `ACCT_REL` | Account relationship information | SAP EDO reconciliation |
| `ACCT_BALN_BKDT` | Account balance backdate (tech view) | Staging data population |
| `DERV_PRTF_ACCT` | Derived portfolio account data | Portfolio setup |
| `DERV_PRTF_PATY` | Derived portfolio party data | Portfolio setup |
| `UTIL_PROS_ISAC` | Utility process ISAC | Data watcher, dependency checking |
| `UTIL_PARM` | Utility parameters | Configuration and control |
| `UTIL_BTCH_ISAC` | Utility batch ISAC | Process key generation |
| `MAP_SAP_INVL_PDCT` | SAP invalid product mapping | Tax insurance processing |
| `MAP_SAP_ACCT_REL` | SAP account relationship mapping | Tax insurance processing |
| `MAP_SAP_RESI_STUS` | SAP residence status mapping | Tax insurance processing |
| `MAP_SAP_IDNN_TYPE` | SAP identification type mapping | Tax insurance processing |
| `MAP_SAP_IDNN_STUS` | SAP identification status mapping | Tax insurance processing |

### 4. CBODS Schema (VCBODS)
**Database**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_V_CBODS_001_STD_0`

| Table Name | Purpose | Used In |
|------------|---------|---------|
| `CBA_FNCL_SERV_GL_DATA_CURR` | CBA financial service GL data current | SAP EDO weekly load |
| `MSTR_CNCT_MSTR_DATA_GENL_CURR` | Master connect master data general current | SAP EDO weekly load |
| `CNCT_HEAD` | Connection head information | SAP EDO weekly load |
| `CNCT_HEAD_CURR` | Connection head current information | SAP EDO weekly load |
| `MSTR_CNCT_BALN_TRNF_PRTP` | Master connect balance transfer participant | SAP EDO weekly load |
| `MSTR_CNCT_PRXY_ACCT` | Master connect proxy account | SAP EDO weekly load |
| `ACCT_MSTR_DATA` | Account master data | SAP EDO weekly load |
| `ACCT_MSTR_DATA_CURR` | Account master data current | SAP EDO weekly load |
| `ACCT_MSTR_DATA_GENL` | Account master data general | Tax insurance processing |
| `WUL_NON_SAP_ACCT` | WUL non-SAP account | SAP EDO weekly load |
| `BUSN_PTNR` | Business partner | Tax insurance processing |
| `PTNR_IDNN_NUMB` | Partner identification number | Tax insurance processing |

### 5. Extract Schema (VEXTR)
**Database**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_V_EXTR_001_STD_0`

| Table Name | Purpose | Used In |
|------------|---------|---------|
| `BD_SALE_PDCT` | Business data sales product | SAP EDO weekly load |

### 6. DCF Framework Tables
**Database**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0`

| Table Name | Purpose | Used In |
|------------|---------|---------|
| `DCF_T_EXEC_LOG` | DCF execution logging table | All processes (logging) |
| `DCF_T_PROCESS_INSTANCE` | DCF process instance tracking | All processes (tracking) |
| `DCF_T_STRM_BUS_DT` | DCF stream business date | Stream validation |

## Tables by Functional Area

### Account Balance Processing
**Primary Tables:**
- `ACCT_BALN_BKDT` (Production)
- `ACCT_BALN_BKDT_STG1` (Staging)
- `ACCT_BALN_BKDT_STG2` (Staging)
- `ACCT_BALN_BKDT_ADJ_RULE` (Staging)
- `ACCT_BALN_BKDT` (VTECH view)

### Derived Account Party Processing
**Primary Tables:**
- `DERV_PRTF_ACCT` (VTECH)
- `DERV_PRTF_PATY` (VTECH)
- `DERV_PRTF_ACCT_STAG` (Staging)
- `DERV_PRTF_PATY_STAG` (Staging)

### Portfolio Technical Processing
**Primary Tables:**
- `ACCT_PDCT` (VTECH)
- `ACCT_BASE` (VTECH)
- `ACCT_OFFR` (VTECH)
- `ACCT_REL` (VTECH)
- All CBODS tables for complex reconciliation

### Process Control
**Primary Tables:**
- `UTIL_PROS_ISAC` (VTECH)
- `UTIL_PARM` (VTECH)
- `UTIL_BTCH_ISAC` (VTECH)

## Data Quality and Dependencies

### Critical Dependencies
1. **DCF Tables** must exist before any processing
2. **Staging tables** must be populated before intermediate processing
3. **Control tables** must have current process/batch keys
4. **Mapping tables** (MAP_SAP_*) must be current for date ranges

### Data Freshness Requirements
- **Daily**: Account balance staging tables
- **Weekly**: SAP EDO reconciliation source tables
- **Monthly**: Tax insurance source tables
- **Real-time**: DCF logging and control tables

## Setup SQL Scripts

### Create Required Schemas
```sql
-- Create databases if they don't exist
CREATE DATABASE IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG_V;
CREATE DATABASE IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.P_V_STG_001_STD_0;
CREATE SCHEMA IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.P_V_TECH_001_STD_0;
CREATE SCHEMA IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.P_V_CBODS_001_STD_0;
CREATE SCHEMA IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.P_V_EXTR_001_STD_0;
CREATE SCHEMA IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0;
CREATE SCHEMA IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG.P_D_GDW_001_STD_0;
```

### DCF Framework Tables
```sql
-- DCF Execution Log Table
CREATE TABLE IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.DCF_T_EXEC_LOG (
    EXEC_LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    PRCS_NAME VARCHAR(100) NOT NULL,
    STRM_NAME VARCHAR(100) NOT NULL,
    STRM_ID VARCHAR(50),
    BUSINESS_DATE DATE,
    STEP_STATUS VARCHAR(20) DEFAULT '-',
    MESSAGE_TYPE NUMBER NOT NULL,
    MESSAGE_TEXT VARCHAR(4000),
    SQL_TEXT VARCHAR(16777216),
    CREATED_BY VARCHAR(100),
    CREATED_TS TIMESTAMP_NTZ(6) DEFAULT CURRENT_TIMESTAMP(),
    SESSION_ID VARCHAR(50),
    WAREHOUSE_NAME VARCHAR(100)
);

-- DCF Process Instance Table
CREATE TABLE IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.DCF_T_PROCESS_INSTANCE (
    PROCESS_INSTANCE_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    PROCESS_NAME VARCHAR(100) NOT NULL,
    STREAM_NAME VARCHAR(100) NOT NULL,
    MODEL_NAME VARCHAR(100),
    PROCESS_STATUS VARCHAR(20) NOT NULL,
    START_TIMESTAMP TIMESTAMP_NTZ(6),
    END_TIMESTAMP TIMESTAMP_NTZ(6),
    STATUS_MESSAGE VARCHAR(4000),
    CREATED_BY VARCHAR(100),
    CREATED_TS TIMESTAMP_NTZ(6) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_BY VARCHAR(100),
    UPDATED_TS TIMESTAMP_NTZ(6)
);

-- DCF Stream Business Date Table
CREATE TABLE IF NOT EXISTS NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.DCF_T_STRM_BUS_DT (
    STRM_NAME VARCHAR(100) NOT NULL,
    BUS_DT DATE NOT NULL,
    STRM_ID VARCHAR(50),
    CREATED_TS TIMESTAMP_NTZ(6) DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (STRM_NAME, BUS_DT)
);
```

## Validation Queries

### Check Table Existence
```sql
-- Verify all required tables exist across both databases
-- Check View/Processing Database (NPD_D12_DMN_GDWMIG_IBRG_V)
SELECT 
    table_schema,
    table_name,
    table_type,
    'NPD_D12_DMN_GDWMIG_IBRG_V' as database_name
FROM NPD_D12_DMN_GDWMIG_IBRG_V.information_schema.tables 
WHERE table_schema IN (
    'P_V_STG_001_STD_0',
    'P_V_TECH_001_STD_0', 
    'P_V_CBODS_001_STD_0',
    'P_V_EXTR_001_STD_0',
    'P_D_DCF_001_STD_0'
)
UNION ALL
-- Check Production Database (NPD_D12_DMN_GDWMIG_IBRG)
SELECT 
    table_schema,
    table_name,
    table_type,
    'NPD_D12_DMN_GDWMIG_IBRG' as database_name
FROM NPD_D12_DMN_GDWMIG_IBRG.information_schema.tables 
WHERE table_schema IN (
    'P_D_GDW_001_STD_0'
)
ORDER BY database_name, table_schema, table_name;
```

### Check Data Freshness
```sql
-- Check latest data in key tables
SELECT 
    'ACCT_BALN_BKDT_STG2' as table_name,
    MAX(LOAD_D) as latest_date,
    COUNT(*) as record_count
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_STG_001_STD_0.ACCT_BALN_BKDT_STG2
UNION ALL
SELECT 
    'ACCT_PDCT' as table_name,
    MAX(LOAD_D) as latest_date,
    COUNT(*) as record_count  
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_TECH_001_STD_0.ACCT_PDCT;
```

## Missing Table Impact

### Critical Impact (Project cannot run)
- DCF framework tables
- Account balance staging tables
- Core technical tables (ACCT_PDCT, ACCT_BASE)

### High Impact (Major functionality affected)
- CBODS source tables (SAP EDO processing fails)
- Mapping tables (Tax insurance processing fails)
- Control tables (Process management affected)

### Medium Impact (Specific features affected)
- Extract tables (Limited reconciliation)
- Utility tables (Reduced monitoring)

---

## Document Updates

**Latest Update**: Database/schema mappings updated to reflect current active configuration from `dbt_project.yml`:
- **Database Migration**: `PSUND_MIGR_*` → `NPD_D12_DMN_GDWMIG_IBRG*`
- **Two-Database Structure**: Production (`NPD_D12_DMN_GDWMIG_IBRG`) and View/Processing (`NPD_D12_DMN_GDWMIG_IBRG_V`)
- **DCF Table Names**: Standardized to `DCF_T_PROCESS_INSTANCE` 
- **Schema Consolidation**: Removed separate `UTILSTG` schema (utility tables moved to `VTECH`)

**Note**: This list represents the comprehensive set of tables referenced in the original 47 BTEQ files. Some tables may need to be created or mapped from existing sources during implementation. Verify table availability and mappings before deployment. 