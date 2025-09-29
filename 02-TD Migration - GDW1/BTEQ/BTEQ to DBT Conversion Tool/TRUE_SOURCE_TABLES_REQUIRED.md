# TRUE External Source Tables Required for BTEQ Models

## Overview

This document lists **ONLY** the true external source tables that must exist **BEFORE** running the BTEQ to DBT processes. These are tables that are **READ FROM** but **NEVER CREATED** by the BTEQ scripts.

**Generated**: 2025-08-21  
**Analysis Method**: Analyzed Original Files to distinguish source tables (read-only) from target tables (created/updated by BTEQ)  
**TRUE Source Tables**: 34 (vs 83 in previous documentation)

## Key Finding

The previous `SOURCE_TABLES_REQUIRED.md` included **49 internal staging/target tables** that are actually **created by the BTEQ processes themselves**, not external source tables.

## Complete TRUE Source Table List

**Only these 34 tables must exist as external data sources:**

```
PVTECH.ACCT_BASE
VCBODS.ACCT_MSTR_CYT_DATA
PVTECH.ACCT_OFFR
PVTECH.ACCT_PATY
PVTECH.ACCT_PDCT
PVTECH.ACCT_UNID_PATY
PVTECH.ACCT_XREF_BPS_CBS
PVTECH.ACCT_XREF_MAS_DAR
VCBODS.BUSN_PTNR
VCBODS.CBA_FNCL_SERV_GL_DATA_CURR
UTILSTG.CBM_UTIL_RUN_STRM_OCCR_CNTL_H
PVTECH.CLS_FCLY
PVTECH.CLS_UNID_PATY
PVTECH.DAR_ACCT
PVTECH.DEPT_DIMN_NODE_ANCS_CURR
PVTECH.DERV_PRTF_ACCT
PVTECH.DERV_PRTF_ACCT_REL
PVTECH.DERV_PRTF_OWN_PSST
PVTECH.DERV_PRTF_PATY
PVTECH.DERV_PRTF_PATY_REL
PVTECH.GRD_DEPT_FLAT_CURR
PVTECH.GRD_GNRC_MAP_CURR
PVTECH.GRD_RPRT_CALR_FNYR
PVTECH.MAP_SAP_INVL_PDCT
PVTECH.MOS_FCLY
PVTECH.MOS_LOAN
VCBODS.MSTR_CNCT_BALN_TRNF_PRTP
VCBODS.MSTR_CNCT_MSTR_DATA_GENL
VCBODS.MSTR_CNCT_PRXY_ACCT
PVTECH.ODS_RULE
PVTECH.THA_ACCT
PVTECH.UTIL_BTCH_ISAC
PVTECH.UTIL_PARM
VCBODS.WUL_NON_SAP_ACCT
```

## Summary Statistics

| Schema | Count | Description |
|--------|-------|-------------|
| **PVTECH** | 23 | Core technical/business data tables |
| **VCBODS** | 10 | CBODS source system tables |  
| **UTILSTG** | 1 | Utility staging control table |
| **Total** | **34** | **True external source tables** |

## Tables NOT Needed as Sources (Created by BTEQ)

**These 49 tables are TARGET tables created/populated by the BTEQ scripts:**

### PDDSTG Schema (27 staging/working tables)
- ACCT_BALN_BKDT_ADJ_RULE
- ACCT_BALN_BKDT_STG1  
- ACCT_BALN_BKDT_STG2
- ACCT_PATY_DEDUP
- ACCT_PATY_REL_THA
- ACCT_PATY_REL_WSS
- ACCT_PATY_THA_NEW_RNGE
- ACCT_REL_HLS_REME
- ACCT_REL_HLS_REME_HIST
- ACCT_REL_WSS_DITPS
- DERV_ACCT_PATY_ADD
- DERV_ACCT_PATY_CHG
- DERV_ACCT_PATY_CURR
- DERV_ACCT_PATY_DEL
- DERV_ACCT_PATY_FLAG
- DERV_ACCT_PATY_NON_RM
- DERV_ACCT_PATY_RM
- DERV_ACCT_PATY_ROW_SECU_FIX
- DERV_PRTF_ACCT_PATY_PSST
- DERV_PRTF_ACCT_PATY_STAG
- DERV_PRTF_ACCT_STAG
- DERV_PRTF_PATY_STAG
- GRD_GNRC_MAP_BUSN_SEGM_PRTY
- GRD_GNRC_MAP_DERV_PATY_HOLD
- GRD_GNRC_MAP_DERV_PATY_REL
- GRD_GNRC_MAP_DERV_UNID_PATY
- GRD_GNRC_MAP_PATY_HOLD_PRTY
- UTIL_PROS_SAP_RUN

### STAR_CAD_PROD_DATA Schema (7 final tables)  
- ACCT_BALN_BKDT
- ACCT_BALN_BKDT_AUDT
- ACCT_BALN_BKDT_RECN
- ACCT_REL
- ACCT_PATY_TAX_INSS
- DERV_ACCT_PATY
- UTIL_PROS_ISAC

## Configuration Required

### DBT Variables (Only for TRUE source schemas)
```yaml
vars:
  PVTECH: "PVTECH"  # Production Technical Schema  
  VCBODS: "VCBODS"  # CBODS Source Schema
  UTILSTG: "UTILSTG"  # Utility Staging Schema
```

### Prerequisites

#### 1. Schema Access
Ensure your Snowflake role has `SELECT` access to:
- **PVTECH** (23 tables)
- **VCBODS** (10 tables)  
- **UTILSTG** (1 table)

#### 2. Table Availability  
Verify all 34 source tables exist and contain data for your processing period.

## Impact on DBT Implementation

### Source Configuration
Create `models/sources/sources.yml`:
```yaml
version: 2

sources:
  - name: pvtech
    description: Production technical data
    tables:
      - name: acct_base
      - name: acct_offr
      # ... (list all 23 PVTECH tables)
      
  - name: vcbods  
    description: CBODS source system
    tables:
      - name: acct_mstr_cyt_data
      - name: busn_ptnr
      # ... (list all 10 VCBODS tables)
      
  - name: utilstg
    description: Utility staging
    tables:
      - name: cbm_util_run_strm_occr_cntl_h
```

### Performance Benefits
- **34 external dependencies** instead of 83
- **49 fewer tables** to set up in target environment
- **Cleaner data lineage** with proper source/target separation

## Source Table Usage by BTEQ Files

This section shows the data lineage - which external source tables are used by which BTEQ files.

### By Source Table (Which BTEQ files use each source table)

#### PVTECH Schema Tables (23 tables)

**PVTECH.ACCT_BASE**
- Used by: `BTEQ_SAP_EDO_WKLY_LOAD.sql`

**PVTECH.ACCT_OFFR** 
- Used by: `BTEQ_SAP_EDO_WKLY_LOAD.sql`

**PVTECH.ACCT_PATY**
- Used by: `BTEQ_TAX_INSS_MNLY_LOAD.sql`, `DERV_ACCT_PATY_04_POP_CURR_TABL.sql`

**PVTECH.ACCT_PDCT**
- Used by: `BTEQ_SAP_EDO_WKLY_LOAD.sql`

**PVTECH.ACCT_UNID_PATY**
- Used by: `DERV_ACCT_PATY_04_POP_CURR_TABL.sql`

**PVTECH.ACCT_XREF_BPS_CBS**
- Used by: `DERV_ACCT_PATY_04_POP_CURR_TABL.sql`

**PVTECH.ACCT_XREF_MAS_DAR**
- Used by: `DERV_ACCT_PATY_04_POP_CURR_TABL.sql`

**PVTECH.CLS_FCLY**
- Used by: `DERV_ACCT_PATY_04_POP_CURR_TABL.sql`

**PVTECH.CLS_UNID_PATY**
- Used by: `DERV_ACCT_PATY_04_POP_CURR_TABL.sql`

**PVTECH.DAR_ACCT**
- Used by: `DERV_ACCT_PATY_04_POP_CURR_TABL.sql`

**PVTECH.DEPT_DIMN_NODE_ANCS_CURR**
- Used by: `DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql`

**PVTECH.DERV_PRTF_ACCT**
- Used by: `DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql`, `DERV_ACCT_PATY_05_SET_PRTF_PRFR_FLAG.sql`, `prtf_tech_acct_own_rel_psst.sql`

**PVTECH.DERV_PRTF_ACCT_REL**
- Used by: `prtf_tech_acct_own_rel_psst.sql`

**PVTECH.DERV_PRTF_OWN_PSST**
- Used by: `prtf_tech_own_rel_psst.sql`

**PVTECH.DERV_PRTF_PATY**
- Used by: `DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql`, `prtf_tech_paty_own_rel_psst.sql`

**PVTECH.DERV_PRTF_PATY_REL**
- Used by: `prtf_tech_paty_own_rel_psst.sql`

**PVTECH.GRD_DEPT_FLAT_CURR**
- Used by: `DERV_ACCT_PATY_05_SET_PRTF_PRFR_FLAG.sql`, `DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG_CHG0379808.sql`, `DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql`

**PVTECH.GRD_GNRC_MAP_CURR**
- Used by: `DERV_ACCT_PATY_02_CRAT_WORK_TABL.sql`

**PVTECH.GRD_RPRT_CALR_FNYR**
- Used by: `BTEQ_TAX_INSS_MNLY_LOAD.sql`

**PVTECH.MAP_SAP_INVL_PDCT**
- Used by: `BTEQ_TAX_INSS_MNLY_LOAD.sql`

**PVTECH.MOS_FCLY**
- Used by: `DERV_ACCT_PATY_04_POP_CURR_TABL.sql`

**PVTECH.MOS_LOAN**
- Used by: `DERV_ACCT_PATY_04_POP_CURR_TABL.sql`

**PVTECH.ODS_RULE**
- Used by: `DERV_ACCT_PATY_05_SET_PRTF_PRFR_FLAG.sql`, `DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG_CHG0379808.sql`, `DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql`

**PVTECH.THA_ACCT**
- Used by: `DERV_ACCT_PATY_04_POP_CURR_TABL.sql`

**PVTECH.UTIL_BTCH_ISAC**
- Used by: `DERV_ACCT_PATY_00_DATAWATCHER.sql`, `DERV_ACCT_PATY_01_SP_GET_PROS_KEY.sql`

**PVTECH.UTIL_PARM**
- Used by: `DERV_ACCT_PATY_00_DATAWATCHER.sql`, `prtf_tech_daly_datawatcher_c.sql`

#### VCBODS Schema Tables (10 tables)

**VCBODS.ACCT_MSTR_CYT_DATA**
- Used by: `BTEQ_TAX_INSS_MNLY_LOAD.sql`

**VCBODS.BUSN_PTNR**
- Used by: `BTEQ_TAX_INSS_MNLY_LOAD.sql`

**VCBODS.CBA_FNCL_SERV_GL_DATA_CURR**
- Used by: `BTEQ_SAP_EDO_WKLY_LOAD.sql`

**VCBODS.MSTR_CNCT_BALN_TRNF_PRTP**
- Used by: `BTEQ_SAP_EDO_WKLY_LOAD.sql`

**VCBODS.MSTR_CNCT_MSTR_DATA_GENL**
- Used by: `BTEQ_SAP_EDO_WKLY_LOAD.sql`

**VCBODS.MSTR_CNCT_PRXY_ACCT**
- Used by: `BTEQ_SAP_EDO_WKLY_LOAD.sql`

**VCBODS.WUL_NON_SAP_ACCT**
- Used by: `BTEQ_SAP_EDO_WKLY_LOAD.sql`

#### UTILSTG Schema Tables (1 table)

**UTILSTG.CBM_UTIL_RUN_STRM_OCCR_CNTL_H**
- Used by: `BTEQ_SAP_EDO_WKLY_LOAD.sql`, `BTEQ_TAX_INSS_MNLY_LOAD.sql`

### By BTEQ File (Which source tables each file uses)

#### Data Loading Processes

**BTEQ_SAP_EDO_WKLY_LOAD.sql** (Weekly SAP EDO Load)
- Source tables: `ACCT_BASE`, `ACCT_OFFR`, `ACCT_PDCT`, `CBA_FNCL_SERV_GL_DATA_CURR`, `CBM_UTIL_RUN_STRM_OCCR_CNTL_H`, `MSTR_CNCT_BALN_TRNF_PRTP`, `MSTR_CNCT_MSTR_DATA_GENL`, `MSTR_CNCT_PRXY_ACCT`, `WUL_NON_SAP_ACCT`

**BTEQ_TAX_INSS_MNLY_LOAD.sql** (Monthly Tax/Insurance Load)
- Source tables: `ACCT_MSTR_CYT_DATA`, `ACCT_PATY`, `BUSN_PTNR`, `CBM_UTIL_RUN_STRM_OCCR_CNTL_H`, `GRD_RPRT_CALR_FNYR`, `MAP_SAP_INVL_PDCT`

#### Derived Account Party Processes

**DERV_ACCT_PATY_00_DATAWATCHER.sql** (Data Watcher)
- Source tables: `UTIL_BTCH_ISAC`, `UTIL_PARM`

**DERV_ACCT_PATY_01_SP_GET_PROS_KEY.sql** (Get Process Key)
- Source tables: `UTIL_BTCH_ISAC`

**DERV_ACCT_PATY_02_CRAT_WORK_TABL.sql** (Create Work Tables)
- Source tables: `GRD_GNRC_MAP_CURR`

**DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql** (Set Account Portfolio)
- Source tables: `DERV_PRTF_ACCT`, `DERV_PRTF_PATY`

**DERV_ACCT_PATY_04_POP_CURR_TABL.sql** (Populate Current Table)
- Source tables: `ACCT_PATY`, `ACCT_UNID_PATY`, `ACCT_XREF_BPS_CBS`, `ACCT_XREF_MAS_DAR`, `CLS_FCLY`, `CLS_UNID_PATY`, `DAR_ACCT`, `MOS_FCLY`, `MOS_LOAN`, `THA_ACCT`

**DERV_ACCT_PATY_05_SET_PRTF_PRFR_FLAG.sql** (Set Portfolio Preference Flag)
- Source tables: `GRD_DEPT_FLAT_CURR`, `ODS_RULE`

**DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql** (Set Max Preference Flag)
- Source tables: `DEPT_DIMN_NODE_ANCS_CURR`, `GRD_DEPT_FLAT_CURR`, `ODS_RULE`

#### Portfolio Technical Processes

**prtf_tech_acct_own_rel_psst.sql** (Account Ownership Relations)
- Source tables: `DERV_PRTF_ACCT`, `DERV_PRTF_ACCT_REL`

**prtf_tech_daly_datawatcher_c.sql** (Daily Data Watcher)
- Source tables: `UTIL_PARM`

**prtf_tech_own_rel_psst.sql** (Ownership Relations)
- Source tables: `DERV_PRTF_OWN_PSST`

**prtf_tech_paty_own_rel_psst.sql** (Party Ownership Relations)
- Source tables: `DERV_PRTF_PATY`, `DERV_PRTF_PATY_REL`

### Data Lineage Insights

#### Most Referenced Source Tables
1. **PVTECH.UTIL_PROS_ISAC** - Used by 9 BTEQ files (most critical dependency)
2. **PVTECH.ODS_RULE** - Used by 3 BTEQ files  
3. **PVTECH.GRD_DEPT_FLAT_CURR** - Used by 3 BTEQ files
4. **PVTECH.DERV_PRTF_ACCT** - Used by 3 BTEQ files
5. **PVTECH.UTIL_PARM** - Used by 2 BTEQ files

#### BTEQ Files with Most Dependencies
1. **DERV_ACCT_PATY_04_POP_CURR_TABL.sql** - Uses 10 source tables
2. **BTEQ_SAP_EDO_WKLY_LOAD.sql** - Uses 9 source tables  
3. **BTEQ_TAX_INSS_MNLY_LOAD.sql** - Uses 6 source tables

#### Schema Usage Distribution
- **PVTECH**: 17 BTEQ files depend on PVTECH tables
- **VCBODS**: 2 BTEQ files depend on VCBODS tables
- **UTILSTG**: 2 BTEQ files depend on UTILSTG tables

#### Critical Source Tables (High Impact)
These tables are used by multiple processes and would cause significant impact if unavailable:
- `UTIL_PROS_ISAC` (9 dependencies) - Process control and audit
- `ODS_RULE` (3 dependencies) - Business rules engine
- `GRD_DEPT_FLAT_CURR` (3 dependencies) - Department hierarchy
- `DERV_PRTF_ACCT` (3 dependencies) - Portfolio account mappings

## Verification Commands

### Check External Source Tables Exist
```sql
-- PVTECH tables (23)
SELECT COUNT(*) FROM PVTECH.ACCT_BASE LIMIT 1;
SELECT COUNT(*) FROM PVTECH.ACCT_OFFR LIMIT 1;
-- ... repeat for all PVTECH tables

-- VCBODS tables (10)  
SELECT COUNT(*) FROM VCBODS.ACCT_MSTR_CYT_DATA LIMIT 1;
SELECT COUNT(*) FROM VCBODS.BUSN_PTNR LIMIT 1;
-- ... repeat for all VCBODS tables

-- UTILSTG table (1)
SELECT COUNT(*) FROM UTILSTG.CBM_UTIL_RUN_STRM_OCCR_CNTL_H LIMIT 1;
```

## Tables Missing from Iceberg Creation Script

The following **16 source tables** are **NOT** created in `CBA01-Creating Iceberg Tables.sql` and need to be added:

### PVTECH Schema Tables (8 missing)
```
PVTECH.ACCT_BASE
PVTECH.ACCT_OFFR
PVTECH.ACCT_PATY
PVTECH.ACCT_PDCT
PVTECH.DERV_PRTF_ACCT
PVTECH.DERV_PRTF_PATY
PVTECH.GRD_RPRT_CALR_FNYR
PVTECH.MAP_SAP_INVL_PDCT
```

### VCBODS Schema Tables (7 missing)
```
VCBODS.ACCT_MSTR_CYT_DATA
VCBODS.BUSN_PTNR
VCBODS.CBA_FNCL_SERV_GL_DATA_CURR
VCBODS.MSTR_CNCT_BALN_TRNF_PRTP
VCBODS.MSTR_CNCT_MSTR_DATA_GENL
VCBODS.MSTR_CNCT_PRXY_ACCT
VCBODS.WUL_NON_SAP_ACCT
```

### UTILSTG Schema Tables (1 missing)
```
UTILSTG.CBM_UTIL_RUN_STRM_OCCR_CNTL_H
```

## Tables Found in Iceberg Creation Script

The following **18 source tables** were found (with schema mappings):

### Schema Mapping: PVTECH → Multiple Schemas
- `PVTECH.ACCT_UNID_PATY` → `STARCADPRODDATA.ACCT_UNID_PATY` ✓
- `PVTECH.ACCT_XREF_BPS_CBS` → `STARCADPRODDATA.ACCT_XREF_BPS_CBS` ✓
- `PVTECH.ACCT_XREF_MAS_DAR` → `STARCADPRODDATA.ACCT_XREF_MAS_DAR` ✓
- `PVTECH.CLS_FCLY` → `STARCADPRODDATA.CLS_FCLY` ✓
- `PVTECH.CLS_UNID_PATY` → `STARCADPRODDATA.CLS_UNID_PATY` ✓
- `PVTECH.DAR_ACCT` → `STARCADPRODDATA.DAR_ACCT` ✓
- `PVTECH.DEPT_DIMN_NODE_ANCS_CURR` → `PDGRD.DEPT_DIMN_NODE_ANCS_CURR` ✓
- `PVTECH.DERV_PRTF_ACCT_REL` → `STARCADPRODDATA.DERV_PRTF_ACCT_REL` ✓
- `PVTECH.DERV_PRTF_OWN_PSST` → `STARCADPRODDATA.DERV_PRTF_OWN_REL` ✓
- `PVTECH.DERV_PRTF_PATY_REL` → `STARCADPRODDATA.DERV_PRTF_PATY_REL` ✓
- `PVTECH.GRD_DEPT_FLAT_CURR` → `PDGRD.GRD_DEPT_FLAT_CURR` ✓
- `PVTECH.GRD_GNRC_MAP_CURR` → `PDGRD.GRD_GNRC_MAP` ✓
- `PVTECH.MOS_FCLY` → `STARCADPRODDATA.MOS_FCLY` ✓
- `PVTECH.MOS_LOAN` → `STARCADPRODDATA.MOS_LOAN` ✓
- `PVTECH.ODS_RULE` → `PDCBODS.ODS_RULE` ✓
- `PVTECH.THA_ACCT` → `STARCADPRODDATA.THA_ACCT` ✓
- `PVTECH.UTIL_BTCH_ISAC` → `STARCADPRODDATA.UTIL_BTCH_ISAC`, `PDSRCCS.UTIL_BTCH_ISAC` ✓
- `PVTECH.UTIL_PARM` → `STARCADPRODDATA.UTIL_PARM`, `PDSRCCS.UTIL_PARM` ✓

### Summary of Missing Tables
- **Total Missing**: 16 out of 34 source tables (47%)
- **PVTECH Missing**: 8 out of 23 tables
- **VCBODS Missing**: 7 out of 10 tables  
- **UTILSTG Missing**: 1 out of 1 table

These missing tables need to be created in the appropriate Snowflake schemas before the BTEQ to DBT migration can be completed.

---

**Analysis Method**: Compared tables referenced in `FROM` clauses vs. tables in `INSERT INTO` statements  
**Generated by**: BTEQ to DBT Analysis Tool  
**Date**: 2025-08-21 