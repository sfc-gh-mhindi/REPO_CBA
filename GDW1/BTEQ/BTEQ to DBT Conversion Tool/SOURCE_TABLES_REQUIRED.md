# Source Tables Required for BTEQ Models

## Overview

This document lists all source tables referenced in the converted BTEQ to DBT models. These tables must be available in your Snowflake environment for the DBT models to execute successfully.

**Generated**: 2025-08-21  
**Source**: Extracted from OutputTest10 intermediate models with RuntimeConfig.txt mappings  
**Total Tables**: 83 tables across 7 schemas

## Complete Source Table List (Database.Table Format)
*Updated with actual production database names from RuntimeConfig.txt*

**All 83 source tables required for BTEQ model execution:**

```
PDDSTG.ACCT_BALN_BKDT_ADJ_RULE
PDDSTG.ACCT_BALN_BKDT_STG1
PDDSTG.ACCT_BALN_BKDT_STG2
PDDSTG.ACCT_PATY_DEDUP
PDDSTG.ACCT_PATY_REL_THA
PDDSTG.ACCT_PATY_REL_WSS
PDDSTG.ACCT_PATY_THA_NEW_RNGE
PDDSTG.ACCT_REL_HLS_REME
PDDSTG.ACCT_REL_WSS_DITPS
PDDSTG.DERV_ACCT_PATY_ADD
PDDSTG.DERV_ACCT_PATY_CHG
PDDSTG.DERV_ACCT_PATY_CURR
PDDSTG.DERV_ACCT_PATY_DEL
PDDSTG.DERV_ACCT_PATY_FLAG
PDDSTG.DERV_ACCT_PATY_NON_RM
PDDSTG.DERV_ACCT_PATY_RM
PDDSTG.DERV_ACCT_PATY_ROW_SECU_FIX
PDDSTG.DERV_PRTF_ACCT_PATY_PSST
PDDSTG.DERV_PRTF_ACCT_PATY_STAG
PDDSTG.DERV_PRTF_ACCT_STAG
PDDSTG.DERV_PRTF_PATY_STAG
PDDSTG.GRD_GNRC_MAP_BUSN_SEGM_PRTY
PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD
PDDSTG.GRD_GNRC_MAP_DERV_PATY_REL
PDDSTG.GRD_GNRC_MAP_DERV_UNID_PATY
PDDSTG.GRD_GNRC_MAP_PATY_HOLD_PRTY
PDDSTG.UTIL_PROS_SAP_RUN
STAR_CAD_PROD_DATA.DERV_ACCT_PATY
UTILSTG.CBM_UTIL_RUN_STRM_OCCR_CNTL_H
VCBODS.ACCT_MSTR_CYT_DATA
VCBODS.ACCT_MSTR_DATA
VCBODS.ACCT_MSTR_DATA_CURR
VCBODS.BUSN_PTNR
VCBODS.CBA_FNCL_SERV_GL_DATA_CURR
VCBODS.CNCT_HEAD
VCBODS.CNCT_HEAD_CURR
VCBODS.MSTR_CNCT_BALN_TRNF_PRTP
VCBODS.MSTR_CNCT_MSTR_DATA_GENL
VCBODS.MSTR_CNCT_MSTR_DATA_GENL_CURR
VCBODS.MSTR_CNCT_PRXY_ACCT
VCBODS.WUL_NON_SAP_ACCT
VEXTR.BD_SALE_PDCT
PVPATY.UTIL_PROS_ISAC
PVTECH.ACCT_BASE
PVTECH.ACCT_OFFR
PVTECH.ACCT_PATY
PVTECH.ACCT_PATY_TAX_INSS
PVTECH.ACCT_PDCT
PVTECH.ACCT_REL
PVTECH.ACCT_UNID_PATY
PVTECH.ACCT_XREF_BPS_CBS
PVTECH.ACCT_XREF_MAS_DAR
PVTECH.CLS_FCLY
PVTECH.CLS_UNID_PATY
PVTECH.DAR_ACCT
PVTECH.DEPT_DIMN_NODE_ANCS_CURR
PVTECH.DERV_ACCT_PATY
PVTECH.DERV_PRTF_ACCT
PVTECH.DERV_PRTF_ACCT_REL
PVTECH.DERV_PRTF_INT_PSST
PVTECH.DERV_PRTF_OWN_PSST
PVTECH.DERV_PRTF_OWN_REL
PVTECH.DERV_PRTF_PATY
PVTECH.DERV_PRTF_PATY_REL
PVTECH.GRD_DEPT_FLAT_CURR
PVTECH.GRD_GNRC_MAP_CURR
PVTECH.GRD_GNRC_MAP_DERV_PATY_HOLD
PVTECH.GRD_GNRC_MAP_DERV_PATY_REL
PVTECH.GRD_GNRC_MAP_DERV_UNID_PATY
PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST
PVTECH.GRD_RPRT_CALR_FNYR
PVTECH.INT_GRUP
PVTECH.INT_GRUP_DEPT
PVTECH.MAP_SAP_ACCT_REL
PVTECH.MAP_SAP_INVL_PDCT
PVTECH.MOS_FCLY
PVTECH.MOS_LOAN
PVTECH.ODS_RULE
PVTECH.PATY_INT_GRUP
PVTECH.THA_ACCT
PVTECH.UTIL_BTCH_ISAC
PVTECH.UTIL_PARM
PVTECH.UTIL_PROS_ISAC
```

## Summary Statistics

| Schema | Count | Description |
|--------|-------|-------------|
| **PVTECH** | 40 | Technical/Core data tables |
| **PDDSTG** | 27 | Data staging tables |
| **VCBODS** | 12 | CBODS source system tables |
| **STAR_CAD_PROD_DATA** | 1 | Star schema data warehouse |
| **PVPATY** | 1 | Party-related utility tables |
| **VEXTR** | 1 | Extract process tables |
| **UTILSTG** | 1 | Utility staging tables |
| **Total** | **83** | **All source tables** |

## Tables Organized by Schema

### 🔶 PDDSTG (Data Staging Schema) - 27 Tables
*Intermediate staging tables for data processing*

- PDDSTG.ACCT_BALN_BKDT_ADJ_RULE
- PDDSTG.ACCT_BALN_BKDT_STG1
- PDDSTG.ACCT_BALN_BKDT_STG2
- PDDSTG.ACCT_PATY_DEDUP
- PDDSTG.ACCT_PATY_REL_THA
- PDDSTG.ACCT_PATY_REL_WSS
- PDDSTG.ACCT_PATY_THA_NEW_RNGE
- PDDSTG.ACCT_REL_HLS_REME
- PDDSTG.ACCT_REL_WSS_DITPS
- PDDSTG.DERV_ACCT_PATY_ADD
- PDDSTG.DERV_ACCT_PATY_CHG
- PDDSTG.DERV_ACCT_PATY_CURR
- PDDSTG.DERV_ACCT_PATY_DEL
- PDDSTG.DERV_ACCT_PATY_FLAG
- PDDSTG.DERV_ACCT_PATY_NON_RM
- PDDSTG.DERV_ACCT_PATY_RM
- PDDSTG.DERV_ACCT_PATY_ROW_SECU_FIX
- PDDSTG.DERV_PRTF_ACCT_PATY_PSST
- PDDSTG.DERV_PRTF_ACCT_PATY_STAG
- PDDSTG.DERV_PRTF_ACCT_STAG
- PDDSTG.DERV_PRTF_PATY_STAG
- PDDSTG.GRD_GNRC_MAP_BUSN_SEGM_PRTY
- PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD
- PDDSTG.GRD_GNRC_MAP_DERV_PATY_REL
- PDDSTG.GRD_GNRC_MAP_DERV_UNID_PATY
- PDDSTG.GRD_GNRC_MAP_PATY_HOLD_PRTY
- PDDSTG.UTIL_PROS_SAP_RUN

### 🔶 PVTECH (Technical Schema) - 40 Tables
*Core technical and business tables*

- PVTECH.ACCT_BASE
- PVTECH.ACCT_OFFR
- PVTECH.ACCT_PATY
- PVTECH.ACCT_PATY_TAX_INSS
- PVTECH.ACCT_PDCT
- PVTECH.ACCT_REL
- PVTECH.ACCT_UNID_PATY
- PVTECH.ACCT_XREF_BPS_CBS
- PVTECH.ACCT_XREF_MAS_DAR
- PVTECH.CLS_FCLY
- PVTECH.CLS_UNID_PATY
- PVTECH.DAR_ACCT
- PVTECH.DEPT_DIMN_NODE_ANCS_CURR
- PVTECH.DERV_ACCT_PATY
- PVTECH.DERV_PRTF_ACCT
- PVTECH.DERV_PRTF_ACCT_REL
- PVTECH.DERV_PRTF_INT_PSST
- PVTECH.DERV_PRTF_OWN_PSST
- PVTECH.DERV_PRTF_OWN_REL
- PVTECH.DERV_PRTF_PATY
- PVTECH.DERV_PRTF_PATY_REL
- PVTECH.GRD_DEPT_FLAT_CURR
- PVTECH.GRD_GNRC_MAP_CURR
- PVTECH.GRD_GNRC_MAP_DERV_PATY_HOLD
- PVTECH.GRD_GNRC_MAP_DERV_PATY_REL
- PVTECH.GRD_GNRC_MAP_DERV_UNID_PATY
- PVTECH.GRD_PRTF_TYPE_ENHC_HIST_PSST
- PVTECH.GRD_RPRT_CALR_FNYR
- PVTECH.INT_GRUP
- PVTECH.INT_GRUP_DEPT
- PVTECH.MAP_SAP_ACCT_REL
- PVTECH.MAP_SAP_INVL_PDCT
- PVTECH.MOS_FCLY
- PVTECH.MOS_LOAN
- PVTECH.ODS_RULE
- PVTECH.PATY_INT_GRUP
- PVTECH.THA_ACCT
- PVTECH.UTIL_BTCH_ISAC
- PVTECH.UTIL_PARM
- PVTECH.UTIL_PROS_ISAC

### 🔶 VCBODS (CBODS Schema) - 12 Tables
*CBODS source system tables*

- VCBODS.ACCT_MSTR_CYT_DATA
- VCBODS.ACCT_MSTR_DATA
- VCBODS.ACCT_MSTR_DATA_CURR
- VCBODS.BUSN_PTNR
- VCBODS.CBA_FNCL_SERV_GL_DATA_CURR
- VCBODS.CNCT_HEAD
- VCBODS.CNCT_HEAD_CURR
- VCBODS.MSTR_CNCT_BALN_TRNF_PRTP
- VCBODS.MSTR_CNCT_MSTR_DATA_GENL
- VCBODS.MSTR_CNCT_MSTR_DATA_GENL_CURR
- VCBODS.MSTR_CNCT_PRXY_ACCT
- VCBODS.WUL_NON_SAP_ACCT

### 🔶 STAR_CAD_PROD_DATA (Star Data Database) - 1 Table
*Star schema data warehouse*

- STAR_CAD_PROD_DATA.DERV_ACCT_PATY

### 🔶 PVPATY (Party Schema) - 1 Table
*Party-related utility tables*

- PVPATY.UTIL_PROS_ISAC

### 🔶 VEXTR (Extract Schema) - 1 Table
*Extract process tables*

- VEXTR.BD_SALE_PDCT

### 🔶 UTILSTG (Utility Staging Schema) - 1 Table
*Utility staging tables*

- UTILSTG.CBM_UTIL_RUN_STRM_OCCR_CNTL_H

## Configuration Required

### DBT Variables
The following variables must be configured in your `dbt_project.yml`:

```yaml
vars:
  DDSTG: "PDDSTG"  # Production Data Staging
  VTECH: "PVTECH"  # Production Technical Schema  
  VCBODS: "VCBODS"  # CBODS Source Schema
  STARDATADB: "STAR_CAD_PROD_DATA"  # Star Schema Production Data
  VPATY: "PVPATY"  # Production Party Schema
  VEXTR: "VEXTR"  # Extract Schema
  UTILSTG: "UTILSTG"  # Utility Staging Schema
```

### Data Lineage
The tables have the following general flow:
1. **Source Systems** (VCBODS, VEXTR, UTILSTG) → Raw data
2. **Technical Layer** (VTECH) → Processed business data
3. **Staging Layer** (DDSTG) → Intermediate processing
4. **Data Warehouse** (STARDATADB) → Final consolidated data

## Prerequisites

### 1. Schema Access
Ensure your Snowflake role has `SELECT` access to all schemas:
- PDDSTG (Production Data Staging)
- PVTECH (Production Technical)
- VCBODS (CBODS Source)
- STAR_CAD_PROD_DATA (Star Schema Production)
- PVPATY (Production Party)
- VEXTR (Extract)
- UTILSTG (Utility Staging)

### 2. Network Access
Verify network connectivity to all source databases/schemas.

### 3. Table Availability
Confirm all 84 tables exist and contain data for your processing period.

## Troubleshooting

### Missing Tables
If tables are missing, check:
1. **Schema mapping**: Verify `dbt_project.yml` variable configuration
2. **Permissions**: Ensure role has access to all schemas
3. **Table names**: Some tables may have different naming conventions in your environment

### Performance Considerations
- **Large Tables**: PVTECH schema has 40 tables - consider partitioning strategies
- **Staging Tables**: PDDSTG tables are intermediate - ensure adequate compute resources
- **Cross-Database Joins**: Multiple schemas may impact query performance

## Notes

- This list was automatically extracted from the converted DBT models
- Tables marked as staging (DDSTG) may be created by upstream processes
- Some tables may be views rather than physical tables in your environment
- Consider creating `sources.yml` files for each schema in your DBT project

---

**Generated by**: BTEQ to DBT Automation Tool  
**Date**: 2025-08-21  
**Version**: Enhanced with RuntimeConfig.txt integration and actual production database names 