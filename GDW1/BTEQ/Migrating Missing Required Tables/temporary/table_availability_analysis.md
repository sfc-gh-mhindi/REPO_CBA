# Table Availability Analysis Report

## Summary
- **Total Tables Analyzed:** 46
- **Available in K_ Databases:** 14 (30.4%)
- **Missing from K_ Databases:** 32 (69.6%)
- **Analysis Date:** 2025-08-25 11:41:24

## Schema Transformation Mappings
The following schema transformations were applied based on `schema_replacements.json`:

| Original Schema | Transformed Schema | Status |
|---|---|---|
| `STARCADPRODDATA` | `K_STAR_CAD_PROD_DATA` | ✅ Active |
| `PDCBODS` | `K_PDCBODS` | ✅ Active |
| `PDGRD` | `K_PDGRD` | ✅ Active |
| `PDDSTG` | `K_PDDSTG` | ✅ Active |
| `PP01STARCADPRODDATA` | `K_P_P01_STAR_CAD_PROD_DATA` | ✅ Active |
| `PDPATY` | `K_PDPATY` | ✅ Active |
| `PDSECURITY` | `K_PDSECURITY` | ✅ Active |
| `PDCBSTG` | `K_PDCBSTG` | ✅ Active |
| `UDTDWRK` | `K_UDTDWRK` | ✅ Active |
| `PTEMP` | `PTEMP` | ➡️ No Change |


## Detailed Table Analysis

### ✅ Available Tables (14 tables)
These tables exist in the K_ databases:

| Original Schema | Table Name | Transformed Schema | Transformed Full Name | Schema Changed |
|---|---|---|---|---|
| `PDCBODS` | `ACCT_MSTR_CYT_DATA` | `K_PDCBODS` | `K_PDCBODS.ACCT_MSTR_CYT_DATA` | 🔄 |
| `PDCBODS` | `BUSN_PTNR` | `K_PDCBODS` | `K_PDCBODS.BUSN_PTNR` | 🔄 |
| `PDCBODS` | `CBA_FNCL_SERV_GL_DATA` | `K_PDCBODS` | `K_PDCBODS.CBA_FNCL_SERV_GL_DATA` | 🔄 |
| `PDCBODS` | `MSTR_CNCT_BALN_TRNF_PRTP` | `K_PDCBODS` | `K_PDCBODS.MSTR_CNCT_BALN_TRNF_PRTP` | 🔄 |
| `PDCBODS` | `MSTR_CNCT_MSTR_DATA_GENL` | `K_PDCBODS` | `K_PDCBODS.MSTR_CNCT_MSTR_DATA_GENL` | 🔄 |
| `PDCBODS` | `MSTR_CNCT_PRXY_ACCT` | `K_PDCBODS` | `K_PDCBODS.MSTR_CNCT_PRXY_ACCT` | 🔄 |
| `PDGRD` | `GRD_RPRT_CALR_FNYR` | `K_PDGRD` | `K_PDGRD.GRD_RPRT_CALR_FNYR` | 🔄 |
| `PDPATY` | `ACCT_PATY` | `K_PDPATY` | `K_PDPATY.ACCT_PATY` | 🔄 |
| `PDSECURITY` | `ROW_LEVL_SECU_USER_PRFL` | `K_PDSECURITY` | `K_PDSECURITY.ROW_LEVL_SECU_USER_PRFL` | 🔄 |
| `STARCADPRODDATA` | `ACCT_BASE` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.ACCT_BASE` | 🔄 |
| `STARCADPRODDATA` | `ACCT_OFFR` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.ACCT_OFFR` | 🔄 |
| `STARCADPRODDATA` | `ACCT_PDCT` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.ACCT_PDCT` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_ACCT_REL` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_REL` | 🔄 |
| `STARCADPRODDATA` | `MAP_SAP_INVL_PDCT` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.MAP_SAP_INVL_PDCT` | 🔄 |


### ❌ Missing Tables (32 tables)
These tables are **NOT** found in the K_ databases:

| Original Schema | Table Name | Transformed Schema | Expected Full Name | Schema Changed |
|---|---|---|---|---|
| `PDCBSTG` | `ACCT_REL_HLS_REME` | `K_PDCBSTG` | `K_PDCBSTG.ACCT_REL_HLS_REME` | 🔄 |
| `PDCBSTG` | `ACCT_REL_HLS_REME_HIST` | `K_PDCBSTG` | `K_PDCBSTG.ACCT_REL_HLS_REME_HIST` | 🔄 |
| `PDCBSTG` | `UTIL_PROS_SAP_RUN` | `K_PDCBSTG` | `K_PDCBSTG.UTIL_PROS_SAP_RUN` | 🔄 |
| `PDDSTG` | `ACCT_BALN_BKDT_ADJ_RULE` | `K_PDDSTG` | `K_PDDSTG.ACCT_BALN_BKDT_ADJ_RULE` | 🔄 |
| `PDDSTG` | `ACCT_BALN_BKDT_STG1` | `K_PDDSTG` | `K_PDDSTG.ACCT_BALN_BKDT_STG1` | 🔄 |
| `PDDSTG` | `ACCT_BALN_BKDT_STG2` | `K_PDDSTG` | `K_PDDSTG.ACCT_BALN_BKDT_STG2` | 🔄 |
| `PDGRD` | `GRD_PRTF_TYPE_ENHC_HIST_PSST` | `K_PDGRD` | `K_PDGRD.GRD_PRTF_TYPE_ENHC_HIST_PSST` | 🔄 |
| `PDGRD` | `GRD_PRTF_TYPE_ENHC_PSST` | `K_PDGRD` | `K_PDGRD.GRD_PRTF_TYPE_ENHC_PSST` | 🔄 |
| `PDGRD` | `GRD_RPRT_CALR_CLYR` | `K_PDGRD` | `K_PDGRD.GRD_RPRT_CALR_CLYR` | 🔄 |
| `PP01STARCADPRODDATA` | `ACCT_INT_GRUP` | `K_P_P01_STAR_CAD_PROD_DATA` | `K_P_P01_STAR_CAD_PROD_DATA.ACCT_INT_GRUP` | 🔄 |
| `PTEMP` | `DERV_PRTF_INT_PSST` | `PTEMP` | `PTEMP.DERV_PRTF_INT_PSST` | ➡️ |
| `STARCADPRODDATA` | `ACCT_BALN` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.ACCT_BALN` | 🔄 |
| `STARCADPRODDATA` | `ACCT_BALN_ADJ` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.ACCT_BALN_ADJ` | 🔄 |
| `STARCADPRODDATA` | `ACCT_BALN_BKDT` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.ACCT_BALN_BKDT` | 🔄 |
| `STARCADPRODDATA` | `ACCT_BALN_BKDT_AUDT` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.ACCT_BALN_BKDT_AUDT` | 🔄 |
| `STARCADPRODDATA` | `ACCT_BALN_BKDT_RECN` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.ACCT_BALN_BKDT_RECN` | 🔄 |
| `STARCADPRODDATA` | `ACCT_INT_GRUP` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.ACCT_INT_GRUP` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_ACCT_HIST_PSST` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_HIST_PSST` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_ACCT_INT_GRUP_PSST` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_INT_GRUP_PSST` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_ACCT_OWN_REL` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_OWN_REL` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_ACCT_PSST` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_ACCT_PSST` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_INT_GRUP_ENHC_PSST` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_INT_GRUP_ENHC_PSST` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_INT_GRUP_OWN_PSST` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_INT_GRUP_OWN_PSST` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_INT_HIST_PSST` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_INT_HIST_PSST` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_INT_PSST` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_INT_PSST` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_OWN_HIST_PSST` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_OWN_HIST_PSST` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_OWN_PSST` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_OWN_PSST` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_PATY_HIST_PSST` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_PATY_HIST_PSST` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_PATY_INT_GRUP_PSST` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_PATY_INT_GRUP_PSST` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_PATY_OWN_REL` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_PATY_OWN_REL` | 🔄 |
| `STARCADPRODDATA` | `DERV_PRTF_PATY_PSST` | `K_STAR_CAD_PROD_DATA` | `K_STAR_CAD_PROD_DATA.DERV_PRTF_PATY_PSST` | 🔄 |
| `UDTDWRK` | `DERV_PRTF_ACCT_HIST_PSST` | `K_UDTDWRK` | `K_UDTDWRK.DERV_PRTF_ACCT_HIST_PSST` | 🔄 |


## Schema-wise Breakdown

| Original Schema | Total Tables | Available | Missing | Availability % |
|---|---|---|---|---|
| `PDCBODS` | 6 | 6 | 0 | 100.0% |
| `PDCBSTG` | 3 | 0 | 3 | 0.0% |
| `PDDSTG` | 3 | 0 | 3 | 0.0% |
| `PDGRD` | 4 | 1 | 3 | 25.0% |
| `PDPATY` | 1 | 1 | 0 | 100.0% |
| `PDSECURITY` | 1 | 1 | 0 | 100.0% |
| `PP01STARCADPRODDATA` | 1 | 0 | 1 | 0.0% |
| `PTEMP` | 1 | 0 | 1 | 0.0% |
| `STARCADPRODDATA` | 25 | 5 | 20 | 20.0% |
| `UDTDWRK` | 1 | 0 | 1 | 0.0% |


## Next Steps & Recommendations

### For Available Tables ✅
- **14 tables** are ready for migration/reference
- Verify data consistency between original and K_ versions
- Update connection strings to use K_ schema names

### For Missing Tables ❌  
- **32 tables** need to be created or restored in K_ databases
- Check if tables exist under different names or schemas
- Coordinate with database administrators for table creation/restoration

### Schema Transformation Summary

**Schemas with transformations applied:**
- PDCBODS → K_PDCBODS
- PDCBSTG → K_PDCBSTG
- PDDSTG → K_PDDSTG
- PDGRD → K_PDGRD
- PDPATY → K_PDPATY
- PDSECURITY → K_PDSECURITY
- PP01STARCADPRODDATA → K_P_P01_STAR_CAD_PROD_DATA
- STARCADPRODDATA → K_STAR_CAD_PROD_DATA
- UDTDWRK → K_UDTDWRK

**Schemas unchanged:**
- PTEMP


---
*Generated by Table Availability Analyzer*
