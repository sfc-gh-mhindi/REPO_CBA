# GDW1 DBT Project - Output Tables and Locations

## Overview

This document details all tables that the GDW1 BTEQ Migration DBT project will create (write) and their exact locations based on the current `dbt_project.yml` configuration.

## Current Database Configuration

### Database Variables (from dbt_project.yml)
- **source_database**: `NPD_D12_DMN_GDWMIG_IBRG_V` (for reading)
- **intermediate_database**: `NPD_D12_DMN_GDWMIG_IBRG_V` (for intermediate tables)
- **target_database**: `NPD_D12_DMN_GDWMIG_IBRG` (for marts/final tables)
- **dcf_database**: `NPD_D12_DMN_GDWMIG_IBRG_V` (for DCF framework)

### Schema Variables (from dbt_project.yml)
- **intermediate_schema**: `P_D_DCF_001_STD_0`
- **target_schema**: `P_D_GDW_001_STD_0`

## Tables Created by DBT Project

### 1. Intermediate Layer Tables (Business Logic Processing)

**Location**: `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0`
**Materialization**: `table` (physical tables)

| Model File | Table Name | Full Table Location | Purpose |
|------------|------------|---------------------|---------|
| `int_acct_baln_bkdt_insert.sql` | `int_acct_baln_bkdt_insert` | `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.int_acct_baln_bkdt_insert` | Account balance insert processing |
| `int_acct_baln_bkdt_cleanup.sql` | `int_acct_baln_bkdt_cleanup` | `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.int_acct_baln_bkdt_cleanup` | Account balance cleanup logic |
| `int_acct_baln_monthly_avg_calc.sql` | `int_acct_baln_monthly_avg_calc` | `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.int_acct_baln_monthly_avg_calc` | Monthly average calculation |
| `int_derv_acct_paty_portfolio_setup.sql` | `int_derv_acct_paty_portfolio_setup` | `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.int_derv_acct_paty_portfolio_setup` | Portfolio setup processing |
| `int_sap_edo_weekly_reconciliation.sql` | `int_sap_edo_weekly_reconciliation` | `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.int_sap_edo_weekly_reconciliation` | SAP EDO reconciliation |

### 2. Marts Layer Tables (Final Production Tables)

**Location**: `NPD_D12_DMN_GDWMIG_IBRG.P_D_GDW_001_STD_0`
**Materialization**: `table` (physical tables)

| Model File | Table Name | Full Table Location | Purpose |
|------------|------------|---------------------|---------|
| `mart_acct_baln_bkdt.sql` | `mart_acct_baln_bkdt` | `NPD_D12_DMN_GDWMIG_IBRG.P_D_GDW_001_STD_0.mart_acct_baln_bkdt` | Final account balance mart |

## Table Categories by Business Function

### Account Balance Processing
**Intermediate Tables** (3):
- `int_acct_baln_bkdt_insert` - Insert logic
- `int_acct_baln_bkdt_cleanup` - Cleanup logic  
- `int_acct_baln_monthly_avg_calc` - Monthly calculations

**Marts Tables** (1):
- `mart_acct_baln_bkdt` - Final production table

### Derived Account Party Processing
**Intermediate Tables** (1):
- `int_derv_acct_paty_portfolio_setup` - Portfolio setup

**Marts Tables**: None yet (would be added in future development)

### Portfolio Technical Processing
**Intermediate Tables** (1):
- `int_sap_edo_weekly_reconciliation` - SAP EDO reconciliation

**Marts Tables**: None yet (would be added in future development)

## Database Permissions Required

### For Intermediate Tables
**Database**: `NPD_D12_DMN_GDWMIG_IBRG_V`
**Schema**: `P_D_DCF_001_STD_0`
**Permissions Needed**:
- `CREATE TABLE`
- `INSERT`
- `SELECT`
- `UPDATE`
- `DELETE`
- `DROP TABLE` (for full refresh)

### For Marts Tables  
**Database**: `NPD_D12_DMN_GDWMIG_IBRG`
**Schema**: `P_D_GDW_001_STD_0`
**Permissions Needed**:
- `CREATE TABLE`
- `INSERT`
- `SELECT`
- `UPDATE`
- `DELETE`
- `DROP TABLE` (for full refresh)

## Table Naming Convention

### Pattern
- **Intermediate**: `int_<functional_area>_<specific_purpose>`
- **Marts**: `mart_<functional_area>_<table_purpose>`

### Examples
- `int_acct_baln_bkdt_insert` = Intermediate + Account Balance + Backdate + Insert
- `mart_acct_baln_bkdt` = Mart + Account Balance + Backdate

## Materialization Strategy

### Intermediate Layer
- **Type**: Physical tables (`materialized: table`)
- **Refresh**: Full refresh by default (can be configured for incremental)
- **Purpose**: Store business logic transformations for debugging and lineage

### Marts Layer
- **Type**: Physical tables (`materialized: table`)
- **Refresh**: Full refresh by default (can be configured for incremental)
- **Purpose**: Final production tables for consumption

## Runtime Behavior

### On `dbt run`:
1. **Intermediate tables** are created/refreshed in `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0`
2. **Marts tables** are created/refreshed in `NPD_D12_DMN_GDWMIG_IBRG.P_D_GDW_001_STD_0`
3. Tables are created with DCF audit columns and process tracking

### On `dbt run --full-refresh`:
1. All existing tables are **dropped** and **recreated**
2. Fresh data load from source systems

## Additional Tables (Not Yet Created)

### Missing Marts Tables
Based on the intermediate models, these marts tables should be created:

| Proposed Table | Location | Purpose |
|----------------|----------|---------|
| `mart_derv_acct_paty` | `NPD_D12_DMN_GDWMIG_IBRG.P_D_GDW_001_STD_0.mart_derv_acct_paty` | Derived account party final table |
| `mart_sap_edo_reconciliation` | `NPD_D12_DMN_GDWMIG_IBRG.P_D_GDW_001_STD_0.mart_sap_edo_reconciliation` | SAP EDO reconciliation final table |

### Future Intermediate Tables
For remaining BTEQ files not yet converted:

| Category | Estimated Tables | Location |
|----------|------------------|----------|
| Account Balance | +7 tables | `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.*` |
| Derived Party | +8 tables | `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.*` |
| Portfolio Tech | +10 tables | `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.*` |
| Process Control | +3 tables | `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.*` |

## Validation Queries

### Check Current Tables
```sql
-- Check intermediate tables
SELECT table_name, created, last_altered 
FROM NPD_D12_DMN_GDWMIG_IBRG_V.information_schema.tables
WHERE table_schema = 'P_D_DCF_001_STD_0'
  AND table_name LIKE 'int_%'
ORDER BY table_name;

-- Check marts tables  
SELECT table_name, created, last_altered
FROM NPD_D12_DMN_GDWMIG_IBRG.information_schema.tables  
WHERE table_schema = 'P_D_GDW_001_STD_0'
  AND table_name LIKE 'mart_%'
ORDER BY table_name;
```

### Check Table Row Counts
```sql
-- Intermediate table sizes
SELECT 
    'int_acct_baln_bkdt_insert' as table_name,
    COUNT(*) as row_count
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0.int_acct_baln_bkdt_insert
UNION ALL
SELECT 
    'mart_acct_baln_bkdt' as table_name,
    COUNT(*) as row_count  
FROM NPD_D12_DMN_GDWMIG_IBRG.P_D_GDW_001_STD_0.mart_acct_baln_bkdt;
```

## Summary

### Current Output (6 Tables Total)
- **5 Intermediate Tables** in `NPD_D12_DMN_GDWMIG_IBRG_V.P_D_DCF_001_STD_0`
- **1 Marts Table** in `NPD_D12_DMN_GDWMIG_IBRG.P_D_GDW_001_STD_0`

### Database Structure
- **Intermediate Database**: `NPD_D12_DMN_GDWMIG_IBRG_V` (with `_V` suffix)
- **Production Database**: `NPD_D12_DMN_GDWMIG_IBRG` (without `_V` suffix)
- **Schema Separation**: DCF schema for intermediate, GDW schema for marts

### Key Configuration Points
- ✅ **Proper separation** between intermediate and production databases
- ✅ **Consistent naming** following DBT and DCF patterns
- ✅ **Physical tables** for both intermediate and marts (better for debugging)
- ✅ **DCF integration** with audit columns and process tracking

The configuration follows best practices with clear separation between processing (intermediate) and consumption (marts) layers. 