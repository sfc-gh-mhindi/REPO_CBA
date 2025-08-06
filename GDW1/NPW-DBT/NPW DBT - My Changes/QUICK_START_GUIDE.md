# **CBA NPW-DBT Quick Start Guide**

## **🚀 Immediate Implementation Steps**

### **1. Prerequisites (5 minutes)**
```sql
-- Run in Snowflake to create required infrastructure
CREATE DATABASE IF NOT EXISTS GDW1_IBRG;
CREATE DATABASE IF NOT EXISTS GDW1_STG; 
CREATE DATABASE IF NOT EXISTS GDW1;
CREATE SCHEMA IF NOT EXISTS GDW1.cse4_ctl;
CREATE SCHEMA IF NOT EXISTS GDW1_STG.files;
CREATE SCHEMA IF NOT EXISTS GDW1_STG.datasets;
CREATE WAREHOUSE IF NOT EXISTS DBT WITH WAREHOUSE_SIZE = 'MEDIUM';
```

### **2. Import to Snowflake dbt (10 minutes)**
1. **Snowflake Console** → Projects → dbt → Create Project
2. **Import from Git:** `https://github.com/sfc-gh-mhindi/REPO_CBA.git`
3. **Subdirectory:** `GDW1/NPW-DBT`
4. **Update connection** to your Snowflake account

### **3. Essential Configuration Updates**
Update these variables in `dbt_project.yml`:
```yaml
vars:
  files_schema: files
  datasets_schema: datasets
  mart_db: GDW1_IBRG
  intermediate_db: GDW1_STG  
  stg_ctl_db: GDW1
  etl_process_dt: 20240101  # ← Update to current date
  run_stream: CSE_COM_BUS_APP_PROD
  app_release: CSEL4
```

### **4. Test Run (Sequential Execution)**
```bash
# Start with foundation
dbt seed
dbt run --select models/cse_dataload/02processkey

# If successful, continue with full pipeline
dbt run
dbt test
```

---

## **📊 Project Structure Overview**

```
NPW-DBT/
├── NPW DBT - My Changes/     # ← Your customizations & documentation
│   ├── README_NPW_DBT_Implementation_Guide.md  # ← Full guide
│   └── QUICK_START_GUIDE.md                    # ← This file
├── NPW DBT Project/          # ← Your project variations
├── models/                   # ← 80+ active dbt models
│   ├── cse_dataload/        # ← Main ETL pipeline (8 phases)
│   └── appt_pdct/           # ← Application product processing
├── translated/              # ← Legacy files (NOT used by dbt)
└── dbt_project.yml         # ← Main configuration
```

---

## **⚡ Key Execution Phases**

| Phase | Directory | Purpose | Key Models |
|-------|-----------|---------|------------|
| 1 | `02processkey` | Process management | `proskeyhash__loadgdwproskeyseq` |
| 2 | `04MappingLookupSets` | Reference data | `ldmap_cse_pack_pdct_pllkp` |
| 3 | `08extraction` | Data ingestion | `srcplappseq__extpl_app` |
| 4 | `12MappingTransformation` | Business logic | `tmpapptpdctds__xfmpl_appfrmext` |
| 5 | `14loadtotemp` | Staging | Temporary processing |
| 6 | `16transformdelta` | Change tracking | `dltappt_deptfrmtmp_appt_dept` |
| 7 | `18loadtogdw` | Final loading | `ldapptpdctupd` |
| 8 | `24processmetadata` | Monitoring | `tgt_step_stus_tbl__*` |

---

## **🔧 Common Issues & Quick Fixes**

### **Issue 1: Source table not found**
```sql
-- Check if source exists
SHOW TABLES IN DATABASE.SCHEMA;
-- Update model source reference if needed
```

### **Issue 2: Variable not found**
```bash
# Verify all variables are defined in dbt_project.yml
# Look for {{ cvar('variable_name') }} in error message
```

### **Issue 3: Permission denied**
```sql
-- Grant necessary permissions
GRANT USAGE ON DATABASE GDW1_IBRG TO ROLE your_role;
GRANT ALL ON SCHEMA GDW1_STG.files TO ROLE your_role;
```

### **Issue 4: Post-hook failure**
```bash
# Post-hooks insert data into target tables
# Ensure target tables exist and you have INSERT permissions
```

---

## **📈 Success Indicators**

✅ **Seeds loaded successfully** - Configuration data ready  
✅ **Process keys generated** - Core infrastructure working  
✅ **No rejection records** - Data quality passing  
✅ **Metadata tables populated** - Process tracking active  
✅ **Target tables loaded** - Pipeline delivering data  

---

## **📞 Getting Help**

1. **Check the full guide:** `README_NPW_DBT_Implementation_Guide.md`
2. **Review model dependencies:** Use `dbt docs generate && dbt docs serve`
3. **Monitor execution:** Check `GDW1.cse4_ctl.STEP_OCCR` table
4. **Debug mode:** Add `--debug` flag to dbt commands

---

**Next Steps:** Once basic execution works, review the full implementation guide for production deployment, performance optimization, and monitoring setup. 