# **CBA NPW-DBT Project - Comprehensive Implementation Guide**

## **📋 Project Overview**

**Project Name:** `np_projects_commbank_sf_dbt`  
**Purpose:** Commonwealth Bank Australia (CBA) data transformation and ETL processing system for migration to Snowflake  
**Scale:** 80+ SQL models, legacy Teradata-to-Snowflake migration project  
**Architecture:** Multi-layered data pipeline with process orchestration, transformation, and loading capabilities

**Project Structure:**
```
NPW-DBT/
├── NPW DBT - My Changes/     # ← Current folder (your customizations)
├── NPW DBT Project/          # ← Original project structure  
├── translated/               # ← Legacy translated files (not actively used)
├── models/                   # ← Active dbt models
├── macros/                   # ← Custom dbt macros
├── seeds/                    # ← Configuration data
└── dbt_project.yml          # ← Main project configuration
```

---

## **🏗️ Project Architecture**

### **Core Business Domains:**
1. **📱 Application Products** (`appt_pdct`) - Customer application and product management
2. **🔄 CSE Data Load** (`cse_dataload`) - Customer Service Environment data processing
3. **📊 Process Orchestration** - Stream status tracking and metadata management

### **Data Processing Layers:**

#### **1. Process Management (02processkey)**
- **Process Key Generation** - Unique identifier management for ETL processes
- **Utility Process ISAC** - Source-to-target mapping and conversion
- **Key Models:** `proskeyhash__loadgdwproskeyseq`, `util_pros_isac__loadgdwproskeyseq`

#### **2. Mapping & Lookups (04MappingLookupSets)**
- **Reference Data Management** - Product mapping and lookup tables
- **Data Standardization** - Code mapping and transformation rules
- **Key Models:** `ldmap_cse_pack_pdct_pllkp`

#### **3. Data Extraction (08extraction)**
- **Source System Integration** - CPL Business App data extraction
- **Data Ingestion** - Raw data capture from legacy systems
- **Key Models:** `srcplappseq__extpl_app`

#### **4. Mapping Transformation (12MappingTransformation)**
- **Business Rule Application** - Complex transformation logic
- **Data Quality Validation** - Rejection handling and error processing
- **Key Models:** `tmpapptpdctds__xfmpl_appfrmext`, `tgtapptpdctrejectsds__xfmpl_appfrmext`

#### **5. Temporary Loading (14loadtotemp)**
- **Staging Area Management** - Temporary table processing
- **Data Preparation** - Pre-transformation staging

#### **6. Transform Delta (16transformdelta)**
- **Change Data Capture** - Delta processing and change tracking
- **Historical Data Management** - SCD (Slowly Changing Dimension) processing
- **Key Models:** `dltappt_deptfrmtmp_appt_dept`

#### **7. Load to GDW (18loadtogdw)**
- **Final Data Loading** - Target system data insertion
- **Data Validation** - Post-load quality checks
- **Key Models:** `ldapptpdctupd`

#### **8. Process Metadata (24processmetadata)**
- **ETL Orchestration** - Process status tracking
- **Audit Trail** - Run stream monitoring and logging
- **Key Models:** `tgt_step_stus_tbl__processrunstreamstepoccrbeginandend`

---

## **💾 Database Configuration**

### **Target Environment:**
```yaml
Database: GDW1_IBRG     # Final production tables
Staging:  GDW1_STG      # Intermediate views and datasets  
Control:  GDW1          # Control tables and procedures
```

### **Key Schemas:**
- **`cse4_ctl`** - Control and metadata tables
- **`files`** - File-based datasets
- **`datasets`** - Processed data staging
- **`stage_views`** - Default dbt schema

### **Current Connection Settings:**
```yaml
Account: next_pathway_partner.us-east-1
User: npcommbank01
Role: ROLE_NPCOMMBANK01
Database: commbankdb
Warehouse: DBT
Schema: stage_views
```

---

## **🚀 Import to Snowflake Native dbt**

### **Step 1: Prerequisites**
```sql
-- Create required databases and schemas
CREATE DATABASE IF NOT EXISTS GDW1_IBRG;
CREATE DATABASE IF NOT EXISTS GDW1_STG; 
CREATE DATABASE IF NOT EXISTS GDW1;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS GDW1.cse4_ctl;
CREATE SCHEMA IF NOT EXISTS GDW1_STG.files;
CREATE SCHEMA IF NOT EXISTS GDW1_STG.datasets;
CREATE SCHEMA IF NOT EXISTS GDW1_STG.stage_views;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS DBT 
  WITH WAREHOUSE_SIZE = 'MEDIUM' 
  AUTO_SUSPEND = 300;
```

### **Step 2: Setup Snowflake Native dbt**
1. **Navigate to Snowflake Console** → Projects → dbt
2. **Create New Project** → Import from Git
3. **Repository URL:** `https://github.com/sfc-gh-mhindi/REPO_CBA.git`
4. **Subdirectory:** `GDW1/NPW-DBT`

### **Step 3: Environment Configuration**
```yaml
# Update profiles.yml in Snowflake dbt
name: np_projects_commbank_sf_dbt
version: '1.0.0'

profile: 'snowflake_profile'
outputs:
  dev:
    type: snowflake
    account: [YOUR_ACCOUNT]
    user: [YOUR_USER]
    role: [YOUR_ROLE]
    database: GDW1_STG
    warehouse: DBT
    schema: stage_views
    threads: 4
```

### **Step 4: Required Variables Configuration**
```yaml
# Add to dbt_project.yml vars section
vars:
  files_schema: files
  datasets_schema: datasets
  mart_db: GDW1_IBRG
  intermediate_db: GDW1_STG  
  stg_ctl_db: GDW1
  etl_process_dt_tbl: YYYYMMDD
  
  # Job-specific parameters (update as needed)
  base_dir: cba_app__csel4__prod
  run_stream: CSE_COM_BUS_APP_PROD
  etl_process_dt: 20240101
  app_release: CSEL4
  gdw_acct_db: STAR_CAD_PROD_DATA
  
  # Environment-specific settings
  envs:
    env_apt_startup_status: 'True'
    env_apt_show_component_calls: 'True'
    env_ds_pxdebug: '0'
    env_apt_disable_combination: 'True'
    env_apt_no_sort_insertion: 'True'
    env_cc_tera_truncate_string_with_null: '1'
    env_apt_string_allpads_not_empty: 'True'
```

---

## **🎯 Model Execution Strategy**

### **Phase 1: Foundation Setup**
```bash
# 1. Seed configuration data
dbt seed

# 2. Install dependencies  
dbt deps
```

### **Phase 2: Core Infrastructure** 
```bash
# 3. Build process key models
dbt run --select models/cse_dataload/02processkey

# 4. Build mapping and lookup sets
dbt run --select models/cse_dataload/04MappingLookupSets
```

### **Phase 3: Data Processing Pipeline**
```bash
# 5. Extract source data
dbt run --select models/cse_dataload/08extraction

# 6. Apply transformations
dbt run --select models/cse_dataload/12MappingTransformation

# 7. Load to temporary staging
dbt run --select models/cse_dataload/14loadtotemp
```

### **Phase 4: Delta Processing & Final Load**
```bash
# 8. Process delta changes
dbt run --select models/cse_dataload/16transformdelta

# 9. Application product processing
dbt run --select models/appt_pdct

# 10. Load to target GDW
dbt run --select models/cse_dataload/18loadtogdw
```

### **Phase 5: Process Management**
```bash
# 11. Update process metadata
dbt run --select models/cse_dataload/24processmetadata

# 12. Run data quality tests
dbt test
```

### **Complete Pipeline Execution**
```bash
# Run entire pipeline in dependency order
dbt run

# Full pipeline with tests
dbt build
```

---

## **📊 Key Models & Dependencies**

### **Critical Models:**

#### **Process Management:**
- **`proskeyhash__loadgdwproskeyseq`** - Process key management and hashing
- **`util_pros_isac__loadgdwproskeyseq`** - Utility process ISAC mapping

#### **Application Products:**
- **`tgtapptpdctinsertds__ldapptpdctins`** - Application product inserts
- **`tgtapptpdctinstera__ldapptpdctins`** - Application product Teradata integration
- **`gtapptpdcttera__ldtmp_appt_pdctfrmxfm`** - Temporary application product transformation

#### **Process Orchestration:**
- **`tgt_step_stus_tbl__processrunstreamstepoccrbeginandend`** - Process step status tracking
- **`xfm_step_status__processrunstreamstepoccrbeginandend`** - Step status transformation
- **`lkp_step_occr__processrunstreamstepoccrbeginandend`** - Step occurrence lookup

#### **Delta Processing:**
- **`srctmpapptpdcttera__dltappt_pdctfrmtmp_appt_pdct`** - Source temporary application product
- **`tgtapptpdctinsertds__dltappt_pdctfrmtmp_appt_pdct`** - Target application product inserts

### **Execution Dependencies:**
```
Seeds → Process Keys → Lookups → Extraction → 
Transformation → Temp Loading → Delta Processing → 
Final Loading → Metadata Updates
```

---

## **🛠️ Custom Macros**

The project includes several custom macros for enhanced functionality:

### **Stream Management:**
- **`check_stream_status.sql`** - Monitors ETL stream status
- **`run_stream_check.sql`** - Validates stream execution

### **Configuration Management:**
- **`load_job_params.sql`** - Loads job-specific parameters
- **`get_custom_schema.sql`** - Dynamic schema name generation

---

## **📁 Configuration Data (Seeds)**

### **Project Configuration (`project_config.csv`):**
```csv
context,parameter_name,parameter_value
CSEL4,GDW_SERVER,teradata.gdw.cba
CSEL4,GDW_ACCT_DB,STAR_CAD_PROD_DATA
CSEL4,BASE_DIR,cba_app__csel4__prod
CSEL4,CTL_SCHEMA,CSE4_CTL
CSEL4,APP_RELEASE,CSEL4
```

**Key Parameters:**
- **GDW_SERVER** - Source Teradata server
- **GDW_ACCT_DB** - Account database reference
- **BASE_DIR** - Application base directory
- **CTL_SCHEMA** - Control schema for metadata
- **APP_RELEASE** - Application release identifier

---

## **⚠️ Important Considerations**

### **Legacy System Integration:**
- **Teradata Origins** - Models contain legacy Teradata-specific logic
- **Source Systems** - References to external systems requiring connectivity
- **Date Parameters** - ETL process dates need proper configuration

### **Data Quality:**
- **Rejection Handling** - Built-in data quality and error processing
- **Audit Trails** - Comprehensive logging and process tracking
- **Post-hooks** - Models use post-hooks for target table loading

### **Performance Optimization:**
- **Warehouse Sizing** - May need larger warehouses for full production loads
- **Clustering** - Consider clustering on process dates and keys
- **Incremental Models** - Some models may benefit from incremental processing

### **File Organization Notes:**
- **`translated/` folder** - Contains legacy translated files that are NOT actively used by dbt
- **Active models only** - dbt only processes files in the `models/` directory
- **Reference purposes** - translated folder may serve as reference for business logic

---

## **🔧 Troubleshooting**

### **Common Issues:**

#### **1. Source Table Missing**
```bash
# Error: Relation 'database.schema.table' does not exist
# Solution: Update source references in models
```

#### **2. Permission Errors**
```bash
# Error: Insufficient privileges to operate on database
# Solution: Ensure proper database/schema access for your role
```

#### **3. Variable Substitution**
```bash
# Error: Unknown variable 'var_name'
# Solution: Verify all cvar() variables are defined in dbt_project.yml
```

#### **4. Post-hook Failures**
```bash
# Error: Post-hook failed during model execution
# Solution: Check target table structures and permissions
```

### **Monitoring:**
```sql
-- Monitor dbt run progress
SELECT * FROM GDW1.cse4_ctl.STEP_OCCR 
WHERE RUN_STRM_C = 'CSE_COM_BUS_APP_PROD'
ORDER BY STEP_OCCR_STRT_S DESC;

-- Check process key generation
SELECT * FROM GDW1_STG.files.PROCESSKEYHASH__HSH
ORDER BY PROS_KEY_I DESC
LIMIT 10;

-- Monitor data quality rejections
SELECT * FROM GDW1_STG.datasets.*_rejects_*
WHERE ETL_PROCESS_DT = '20240101';
```

---

## **📈 Production Deployment**

### **Environment Promotion:**
1. **Development** → Test configuration and model logic
2. **Staging** → Validate with production-like data volumes
3. **Production** → Full deployment with monitoring

### **Performance Considerations:**
```sql
-- Recommended warehouse sizes by phase
-- Phase 1-2 (Setup): SMALL
-- Phase 3-4 (Processing): MEDIUM to LARGE
-- Phase 5 (Metadata): SMALL

-- Clustering recommendations
ALTER TABLE GDW1_IBRG.schema.table 
CLUSTER BY (ETL_PROCESS_DT, PROS_KEY_I);
```

### **Monitoring & Alerts:**
```sql
-- Set up alerts for failed runs
CREATE OR REPLACE ALERT dbt_failure_alert
WAREHOUSE = DBT
SCHEDULE = '5 MINUTE'
IF (
  SELECT COUNT(*) FROM GDW1.cse4_ctl.STEP_OCCR 
  WHERE STEP_STUS_C = 'F' 
  AND STEP_OCCR_STRT_S > DATEADD(MINUTE, -10, CURRENT_TIMESTAMP)
) > 0
THEN CALL alert_notification_procedure();
```

---

## **📚 Additional Resources**

### **dbt Documentation:**
- [dbt Official Documentation](https://docs.getdbt.com/)
- [Snowflake dbt Integration](https://docs.snowflake.com/en/user-guide/ui-snowsight-projects-dbt)
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices)

### **CBA-Specific Resources:**
- **Project Repository:** [GitHub - REPO_CBA](https://github.com/sfc-gh-mhindi/REPO_CBA)
- **Legacy Documentation:** Available in translated folder for reference
- **Business Logic:** Preserved from original Teradata implementation

---

## **🏁 Conclusion**

This dbt project represents a comprehensive enterprise data pipeline migration from Teradata to Snowflake, featuring:

✅ **Robust Process Orchestration** - Complete ETL workflow management  
✅ **Data Quality Controls** - Built-in validation and rejection handling  
✅ **Business Logic Preservation** - Legacy system logic maintained  
✅ **Scalable Architecture** - Multi-layered processing approach  
✅ **Comprehensive Monitoring** - Full audit trail and process tracking  

The project is production-ready with proper configuration and provides a solid foundation for CBA's data transformation needs in Snowflake.

---

**Created:** August 6, 2024  
**Version:** 1.0  
**Author:** CBA Data Engineering Team  
**Location:** NPW DBT - My Changes/README_NPW_DBT_Implementation_Guide.md 