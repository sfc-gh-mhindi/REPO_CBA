# 🚀 GDW1 BCFINSG Implementation Guide
## **DataStage to dbt Migration using DCF Framework**

### 📋 **Overview**

This comprehensive implementation guide provides step-by-step instructions for migrating the **GDW1 BCFINSG stream** from DataStage to dbt using the **DCF (DBT Control Framework)**. The implementation modernizes the legacy DataStage ETL pipeline while preserving all enterprise control capabilities and audit requirements.

**Key Deliverables:**
- Stream configuration in shared DCF framework
- dbt model implementation with DCF integration
- Execution scripts and validation procedures
- Migration from Oracle/Teradata to Snowflake

---

## 🏗️ **Architecture Overview**

### **Original DataStage Flow**
```
SQ10COMMONPreprocess → RunStreamStart → BCFINSG Processing → Job Completion
     ↓                      ↓                  ↓               ↓
Job Occurrence       Stream Occurrence    File Processing   Status Update
```

### **Modern DCF Flow**
```
start_stream_op → dbt Models → stream_completion
      ↓               ↓              ↓
Stream Instance    Incremental     DCF Audit
Creation          Processing      Updates
```

---

## 📁 **Project Structure**

```
gdw1_igsn_xfm/gdw1_dbt/
├── schema_definitions/
│   └── gdw1_dcf_schema_init.sql          # Adds BCFINSG stream to DCF
├── macros/
│   ├── dcf/                              # Complete DCF framework (from GDW2)
│   │   ├── common.sql                        # Database refs, audit columns
│   │   ├── stream_operations.sql             # Mature stream lifecycle management
│   │   ├── process_control.sql               # Process registration & tracking
│   │   └── logging.sql                       # Execution logging & audit
│   ├── dcf_macros_import.sql             # GDW1-specific DCF helpers
│   ├── validate_source_files.sql         # File validation (existing)
│   ├── update_process_metadata.sql       # Process tracking (existing)
│   └── generate_business_key.sql         # Business key generation (existing)
├── models/
│   ├── staging/
│   │   └── stg_bcfinsg_plan_baln_segm.sql     # Enhanced with DCF validation
│   ├── intermediate/
│   │   └── int_bcfinsg_validated.sql          # Comprehensive business validation
│   └── marts/core/
│       └── fct_plan_baln_segm_mstr.sql        # Incremental fact table with DCF
└── dbt_project.yml                       # DCF variables (shared with GDW2)
```

---

## ⚙️ **Setup and Configuration**

### **Step 1: Initialize DCF Schema**

**⚠️ One-Time Setup Required:** Run this script to create DCF control tables:

```bash
# Execute DCF schema initialization
snowsql -f schema_definitions/gdw1_dcf_schema_init.sql

# Verify tables were created
snowsql -q "SELECT table_name, row_count FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'P_D_DCF_001_STD_0'"
```

### **Step 2: Configure dbt Connection**

Update `profiles.yml` with GDW1-specific connection details:

```yaml
ccods_gdw1:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE') }}"
      database: "{{ var('dcf_database') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      schema: P_D_DCF_001_STD_0
      threads: 4
      keepalives_idle: 240
```

### **Step 3: Install dbt Dependencies**

```bash
cd gdw1_igsn_xfm/gdw1_dbt
dbt deps
dbt debug
```

---

## 🚀 **Phase 1 Implementation: SQ10 + RunStreamStart**

### **📋 Task 1.1: DCF Stream Configuration**

**File: `schema_definitions/gdw1_dcf_schema_init.sql`**
**Status: ✅ COMPLETED**

```sql
INSERT INTO {{ var('dcf_database') }}.{{ var('dcf_schema') }}.DCF_T_STRM (
    STRM_ID, STRM_NAME, STRM_DESC, STRM_TYPE, CYCLE_FREQ_CODE, 
    MAX_CYCLES_PER_DAY, ALLOW_MULTIPLE_CYCLES, BUSINESS_DOMAIN, 
    TARGET_SCHEMA, TARGET_TABLE, DBT_TAG, CTL_ID, STRM_STATUS
) VALUES (
    1490, 'BCFINSG_PLAN_BALN_SEGM_LOAD', 'BCFINSG Plan Balance Segment Master Load', 
    'DAILY', 1, 1, FALSE, 'FINANCE', 'P_D_BAL_001_STD_0', 
    'PLAN_BALN_SEGM_MSTR', 'tag:stream_bcfinsg', 149, 'ACTIVE'
);
```

### **📋 Task 1.2: dbt Project Configuration**

**File: `dbt_project.yml`**
**Status: ✅ COMPLETED**

```yaml
vars:
  dcf_database: "{{ env_var('DCF_DATABASE') }}"
  dcf_schema: "{{ env_var('DCF_SCHEMA') }}"
  target_database: "{{ env_var('TARGET_DATABASE') }}"
  stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD"
  stream_id: 1490
  
  # Legacy parameter mappings for DataStage compatibility
  pRUN_STRM_C: "{{ var('stream_name', 'BCFINSG_PLAN_BALN_SEGM_LOAD') }}"
  pRUN_STRM_PROS_D: "{{ var('run_stream_process_date') }}"
  pODS_BTCH_ID: "{{ var('ods_batch_id') }}"
  pCTL_DATABASE: "{{ var('dcf_database') }}"
  pCTL_USER: "{{ env_var('SNOWFLAKE_USER', 'dbt_service') }}"
```

### **📋 Task 1.3: Stream Execution Command**

**Replaces entire SQ10COMMONPreprocess + RunStreamStart sequence:**

```bash
# Single command replaces all DataStage preprocessing
dbt run-operation start_stream_op --args '{
  stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD",
  business_date: "2024-12-20"
}'
```

### **📋 Task 1.4: Validation Commands**

```bash
# Verify stream instance creation
dbt run-operation get_stream_status_op --args '{
  stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD"
}'

# Check DCF audit trail
snowsql -q "SELECT * FROM {{ dcf_database }}.{{ dcf_schema }}.DCF_T_STRM_INST 
            WHERE STRM_NAME = 'BCFINSG_PLAN_BALN_SEGM_LOAD' 
            ORDER BY INST_TS DESC LIMIT 5"
```

---

## 🔄 **DCF Operations Guide**

### **Stream Lifecycle Management**

#### **Start Stream Operation**
```bash
dbt run-operation start_stream_op --args '{
  stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD",
  business_date: "{{ var('target_date', ds) }}"
}'
```

#### **Check Stream Status**
```bash
dbt run-operation get_stream_status_op --args '{
  stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD"
}'
```

#### **End Stream Operation**
```bash
dbt run-operation end_stream_op --args '{
  stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD",
  status: "COMPLETED"
}'
```

#### **Reset Stream (if needed)**
```bash
dbt run-operation reset_stream_op --args '{
  stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD",
  business_date: "2024-12-20"
}'
```

### **Process Instance Tracking**

#### **Register Process Start**
```sql
{{ register_process_start('validate_bcfinsg', 'VALIDATION') }}
```

#### **Register Process Completion**
```sql
{{ register_process_completion('validate_bcfinsg', 'COMPLETED', row_count) }}
```

### **Execution Logging**
```sql
{{ log_model_execution_stats(this, 'STAGING', row_count, start_time, end_time) }}
```

---

## 📊 **Model Implementation Examples**

### **Staging Model with DCF Integration**

**File: `models/staging/stg_bcfinsg_plan_baln_segm.sql`**

```sql
{{
  config(
    materialized='table',
    pre_hook="{{ validate_stream_status(var('stream_name')) }}",
    post_hook=[
      "{{ register_process_start('stg_bcfinsg_plan_baln_segm', 'STAGING') }}",
      "{{ register_process_completion('stg_bcfinsg_plan_baln_segm', 'COMPLETED', this.rows) }}",
      "{{ log_model_execution_stats(this, 'STAGING', this.rows, run_started_at, run_completed_at) }}"
    ]
  )
}}

SELECT 
    -- Source columns with DCF audit
    {{ dcf_audit_columns() }},
    BCF_DT_CURR_PROC,
    PLAN_CODE,
    BALN_SEGM_CODE,
    -- ... other business columns
FROM {{ source('raw', 'bcfinsg_plan_baln_segm') }}
WHERE BCF_DT_CURR_PROC = '{{ var("run_stream_process_date") }}'
```

### **Intermediate Model with Validation**

**File: `models/intermediate/int_bcfinsg_validated.sql`**

```sql
{{
  config(
    materialized='table',
    pre_hook="{{ validate_stream_status(var('stream_name')) }}",
    post_hook=[
      "{{ register_process_start('int_bcfinsg_validated', 'VALIDATION') }}",
      "{{ register_process_completion('int_bcfinsg_validated', 'COMPLETED', this.rows) }}",
      "{{ log_model_execution_stats(this, 'VALIDATION', this.rows, run_started_at, run_completed_at) }}"
    ]
  )
}}

WITH validation AS (
  SELECT *,
    CASE 
      WHEN BCF_DT_CURR_PROC = '{{ var("run_stream_process_date") }}' THEN 'VALID'
      ELSE 'INVALID_DATE'
    END AS validation_status,
    
    -- Business validation rules
    CASE 
      WHEN PLAN_CODE IS NULL THEN 'MISSING_PLAN_CODE'
      WHEN BALN_SEGM_CODE IS NULL THEN 'MISSING_SEGMENT'
      ELSE 'VALID'
    END AS business_validation_status
    
  FROM {{ ref('stg_bcfinsg_plan_baln_segm') }}
)

SELECT * FROM validation
WHERE validation_status = 'VALID' 
  AND business_validation_status = 'VALID'
```

### **Final Mart Model with Incremental Loading**

**File: `models/marts/core/fct_plan_baln_segm_mstr.sql`**

```sql
{{
  config(
    materialized='incremental',
    unique_key='business_key',
    merge_update_columns=['baln_amt', 'last_updated_ts'],
    pre_hook="{{ validate_stream_status(var('stream_name')) }}",
    post_hook=[
      "{{ register_process_start('fct_plan_baln_segm_mstr', 'LOADING') }}",
      "{{ register_process_completion('fct_plan_baln_segm_mstr', 'COMPLETED', this.rows) }}",
      "{{ log_model_execution_stats(this, 'LOADING', this.rows, run_started_at, run_completed_at) }}"
    ]
  )
}}

SELECT 
    {{ generate_business_key(['PLAN_CODE', 'BALN_SEGM_CODE', 'BCF_DT_CURR_PROC']) }} AS business_key,
    {{ dcf_audit_columns() }},
    PLAN_CODE,
    BALN_SEGM_CODE,
    BALN_AMT,
    BCF_DT_CURR_PROC AS process_date,
    CURRENT_TIMESTAMP() AS last_updated_ts
    
FROM {{ ref('int_bcfinsg_validated') }}

{% if is_incremental() %}
  WHERE BCF_DT_CURR_PROC = '{{ var("run_stream_process_date") }}'
{% endif %}
```

---

## ✅ **Execution Workflow**

### **Daily Processing Sequence**

```bash
# 1. Initialize stream for business date
dbt run-operation start_stream_op --args '{
  stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD",
  business_date: "2024-12-20"
}'

# 2. Run staging model
dbt run --select stg_bcfinsg_plan_baln_segm

# 3. Run validation model  
dbt run --select int_bcfinsg_validated

# 4. Run final mart model
dbt run --select fct_plan_baln_segm_mstr

# 5. Finalize stream
dbt run-operation end_stream_op --args '{
  stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD",
  status: "COMPLETED"
}'
```

### **Validation & Monitoring**

```bash
# Check execution status
dbt run-operation get_stream_history_op --args '{
  stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD"
}'

# Review execution logs
snowsql -q "SELECT * FROM {{ dcf_database }}.{{ dcf_schema }}.DCF_T_EXEC_LOG 
            WHERE STRM_NAME = 'BCFINSG_PLAN_BALN_SEGM_LOAD' 
            ORDER BY EXEC_TS DESC"

# Verify data quality
dbt test --select fct_plan_baln_segm_mstr
```

---

## 🚨 **Error Handling & Recovery**

### **Stream Recovery Operations**

```bash
# If stream fails, reset and restart
dbt run-operation reset_stream_op --args '{
  stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD",
  business_date: "2024-12-20"
}'

# Force restart (if needed)
dbt run-operation start_stream_op --args '{
  stream_name: "BCFINSG_PLAN_BALN_SEGM_LOAD",
  business_date: "2024-12-20",
  force_restart: true
}'
```

### **Data Quality Issues**
```bash
# Run specific tests
dbt test --select fct_plan_baln_segm_mstr

# Check data freshness
dbt source freshness
```

---

## 📋 **Success Criteria**

### **✅ Phase 1 Completion Checklist**

- [ ] DCF stream configuration deployed
- [ ] dbt models execute successfully 
- [ ] Stream lifecycle operations working
- [ ] Data validation tests passing
- [ ] Execution logging captured in DCF tables
- [ ] Performance meets or exceeds DataStage baseline

### **📊 Key Metrics**
- **Processing Time**: Target < 30 minutes for daily load
- **Data Quality**: 100% validation pass rate
- **Availability**: 99.9% successful execution rate
- **Audit Compliance**: Complete DCF audit trail

---

**Implementation Status**: Phase 1 (SQ10 + RunStreamStart) design complete  
**Next Phase**: Full end-to-end implementation and testing  
**Framework**: DCF framework from GDW2 project