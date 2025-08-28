# CSEL4 Validation Framework Implementation

## Overview

This document summarizes the implementation of the CSEL4 Data Validation Framework in dbt, following the comprehensive validation strategy defined in the DataStage pipeline.

## ✅ Completed Implementations

### 1. **Dead Code Cleanup**
- ✅ **Removed MAP_CSE_JRNL_TYPE references** - Confirmed as dead code that doesn't populate target tables
- ✅ **Updated dbt models** to remove journal type enrichment
- ✅ **Updated migration status** to reflect removal

### 2. **Validation Macros**

#### `validate_header_trailer` Macro (`macros/validate_header_trailer.sql`)
Implements **Layer 1: File-Level Validation** equivalent to `FileValidationLevel01CSEL4` using the DCF control table framework:

| **Validation Code** | **Check** | **Implementation** |
|-------------------|----------|-------------------|
| **FVL0001** | Header record present | Validates single header record exists |
| **FVL0002** | Date validation | Header date matches ETL process date |
| **FVL0003** | File name validation | Header filename matches expected |
| **FVL0004** | Delimiter count check | Validates pipe delimiter count per row |
| **FVL0005** | Row count validation | Detail rows match trailer count |
| **FVL0006** | Trailer record present | Validates single trailer record exists |

#### Additional Helper Macros (`macros/validation_helpers.sql`)
- `validate_date_format` - Date validation using DCF `fn_is_valid_dt` UDF (same as CCODS)

#### Control Table Integration Macros (`macros/validate_header_trailer.sql`)
- `update_control_table_status` - Updates DCF_T_IGSN_FRMW_HDR_CTRL processing status
- `validate_and_update_control_status` - Comprehensive validation with status updates
- `get_validation_summary` - Monitoring and reporting for validation results

### 3. **Validation Intermediate Model (`models/csel4/intermediate/int_validate_cse_cpl_bus_app.sql`)**

Implements **Layer 2: Field-Level Validation** equivalent to `ExtPL_APP`:

#### Mandatory Field Validation
```sql
-- Critical fields that must be present
- PL_APP_ID (Primary Key)
- NOMINATED_BRANCH_ID (Required for DEPT_I mapping)  
- MOD_TIMESTAMP (Required for effective date)
```

#### Data Type Validation
```sql
-- Type conversion validation
- PL_APP_ID numeric conversion (DECIMAL(18,0))
- REQUESTED_LIMIT_AMT numeric validation (DECIMAL(21,2))
- Date format validation (YYYYMMDD)
```

#### Data Quality Scoring
```sql
-- 4-tier quality scoring system
- 100%: All validations pass
- 85%: Core mandatory fields present  
- 70%: Primary key fields present
- 0%: Critical validation failures
```

#### Validation Flags
Each record includes validation flags for monitoring:
- `pl_app_id_valid`
- `nominated_branch_id_valid`
- `mod_timestamp_valid`
- `effective_date_valid`
- `record_valid` (overall flag)
- `data_quality_score` (0-100)

### 4. **Comprehensive Testing Framework**

#### Schema Tests (`models/csel4/staging/schema.yml`)
- **Source freshness tests** - Ensures timely data delivery
- **Data quality threshold tests** - Validates minimum quality scores
- **Mandatory field tests** - Ensures critical fields are populated
- **Data type tests** - Validates field formats and ranges

#### Custom Tests
- `assert_data_quality_threshold` - Custom test for quality score validation
- `test_file_validation.sql` - Unit test for header/trailer validation

#### Validation Monitoring
- **Real-time logging** of validation summaries
- **Error counting** by field and validation type
- **Quality score tracking** for monitoring trends

## 🔧 Usage Instructions

### 1. **Run File Validation**
```sql
-- Test header/trailer validation using DCF control table
{{ validate_header_trailer(
    stream_name='BCFINSG_PLAN_BALN_SEGM_LOAD',
    expected_file_name='BCFINSG_CA',
    etl_process_date=var('etl_process_date')
) }}

-- Or run validation with automatic status updates
{{ validate_and_update_control_status(
    stream_name='BCFINSG_PLAN_BALN_SEGM_LOAD',
    expected_file_name='BCFINSG_CA',
    etl_process_date=var('etl_process_date')
) }}
```

### 2. **Monitor Data Quality**
```bash
# Run with validation logging
dbt run --vars '{"etl_process_date": "20240115"}' --log-level info

# Check validation test results
dbt test --models tag:validated
```

### 3. **Quality Score Monitoring**
```sql
-- Query field validation results
SELECT 
    etl_process_date,
    AVG(data_quality_score) as avg_quality,
    COUNT(*) as total_records,
    SUM(CASE WHEN record_valid THEN 1 ELSE 0 END) as valid_records
FROM {{ ref('int_validate_cse_cpl_bus_app') }}
GROUP BY etl_process_date
ORDER BY etl_process_date DESC;

-- Get file validation summary from control table
{{ get_validation_summary(
    stream_name='BCFINSG_PLAN_BALN_SEGM_LOAD',
    days_back=7
) }}
```

## 📊 Validation Layers Implemented

| **Layer** | **DataStage Equivalent** | **dbt Implementation** | **Status** |
|-----------|-------------------------|----------------------|------------|
| **Layer 1: File Validation** | `FileValidationLevel01CSEL4` | `validate_header_trailer` macro | ✅ Complete |
| **Layer 2: Field Validation** | `ExtPL_APP` | Enhanced staging model | ✅ Complete |
| **Layer 3: Business Rules** | `XfmPL_APPFrmExt` | Intermediate models | ✅ Complete |
| **Layer 4: Load Validation** | Delta processing jobs | Incremental models | ✅ Complete |

## 🚨 Error Handling Strategy

### Validation Failure Actions
1. **File validation failure** → Test failure, pipeline stops
2. **Field validation failure** → Records filtered out, processing continues with valid data
3. **Quality threshold breach** → Warning logged, optional pipeline failure
4. **Data type conversion failure** → Field set to NULL, validation flag = FALSE

### Monitoring and Alerting
- **dbt test failures** trigger alerts for critical validation issues
- **Quality score logging** enables proactive monitoring
- **Validation flags** allow downstream models to handle data quality appropriately

## 🔄 Migration Benefits

### Compared to DataStage Implementation
✅ **Declarative validation** - Tests defined in YAML, version controlled  
✅ **Integrated monitoring** - Built-in logging and alerting  
✅ **Flexible thresholds** - Configurable quality score requirements  
✅ **Dead code elimination** - Removed unused journal type lookups  
✅ **Enhanced visibility** - Field-level validation flags  
✅ **Automated testing** - Continuous validation in CI/CD pipeline  

### Production Readiness
- ✅ **Comprehensive validation coverage** matching DataStage functionality
- ✅ **Quality score monitoring** for proactive data quality management  
- ✅ **Flexible error handling** with configurable thresholds
- ✅ **Complete audit trail** with validation flags and timestamps

## 📚 Reference Documentation

- **CSEL4 Data Validation Framework** - Complete validation strategy documentation
- **DataStage Job Analysis** - Original validation logic from XML analysis
- **dbt Best Practices** - Validation testing and monitoring patterns

This implementation provides enterprise-grade data validation that matches and exceeds the original DataStage validation capabilities while leveraging modern dbt testing and monitoring features.
