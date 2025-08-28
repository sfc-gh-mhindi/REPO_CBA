# 📋 **Modern Error Table Design: XFM_ERR_DTL**

## **Overview**

The `XFM_ERR_DTL` table represents a modern, JSON-based error tracking design that captures **one row per source record** with all validation errors stored as a JSON array. This approach is more efficient and scalable than the traditional one-row-per-error design.

## **📊 Table Structure**

### **XFM_ERR_DTL (Main Error Table)**

```sql
-- Primary error table with JSON error details
CREATE TABLE PSUND_MIGR_DCF.P_D_DCF_001_STD_0.XFM_ERR_DTL (
    -- Primary identifiers  
    ERR_ID                  NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    SRCE_KEY_NM             VARCHAR(100) NOT NULL,      -- Source record identifier
    
    -- Process tracking (using standard naming conventions)
    STRM_NM                 VARCHAR(100) NOT NULL,      -- Stream name
    PRCS_NM                 VARCHAR(100) NOT NULL,      -- Process name  
    PRCS_DT                 DATE NOT NULL,              -- Processing date
    PRCS_TS                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Source information
    SRCE_FILE_NM            VARCHAR(255),               -- Source file name
    SRCE_SYST_NM            VARCHAR(50) DEFAULT 'BCFINSG',
    SRCE_ROW_NUM            NUMBER(10,0),               -- Source row number
    
    -- Error details (JSON structure - key innovation)
    ERR_DTLS_JSON           VARIANT NOT NULL,           -- JSON array of all errors
    ERR_CNT                 NUMBER(5,0) NOT NULL,       -- Total error count
    ERR_CTGRY_NM            VARCHAR(50) DEFAULT 'VALIDATION_ERROR',
    
    -- Processing metadata
    BTCH_ID                 VARCHAR(100),               -- Batch identifier
    RUN_ID                  VARCHAR(100),               -- Run identifier  
    LOAD_TS                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LOAD_BY_NM              VARCHAR(100) DEFAULT CURRENT_USER()
);
```

### **VW_XFM_ERR_DTL_FLAT (Flattening View)**

```sql
-- View to flatten JSON errors for analysis (one row per error)
CREATE VIEW PSUND_MIGR_DCF.P_D_DCF_001_STD_0.VW_XFM_ERR_DTL_FLAT AS
SELECT 
    -- Main record identifiers
    e.ERR_ID,
    e.SRCE_KEY_NM,
    e.STRM_NM,
    e.PRCS_NM,
    e.PRCS_DT,
    
    -- Flattened error details (one row per error)
    f.index as ERR_SEQ_NUM,
    f.value:column_name::VARCHAR(100) as ERR_COLM_NM,
    f.value:error_type::VARCHAR(100) as ERR_TYPE_NM,
    f.value:error_message::VARCHAR(500) as ERR_MSG_TXT,
    f.value:original_value::VARCHAR(255) as ORIG_VAL_TXT,
    f.value:attempted_value::VARCHAR(255) as ATMPTD_VAL_TXT,
    
    -- Metadata
    e.ERR_CNT,
    e.SRCE_FILE_NM,
    e.LOAD_TS
    
FROM XFM_ERR_DTL e,
     LATERAL FLATTEN(input => e.ERR_DTLS_JSON) f;
```

## **📝 JSON Error Structure**

### **ERR_DTLS_JSON Format**

```json
[
  {
    "column_name": "BCF_DT_FIRST_TRANS",
    "error_type": "DATE_CONVERSION_ERROR",
    "error_message": "Invalid EBCDIC date: cannot convert to valid date",
    "original_value": "20241399",
    "attempted_value": null,
    "validation_rule": "fn_is_valid_dt"
  },
  {
    "column_name": "BCF_DT_LAST_PAYMENT",
    "error_type": "DATE_CONVERSION_ERROR", 
    "error_message": "Invalid EBCDIC date: cannot convert to valid date",
    "original_value": "999",
    "attempted_value": null,
    "validation_rule": "fn_is_valid_dt"
  }
]
```

## **💡 Key Benefits**

### **1. Efficiency**
- **One row per source record** instead of one row per error
- Reduces table size significantly for records with multiple errors
- Better performance for bulk operations

### **2. Flexibility**
- JSON structure supports unlimited error types
- Easy to add new validation rules without schema changes
- Rich metadata can be stored per error

### **3. Modern Design**
- Clean naming conventions using `_NM`, `_TS`, `_DT` patterns
- Leverages Snowflake's native JSON capabilities
- Future-proof design for expanding validation requirements

### **4. Analysis Friendly**
- Flattening view provides traditional one-row-per-error format
- JSON enables complex filtering and aggregation
- Supports both summary and detailed error analysis

## **🔍 Usage Examples**

### **Daily Error Summary**
```sql
-- Summary of errors by day
SELECT 
    PRCS_DT,
    COUNT(DISTINCT SRCE_KEY_NM) as failed_records,
    SUM(ERR_CNT) as total_errors,
    COUNT(DISTINCT SRCE_FILE_NM) as affected_files,
    ROUND(AVG(ERR_CNT), 2) as avg_errors_per_record
FROM XFM_ERR_DTL 
WHERE PRCS_DT >= CURRENT_DATE - 7
GROUP BY PRCS_DT
ORDER BY PRCS_DT DESC;
```

### **Error Analysis by Column**
```sql
-- Analysis using flattened view
SELECT 
    ERR_COLM_NM,
    ERR_TYPE_NM,
    COUNT(*) as error_count,
    COUNT(DISTINCT SRCE_KEY_NM) as affected_records,
    MIN(ORIG_VAL_TXT) as sample_bad_value
FROM VW_XFM_ERR_DTL_FLAT
WHERE PRCS_DT >= CURRENT_DATE - 7
GROUP BY ERR_COLM_NM, ERR_TYPE_NM
ORDER BY error_count DESC;
```

### **Records with Multiple Errors**
```sql
-- Find records with multiple validation errors
SELECT 
    SRCE_KEY_NM,
    ERR_CNT,
    SRCE_FILE_NM,
    ERR_DTLS_JSON
FROM XFM_ERR_DTL
WHERE ERR_CNT > 1
  AND PRCS_DT = CURRENT_DATE
ORDER BY ERR_CNT DESC;
```

### **Specific Error Type Analysis**
```sql
-- Analysis of specific error patterns using JSON queries
SELECT 
    ERR_DTLS_JSON[0]:column_name::VARCHAR as first_error_column,
    ERR_DTLS_JSON[0]:original_value::VARCHAR as first_error_value,
    COUNT(*) as frequency
FROM XFM_ERR_DTL
WHERE PRCS_DT >= CURRENT_DATE - 7
  AND ERR_DTLS_JSON[0]:error_type::VARCHAR = 'DATE_CONVERSION_ERROR'
GROUP BY 1, 2
ORDER BY frequency DESC
LIMIT 10;
```

## **📈 Sample Data**

### **Example Error Record**
```sql
INSERT INTO XFM_ERR_DTL VALUES (
    123,                                    -- ERR_ID (auto-generated)
    'ACC123456789_PLAN001_1001',           -- SRCE_KEY_NM
    'BCFINSG_PLAN_BALN_SEGM_LOAD',        -- STRM_NM
    'XfmPlanBalnSegmMstrFromBCFINSG',      -- PRCS_NM
    '2024-12-20',                          -- PRCS_DT
    '2024-12-20 10:30:45'::TIMESTAMP,      -- PRCS_TS
    'BCFINSG_C001_20241220.DLY',          -- SRCE_FILE_NM
    'BCFINSG',                             -- SRCE_SYST_NM
    1001,                                  -- SRCE_ROW_NUM
    [                                      -- ERR_DTLS_JSON
        {
            "column_name": "BCF_DT_FIRST_TRANS",
            "error_type": "DATE_CONVERSION_ERROR",
            "error_message": "Invalid EBCDIC date: cannot convert to valid date",
            "original_value": "20241399",
            "attempted_value": null,
            "validation_rule": "fn_is_valid_dt"
        },
        {
            "column_name": "BCF_DT_LAST_PAYMENT",
            "error_type": "DATE_CONVERSION_ERROR",
            "error_message": "Invalid EBCDIC date: cannot convert to valid date", 
            "original_value": "999",
            "attempted_value": null,
            "validation_rule": "fn_is_valid_dt"
        }
    ],
    2,                                     -- ERR_CNT
    'VALIDATION_ERROR',                    -- ERR_CTGRY_NM
    'BATCH_20241220_001',                  -- BTCH_ID
    '20241220',                            -- RUN_ID
    CURRENT_TIMESTAMP(),                   -- LOAD_TS
    'dbt_service_user'                     -- LOAD_BY_NM
);
```

## **🔄 dbt Implementation**

### **Error Collection (int_bcfinsg_router.sql)**
```sql
-- Router collects all date field errors as JSON
ARRAY_CONSTRUCT_COMPACT(
    CASE WHEN NOT fn_is_valid_dt(BCF_DT_FIRST_TRANS) 
        THEN OBJECT_CONSTRUCT(
            'column_name', 'BCF_DT_FIRST_TRANS',
            'error_type', 'DATE_CONVERSION_ERROR',
            'error_message', 'Invalid EBCDIC date: cannot convert to valid date',
            'original_value', BCF_DT_FIRST_TRANS::VARCHAR,
            'attempted_value', NULL,
            'validation_rule', 'fn_is_valid_dt'
        ) END,
    -- Additional date fields...
) as error_details_json
```

### **Error Storage (xfm_err_dtl.sql)**
```sql
-- Direct mapping from router to error table
SELECT 
    source_key as SRCE_KEY_NM,
    '{{ var("stream_name") }}' as STRM_NM,
    'XfmPlanBalnSegmMstrFromBCFINSG' as PRCS_NM,
    error_details_json as ERR_DTLS_JSON,
    ARRAY_SIZE(error_details_json) as ERR_CNT,
    -- Additional metadata...
FROM {{ ref('int_bcfinsg_router') }}
WHERE record_type = 'ERROR'
  AND ARRAY_SIZE(error_details_json) > 0
```

## **🏆 Comparison: Old vs New Design**

| **Aspect** | **Old UTIL_TRSF_EROR_RQM3** | **New XFM_ERR_DTL** |
|------------|----------------------------|-------------------|
| **Rows per record** | One row per error | One row per source record |
| **Multiple errors** | Multiple table rows | Single JSON array |
| **Storage efficiency** | High redundancy | Compact JSON storage |
| **Query complexity** | Simple SELECT | JSON + flattening view |
| **Naming convention** | Legacy DataStage names | Modern `_NM`, `_TS` patterns |
| **Extensibility** | Schema changes required | JSON structure expansion |
| **Analysis flexibility** | Basic SQL only | JSON queries + SQL |

**Result: The new design is more efficient, flexible, and future-proof while maintaining compatibility through the flattening view.**