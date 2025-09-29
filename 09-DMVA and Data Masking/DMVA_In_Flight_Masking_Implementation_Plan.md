# DMVA V2 In-Flight Data Masking Implementation Plan

## 🎯 **Project Overview**

### **Objective**
Implement in-flight data masking capabilities for DMVA V2 that enables secure data migration from Teradata sources with the following requirements:
- Allow users to specify which columns need to be masked
- Provide dedicated masking functions for strings, numbers, and dates
- Ensure data is never stored unmasked in any stage or temporary location
- Apply masking exclusively to Teradata source systems

### **Key Benefits**
- **Security**: Sensitive data is masked at the source before any storage or transmission
- **Compliance**: Meets data privacy regulations by preventing exposure of PII/PHI
- **Flexibility**: Column-level granular control over masking policies
- **Performance**: In-flight masking with minimal performance overhead
- **Integrity**: Maintains referential integrity through deterministic masking options

---

## 📋 **Detailed Implementation Plan**

### **Phase 1: Database Schema Extensions**

#### 1.1 Extend `dmva_column_info` Table
**File**: `snowflake/03_Tables/dmva_column_info.sql`

**Purpose**: Add masking configuration columns to existing column metadata table

```sql
-- Add new columns for masking configuration
ALTER TABLE dmva_column_info ADD COLUMN (
    masking_enabled BOOLEAN DEFAULT FALSE,
    masking_function_type VARCHAR(50), -- 'STRING', 'NUMBER', 'DATE'
    masking_parameters VARIANT,        -- JSON config for masking parameters
    masking_seed VARCHAR(100)          -- Optional seed for deterministic masking
);
```

**Impact**: Extends existing column metadata with masking configuration without breaking existing functionality.

#### 1.2 Create Masking Configuration Table
**New File**: `snowflake/03_Tables/dmva_masking_config.sql`

**Purpose**: Centralized configuration for pattern-based masking rule assignment

```sql
CREATE TABLE dmva_masking_config (
    config_id NUMBER AUTOINCREMENT,
    system_name VARCHAR NOT NULL,
    database_pattern VARCHAR,
    schema_pattern VARCHAR,
    table_pattern VARCHAR,
    column_pattern VARCHAR,
    data_type_pattern VARCHAR,
    masking_function_type VARCHAR(50) NOT NULL,
    masking_parameters VARIANT,
    priority NUMBER DEFAULT 100,
    active BOOLEAN DEFAULT TRUE,
    created_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_ts TIMESTAMP_NTZ
);
```

**Use Cases**:
- Bulk configuration of masking policies
- Pattern-based rule application (e.g., all columns named '*SSN*' get partial masking)
- Priority-based rule resolution for overlapping patterns

---

## 🔧 **Implementation Considerations**

### **Security Requirements**

#### Data Protection
- **Zero Unmasked Storage**: Implement validation checks to ensure no unmasked sensitive data is written to any temporary location
- **Audit Trail**: Log all masking operations with timestamps, user context, and configuration details
- **Access Control**: Restrict masking configuration to authorized users only

#### Teradata-Only Enforcement
- **System Validation**: Add checks in all masking procedures to verify system type is Teradata
- **Configuration Guards**: Prevent masking configuration on non-Teradata systems
- **Runtime Validation**: Double-check system type during query execution

### **Performance Considerations**

#### Query Optimization
- **Function Efficiency**: Ensure masking functions are optimized for large datasets
- **Pushdown Optimization**: Generate Teradata-native SQL to leverage database optimization
- **Batch Processing**: Maintain existing parallelization capabilities

#### Performance Monitoring
- **Baseline Metrics**: Establish performance baselines before masking implementation
- **Continuous Monitoring**: Track query execution times with masking enabled
- **Alerting**: Set up alerts for performance degradation beyond acceptable thresholds

---

## 📋 **Implementation Timeline and Phases**

### **Phase 1: Foundation (Weeks 1-2)**
- Database schema extensions
- Core masking functions development
- Basic configuration procedures

**Deliverables**:
- Extended `dmva_column_info` table
- New `dmva_masking_config` table
- Three core masking functions (string, number, date)
- Basic configuration procedures

**Success Criteria**:
- All database objects created successfully
- Masking functions pass unit tests
- Basic configuration workflow functional

### **Phase 2: Query Generation (Weeks 3-4)**
- Teradata SQL generation enhancement
- Query transformation logic
- Integration with existing unload task generation

**Deliverables**:
- `TeradataMaskingGenerator` class
- Modified `dmva_get_unload_tasks` procedure
- Teradata-specific masking SQL expressions

**Success Criteria**:
- Masked queries generate correctly
- Teradata-only restriction enforced
- Integration with existing workflow seamless

---

## 🎯 **Success Criteria and Acceptance Tests**

### **Functional Requirements**
- ✅ **Column-Level Masking**: Users can specify individual columns for masking
- ✅ **Three Data Type Functions**: Dedicated masking for strings, numbers, and dates
- ✅ **Teradata-Only Application**: Masking only applies to Teradata source systems
- ✅ **No Unmasked Storage**: Data is never stored unmasked in any location
- ✅ **Configuration Flexibility**: Support for both manual and pattern-based configuration

### **Performance Requirements**
- ✅ **Minimal Overhead**: Masking adds <10% to query execution time
- ✅ **Scalability**: Handles large tables (100M+ rows) without significant degradation
- ✅ **Parallel Processing**: Maintains existing parallelization capabilities
- ✅ **Memory Efficiency**: No significant increase in memory usage

### **Security Requirements**
- ✅ **Data Protection**: No sensitive data exposed in logs, temporary files, or error messages
- ✅ **Access Control**: Masking configuration restricted to authorized users
- ✅ **Audit Trail**: Complete logging of masking operations and configuration changes
- ✅ **Compliance**: Meets data privacy regulations (GDPR, HIPAA, etc.)

---

This comprehensive implementation plan provides a roadmap for successfully implementing in-flight data masking capabilities in DMVA V2 while maintaining security, performance, and operational requirements. The phased approach ensures systematic development and validation of each component before integration into the production environment.
