# CSEL & CCODS DBT Projects - Text-Only Version

## Overview

This repository contains two complementary data pipeline projects implemented using DBT (Data Build Tool) and deployed within Snowflake:

- **CSEL (Commonwealth Bank Service Layer)**: Processes customer service data, appointments, products, and department information through 18 sequential transformations
- **CCODS (Commonwealth Bank Operations Data System)**: Processes BCFINSG data and populates the PLAN_BALN_SEGM_MSTR table through 2 sequential model groups

Both projects share the same Snowflake infrastructure and deployment framework while serving different business domains.

---

## 🏗️ **CSEL Project**

### CSEL Execution Flow (18 Steps)

**Execution Schedule**: Daily at 3:00 AM Australia/Sydney

The CSEL process executes 18 sequential steps organized into four distinct phases:

#### Phase 1: Control Setup (Steps 1-5)
1. **Process Stream Status Check** - Validates stream processing readiness
2. **Util Pros ISAC Previous Load Check** - Verifies previous load completion
3. **Load GDW Process Key Sequence** - Generates process keys for the current run
4. **Load Mapping Lookups** - Populates mapping and lookup tables
5. **Process Stream Finishing Point** - Sets stream processing boundaries

#### Phase 2: Data Processing (Steps 6-12)
6. **Process Stream Status Check (CSE_CPL_BUS_APP)** - Validates source data availability
7. **Extract Applications** - Extracts application data from source systems
8. **Transform Application Data** - Applies business rules and transformations
9. **Load Appointment Department Temp** - Loads data to temporary staging tables
10. **Delta Processing (Appointment Departments)** - Identifies changes since last run
11. **Update Appointment Department Data** - Updates existing records with changes
12. **Insert Appointment Department Data** - Inserts new records

#### Phase 3: Product Processing (Steps 13-16)
13. **Load Appointment Product Temp** - Loads product data to temporary tables
14. **Delta Processing (Appointment Products)** - Identifies product changes
15. **Update Appointment Product Data** - Updates existing product records
16. **Insert Appointment Product Data** - Inserts new product records

#### Phase 4: Final Loading (Steps 17-18)
17. **Update Department Appointment Data** - Final updates to department appointments
18. **Insert Department Appointment Data** - Final inserts to department appointments

**Success Criteria**: All 18 steps must complete successfully for the process to be marked as complete.

**Error Handling**: Any step failure triggers error logging and process termination.

---

## 🏗️ **CCODS Project**

### CCODS Execution Flow (2 Steps)

**Execution Schedule**: Daily at 4:00 AM Australia/Sydney (1 hour after CSEL)

The CCODS process executes 2 sequential steps with a dependency on CSEL completion:

#### Step 1: Transform BCFINSG Data
- **Location**: `models/ccods/40_transform/xfmplanbalnsegmmstrfrombcfinsg/`
- **Purpose**: Transforms raw BCFINSG data according to business requirements
- **Activities**: Data cleansing, validation, structuring, and business rule application
- **Output**: Transformed data ready for GDW loading

#### Step 2: Load to GDW
- **Location**: `models/ccods/60_load_gdw/ldbcfinsgplanbalnsegmmstr/`
- **Purpose**: Loads transformed data to the final GDW table
- **Target Table**: `PLAN_BALN_SEGM_MSTR_NPW`
- **Activities**: Final data validation, loading, and audit trail creation

**Success Criteria**: Both steps must complete successfully to populate the target table.

**Error Handling**: Step 1 failure prevents Step 2 execution; Step 2 failure leaves partial data.

---

## 🏛️ **Shared Infrastructure**

### Snowflake Environment Architecture

The projects operate within a comprehensive Snowflake environment consisting of multiple databases, schemas, and external systems.

#### Primary Database: NPD_D12_DMN_GDWMIG
**Schema**: TMP

**Stored Procedures**:
- `P_EXECUTE_DBT_CSEL` - Orchestrates the 18-step CSEL process
- `P_EXECUTE_DBT_CCODS` - Orchestrates the 2-step CCODS process

**Scheduled Tasks**:
- `T_EXECUTE_DBT_CSEL` - Executes daily at 3:00 AM Australia/Sydney
- `T_EXECUTE_DBT_CCODS` - Executes daily at 4:00 AM Australia/Sydney

**Shared Resources**:
- `GDW1_DBT` - Shared DBT workspace containing all models for both projects

#### Secondary Database: NPD_D12_DMN_GDWMIG_IBRG_V
**Schema**: P_V_OUT_001_STD_0

**Audit and Control Tables**:
- `DCF_T_EXEC_LOG` - Unified audit table tracking execution status for both projects
- `RUN_STRM_TMPL` - Control table managing stream processing templates

**Model Outputs**:
- **CSEL Models**: Appointment tables, product tables, department tables
- **CCODS Models**: BCFINSG transformations, PLAN_BALN_SEGM_MSTR table

#### External Systems
- **Snowflake DBT Workspace** - Development and execution environment
- **Compute Warehouse**: `wh_usr_npd_d12_gdwmig_001` - Processing power allocation

### Data Flow Timeline
The infrastructure supports a sequential execution pattern:
1. **3:00 AM** - CSEL process begins (18 steps)
2. **1-hour buffer** - Ensures CSEL completion before CCODS
3. **4:00 AM** - CCODS process begins (2 steps)
4. **Completion** - Both processes finished, data available for consumption

---

## 📊 **Monitoring and Troubleshooting**

### Comprehensive Monitoring Strategy

The monitoring approach encompasses three complementary methods to ensure complete visibility into both processes.

#### Method 1: DBT Workspace Monitoring
**Purpose**: Real-time status monitoring and immediate issue identification

**Tools and Commands**:
- `SHOW TASKS` - Displays current task status and schedules
- `SHOW TASK RUNS` - Shows recent task execution history
- Snowflake DBT Workspace interface - Visual monitoring dashboard

**Benefits**: 
- Immediate visibility into task status
- Real-time error detection
- Direct access to execution logs

#### Method 2: Query History Monitoring
**Purpose**: Historical analysis and trend identification

**Functions and Queries**:
- `TASK_HISTORY()` - Retrieves detailed task execution history
- `RESULT_SCAN()` - Accesses results from previous executions
- Custom queries on `INFORMATION_SCHEMA.TASK_HISTORY`

**Benefits**:
- Historical trend analysis
- Performance monitoring over time
- Detailed execution statistics

#### Method 3: Unified Audit Monitoring
**Purpose**: Process-specific detailed logging and cross-process analysis

**Primary Table**: `DCF_T_EXEC_LOG`
- Captures detailed execution logs for both CSEL and CCODS
- Provides step-by-step execution status
- Enables comparative analysis between processes

**Query Capabilities**:
- Multi-process status queries
- Step-level execution tracking
- Error analysis and troubleshooting
- Performance metrics and timing analysis

### Monitoring Outputs

#### Real-time Status
- Current execution status for both projects
- Active step identification
- Immediate error alerts

#### Historical Analysis
- Execution trends over time
- Performance comparisons
- Success/failure patterns

#### Detailed Process Logs
- Step-by-step execution details
- Error messages and stack traces
- Process-specific metrics and timings

---

## 🔧 **Installation and Deployment**

### Sequential Installation Process

The installation follows a carefully orchestrated sequence to ensure proper dependencies and operational readiness.

#### Phase 1: CSEL Foundation (Steps 1-4)

**Step 1: CSEL Database Setup**
- Execute database creation scripts
- Create required tables and views
- Set up initial schema structure
- **Result**: ✅ Tables & Views Created

**Step 2: CSEL Control Data Population**
- Populate reference data tables
- Insert stream template entries
- Configure ETL processing dates
- **Result**: ✅ Reference Data Populated

**Step 3: CSEL Procedure Deployment**
- Deploy `P_EXECUTE_DBT_CSEL` stored procedure
- Configure 18-step execution logic
- Implement error handling and logging
- **Result**: ✅ P_EXECUTE_DBT_CSEL Created

**Step 4: CSEL Task Deployment**
- Create `T_EXECUTE_DBT_CSEL` scheduled task
- Configure 3:00 AM Australia/Sydney schedule
- Set up task dependencies and error handling
- **Result**: ✅ T_EXECUTE_DBT_CSEL Created

#### Phase 2: CCODS Implementation (Steps 5-6)

**Step 5: CCODS Procedure Deployment**
- Deploy `P_EXECUTE_DBT_CCODS` stored procedure
- Configure 2-step execution logic
- Implement CSEL dependency checking
- **Result**: ✅ P_EXECUTE_DBT_CCODS Created

**Step 6: CCODS Task Deployment**
- Create `T_EXECUTE_DBT_CCODS` scheduled task
- Configure 4:00 AM Australia/Sydney schedule
- Set up 1-hour buffer after CSEL
- **Result**: ✅ T_EXECUTE_DBT_CCODS Created

#### Phase 3: Shared Infrastructure (Steps 7-9)

**Step 7: DBT Project Upload**
- Upload shared DBT project to Snowflake workspace
- Configure model dependencies and relationships
- Validate project structure and connections
- **Result**: ✅ GDW1_DBT Workspace Ready

**Step 8: CSEL Task Activation**
- Resume CSEL task for operational execution
- Validate schedule and dependencies
- Perform initial test execution
- **Result**: ✅ CSEL Task Operational

**Step 9: CCODS Task Activation**
- Resume CCODS task for operational execution
- Validate CSEL dependency and timing
- Perform end-to-end test execution
- **Result**: ✅ Both Projects Operational

### Post-Installation Validation

#### Verification Checklist
- [ ] Both tasks appear in `SHOW TASKS` output
- [ ] Task schedules are correctly configured
- [ ] DBT workspace contains all required models
- [ ] Audit tables are receiving execution logs
- [ ] Test executions complete successfully
- [ ] Monitoring queries return expected results

#### Operational Readiness
Once all installation steps are complete, both projects operate autonomously according to their schedules, with comprehensive monitoring and error handling ensuring reliable daily execution.

---

## 📚 **Project Structure and Organization**

### Shared DBT Project Layout

The projects share a common DBT workspace with organized model directories:

#### CSEL Models (18-Step Process)
- `cse_dataload/02processkey/` - Process key generation and management
- `cse_dataload/04MappingLookupSets/` - Mapping tables and lookup sets
- `cse_dataload/08extraction/` - Data extraction from source systems
- `cse_dataload/12MappingTransformation/` - Data transformation and business rules
- `cse_dataload/14loadtotemp/` - Temporary staging table loading
- `cse_dataload/16transformdelta/` - Delta processing and change capture
- `cse_dataload/18loadtogdw/` - Final GDW loading
- `cse_dataload/24processmetadata/` - Process metadata management
- `appt_pdct/` - Appointment and product specific models

#### CCODS Models (2-Step Process)
- `ccods/40_transform/` - BCFINSG data transformation layer
- `ccods/60_load_gdw/` - Final GDW loading for PLAN_BALN_SEGM_MSTR

### Configuration and Deployment
- **Project Name**: `np_projects_commbank_sf_dbt`
- **Target Database**: `NPD_D12_DMN_GDWMIG_IBRG_V`
- **Materialization**: Views (default configuration)
- **Execution Method**: Snowflake stored procedures with scheduled tasks

---

*This README demonstrates comprehensive text-only documentation that maintains clarity and completeness without requiring any visual diagrams or external rendering tools.* 