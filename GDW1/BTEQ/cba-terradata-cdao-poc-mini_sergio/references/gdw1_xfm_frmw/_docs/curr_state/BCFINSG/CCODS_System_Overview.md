# CCODS System Overview

## Documentation Sources

This comprehensive documentation was created by analyzing the following key files from the CCODS system:

### **📋 Primary Configuration Files:**
- **`CCODS.param`** (937 lines) - Master parameter file with stream configurations, database connections, and performance settings
- **`CCODS.crypt`** - Encrypted password file for secure database authentication
- **`CCODS.key`** - Encryption key file for password decryption

### **🔧 Core Shell Scripts:**
- **`initiate_job`** (150 lines) - Main job execution framework and parameter loading
- **`file_watcher_ccods.sh`** (77 lines) - Generic file monitoring with timeout handling
- **`file_watcher_ccods_1_ctl.sh`** (82 lines) - Control file-based file watcher
- **`ahl_file_watcher_ccods_ctl.sh`** (84 lines) - AHL-specific file monitoring
- **`ahl_ext_dependancy.sh`** (71 lines) - AHL dependency checker with database validation
- **`reset_autosys_jobs`** (49 lines) - Job reset and recovery operations
- **`Catchup.sh`** (117 lines) - Failure recovery and catchup mechanism
- **`setup_job`** (124 lines) - Stream setup and configuration management
- **`UtilPros_Check.sh`** (43 lines) - Utility process status checking

### **📂 Stream Configuration Files (.cnf):**
- **Primary Streams:** `BCFINSG.cnf`, `BCMASTER.cnf`
- **Segment Streams:** `SEG01.cnf` through `SEG04.cnf`, `SEG51.cnf` through `SEG54.cnf`  
- **AHL Streams:** `AHLEXT.cnf`, `AHLFINSG.cnf`, `AHLMASTER.cnf`, `AHLNAME.cnf`
- **Auxiliary Streams:** `AHLAUX2.cnf`, `AHLAUX3.cnf`, `AHLAUX4.cnf`, `AHLAUX51.cnf` through `AHLAUX54.cnf`

### **🔄 Utility Scripts:**
- **`UtilPros_Update.sh`** (67 lines) - Process status updates
- **`bcmname_file_watcher_ccods_6_ctl.sh`** (103 lines) - Specialized file monitoring
- **`aux_file_watcher_ccods_1_ctl.sh`** (86 lines) - Auxiliary file watching
- **`ccods_publ_hldy_gen.ksh`** (108 lines) - Holiday calendar generation

### **🗄️ DataStage Integration:**
- **`ds_profile`** (90 lines) - DataStage environment setup and configuration
- **`ds_common.func`** (102 lines) - Common DataStage functions

---

## Introduction

**CCODS** (Commonwealth Bank Credit Card Operations Data System) is a comprehensive ETL (Extract, Transform, Load) data processing system designed for Commonwealth Bank of Australia's credit card and customer data management operations. The system is built on IBM DataStage and manages critical data flows between source systems and the bank's Teradata-based Operational Data Store (ODS).

## System Architecture

### High-Level Component Architecture

```mermaid
graph TB
    subgraph "Source Systems"
        MF[Mainframe Card Systems]
        CMS[Customer Management]
        FTS[Financial Transaction Systems]
    end
    
    subgraph "CCODS ETL Platform"
        DS[IBM DataStage Engine]
        FW[File Watchers]
        JS[Job Schedulers]
        PM[Parameter Management]
    end
    
    subgraph "File Processing Layer"
        IB[Inbound Directory]
        IP[In-Process Directory]
        ST[Staging Directory]
        AR[Archive Directory]
        RJ[Reject Directory]
    end
    
    subgraph "Teradata ODS"
        PDSRCCS[(PDSRCCS - Load DB)]
        PPSRCCS[(PPSRCCS - Process DB)]
        PVTECH[(PVTECH - Calendar DB)]
        PUTIL[(PUTIL - Utility DB)]
    end
    
    subgraph "Downstream Systems"
        GDW[Group Data Warehouse]
        ARS[Analytics & Reporting]
        RMS[Risk Management]
        CAS[Customer Analytics]
    end
    
    subgraph "Monitoring & Notifications"
        EM[Email Notifications]
        LG[Logging System]
        AS[Autosys Scheduling]
    end
    
    MF --> IB
    CMS --> IB
    FTS --> IB
    
    IB --> FW
    FW --> DS
    DS --> IP
    IP --> ST
    ST --> PDSRCCS
    ST --> PPSRCCS
    
    DS --> PM
    PM --> PVTECH
    PM --> PUTIL
    
    PDSRCCS --> GDW
    PPSRCCS --> ARS
    PDSRCCS --> RMS
    PPSRCCS --> CAS
    
    FW --> EM
    DS --> LG
    JS --> AS
    
    ST --> AR
    ST --> RJ
```

## Job Orchestration and Flow

### Daily Processing Cycle Overview

```mermaid
flowchart LR
    subgraph "6:00 AM - Morning"
        A1["📁 File Arrival Window Opens"]
        A2["👁️ File Watchers Start Monitoring"] 
        A3["📊 BCFINSG Files Expected"]
        A4["💳 BCMASTER Files Expected"]
    end
    
    subgraph "8:00 AM - Mid-Morning"
        B1["🚀 Primary Loads Start"]
        B2["📊 BCFINSG Stream Initiated"]
        B3["💳 BCMASTER Stream Initiated"]
        B4["📋 SEG01-04 Streams Triggered"]
        B5["✅ File Validation & Processing"]
    end
    
    subgraph "10:00 AM - Late Morning"
        C1["🔗 Auxiliary Processing"]
        C2["🏥 AHL Streams Wait for Dependencies"]
        C3["📑 SEG51-54 Auxiliary Segments"]
        C4["⚠️ Error Handling for Missing Files"]
        C5["🔄 Retry Mechanisms Activated"]
    end
    
    subgraph "2:00 PM - Afternoon"
        D1["⬇️ Downstream Triggers"]
        D2["🏢 GDW Extract Jobs Start"]
        D3["📈 Analytics Processing Begins"]
        D4["📦 Archive & Cleanup Operations"]
        D5["📧 Success/Failure Notifications"]
    end
    
    subgraph "6:00 PM - Evening"
        E1["🔍 End of Day"]
        E2["📊 Final Reconciliation"]
        E3["📋 Report Generation"]
        E4["🔄 Next Day Setup"]
        E5["🔧 Job Reset for Next Cycle"]
    end
    
    A1 --> A2 --> A3 --> A4
    A4 --> B1 --> B2 --> B3 --> B4 --> B5
    B5 --> C1 --> C2 --> C3 --> C4 --> C5
    C5 --> D1 --> D2 --> D3 --> D4 --> D5
    D5 --> E1 --> E2 --> E3 --> E4 --> E5
    
    classDef morning fill:#ff9800,stroke:#e65100,stroke-width:3px,color:#000000,font-weight:bold,font-size:12px
    classDef midMorning fill:#4caf50,stroke:#2e7d32,stroke-width:3px,color:#ffffff,font-weight:bold,font-size:12px
    classDef lateMorning fill:#2196f3,stroke:#1565c0,stroke-width:3px,color:#ffffff,font-weight:bold,font-size:12px
    classDef afternoon fill:#9c27b0,stroke:#6a1b9a,stroke-width:3px,color:#ffffff,font-weight:bold,font-size:12px
    classDef evening fill:#e91e63,stroke:#ad1457,stroke-width:3px,color:#ffffff,font-weight:bold,font-size:12px
    
    class A1,A2,A3,A4 morning
    class B1,B2,B3,B4,B5 midMorning
    class C1,C2,C3,C4,C5 lateMorning
    class D1,D2,D3,D4,D5 afternoon
    class E1,E2,E3,E4,E5 evening
```

### Detailed Daily Processing Flow

```mermaid
graph TB
    subgraph "Daily Processing Cycle - CCODS Job Orchestration"
        
        subgraph "6:00 AM - File Arrival Window"
            FA1[Source Systems Send Files]
            FA2[File Watchers Start Monitoring]
            FA3[BCFINSG Files Expected]
            FA4[BCMASTER Files Expected]
            FA5[AHL Files Expected]
        end
        
        subgraph "File Detection & Validation"
            FW1[file_watcher_ccods.sh]
            FW2[file_watcher_ccods_1_ctl.sh]
            FW3[ahl_file_watcher_ccods_ctl.sh]
            VAL[File Format Validation]
            TIMEOUT[Timeout Alert System]
        end
        
        subgraph "8:00 AM - Autosys Job Triggering"
            AS1[sendevent -E SET_GLOBAL file_arrived=Y]
            AS2[autorep -j job_mask dependency check]
            AS3[sendevent -E FORCE_STARTJOB]
            AS4[Job Dependency Resolution]
        end
        
        subgraph "DataStage ETL Execution"
            IJ[initiate_job stream_name sequence_name]
            PROF[Load .cnf Profile & CCODS.param]
            CRYPT[Decrypt CCODS.crypt Passwords]
            DS[DataStage Sequence Execution]
        end
        
        subgraph "Primary Stream Processing"
            BCFINSG[BCFINSG → PLAN_BALN_SEGM_MSTR]
            BCMASTER[BCMASTER → CAHD_MST1]
            SEG01[SEG01 → CAHD_MSTR_AUX_1]
            SEG02[SEG02 → CAHD_MSTR_AUX_2]
        end
        
        subgraph "10:00 AM - Auxiliary Stream Dependencies"
            DEP_CHECK[ahl_ext_dependancy.sh]
            UTIL_CHECK[Check UTIL_PROS_ISAC Table]
            AHL_TRIGGER[Trigger AHL Streams]
        end
        
        subgraph "Error Handling & Recovery"
            ERR_LOG[Log to UTIL_TRSF_EROR_RQM3]
            EMAIL_ALERT[Send Failure Notifications]
            RESET[reset_autosys_jobs.sh]
            HOLD[sendevent -E JOB_ON_HOLD]
        end
        
        subgraph "2:00 PM - Downstream Processing"
            ARCHIVE[Archive Source Files]
            GDW_TRIGGER[Trigger GDW Extracts]
            ANALYTICS[Analytics Processing]
            CLEANUP[Cleanup Operations]
        end
        
        subgraph "6:00 PM - End of Day"
            RECONCILE[Final Reconciliation]
            REPORTS[Generate Reports]
            NEXT_SETUP[Next Day Setup]
            CATCHUP[Catchup.sh for Failures]
        end
    end
    
    %% Flow connections
    FA1 --> FW1
    FA3 --> FW1
    FA4 --> FW2
    FA5 --> FW3
    
    FW1 --> VAL
    FW2 --> VAL
    FW3 --> VAL
    VAL --> AS1
    VAL --> TIMEOUT
    
    AS1 --> AS2
    AS2 --> AS3
    AS3 --> AS4
    AS4 --> IJ
    
    IJ --> PROF
    PROF --> CRYPT
    CRYPT --> DS
    
    DS --> BCFINSG
    DS --> BCMASTER
    DS --> SEG01
    DS --> SEG02
    
    BCFINSG --> DEP_CHECK
    BCMASTER --> DEP_CHECK
    DEP_CHECK --> UTIL_CHECK
    UTIL_CHECK --> AHL_TRIGGER
    
    DS --> ERR_LOG
    ERR_LOG --> EMAIL_ALERT
    EMAIL_ALERT --> RESET
    RESET --> HOLD
    
    BCFINSG --> ARCHIVE
    BCMASTER --> ARCHIVE
    ARCHIVE --> GDW_TRIGGER
    GDW_TRIGGER --> ANALYTICS
    ANALYTICS --> CLEANUP
    
    CLEANUP --> RECONCILE
    RECONCILE --> REPORTS
    REPORTS --> NEXT_SETUP
    ERR_LOG --> CATCHUP
    
    %% Styling
    classDef fileWatcher fill:#0277bd,stroke:#01579b,stroke-width:3px,color:#ffffff,font-weight:bold,font-size:11px
    classDef autosys fill:#ff8f00,stroke:#e65100,stroke-width:3px,color:#000000,font-weight:bold,font-size:11px
    classDef datastage fill:#7b1fa2,stroke:#4a148c,stroke-width:3px,color:#ffffff,font-weight:bold,font-size:11px
    classDef database fill:#388e3c,stroke:#2e7d32,stroke-width:3px,color:#ffffff,font-weight:bold,font-size:11px
    classDef error fill:#d32f2f,stroke:#b71c1c,stroke-width:3px,color:#ffffff,font-weight:bold,font-size:11px
    classDef success fill:#00796b,stroke:#004d40,stroke-width:3px,color:#ffffff,font-weight:bold,font-size:11px
    
    class FW1,FW2,FW3,VAL fileWatcher
    class AS1,AS2,AS3,AS4,RESET,HOLD autosys
    class IJ,PROF,CRYPT,DS datastage
    class BCFINSG,BCMASTER,SEG01,SEG02 database
    class ERR_LOG,EMAIL_ALERT,TIMEOUT error
    class ARCHIVE,GDW_TRIGGER,ANALYTICS,CLEANUP success
```

### Autosys Job Orchestration Flow

```mermaid
graph TD
    subgraph "Autosys Scheduler"
        AS_MAIN[Main Autosys Controller]
        AS_WATCHER[File Watcher Jobs]
        AS_ETL[ETL Processing Jobs]
        AS_POST[Post-Processing Jobs]
        AS_CLEANUP[Cleanup Jobs]
    end
    
    subgraph "Job Control Commands"
        CMD1[sendevent -E FORCE_STARTJOB]
        CMD2[sendevent -E JOB_ON_HOLD]
        CMD3[sendevent -E JOB_OFF_HOLD]
        CMD4[sendevent -E SET_GLOBAL]
        CMD5[sendevent -E CHANGE_STATUS]
    end
    
    subgraph "Job Dependencies"
        DEP1[File Arrival Dependencies]
        DEP2[Stream Completion Dependencies]
        DEP3[Database Status Dependencies]
        DEP4[Global Variable Dependencies]
    end
    
    AS_MAIN --> AS_WATCHER
    AS_WATCHER --> AS_ETL
    AS_ETL --> AS_POST
    AS_POST --> AS_CLEANUP
    
    AS_WATCHER --> CMD1
    AS_ETL --> CMD2
    AS_ETL --> CMD3
    AS_POST --> CMD4
    AS_CLEANUP --> CMD5
    
    DEP1 --> AS_WATCHER
    DEP2 --> AS_ETL
    DEP3 --> AS_POST
    DEP4 --> AS_CLEANUP
```

### Detailed Job Triggering Mechanism

```mermaid
sequenceDiagram
    participant FS as File System
    participant FW as File Watcher
    participant AS as Autosys
    participant IJ as initiate_job
    participant DS as DataStage
    participant TD as Teradata
    participant NS as Notification System
    
    FS->>FW: File arrives in inbound directory
    FW->>FW: Monitor file for completeness
    FW->>AS: Set global variable (file_arrived=Y)
    AS->>AS: Check job dependencies
    AS->>IJ: Trigger job (stream_name, sequence_name)
    
    IJ->>IJ: Load stream configuration (.cnf)
    IJ->>IJ: Parse CCODS.param parameters
    IJ->>IJ: Decrypt passwords from CCODS.crypt
    IJ->>DS: Execute DataStage sequence
    
    DS->>TD: Connect to database
    DS->>TD: Begin transaction
    DS->>TD: Execute ETL operations
    
    alt Success Path
        TD->>DS: Commit transaction
        DS->>IJ: Return success status
        IJ->>AS: Job completed successfully
        AS->>NS: Send success notification
        AS->>AS: Trigger dependent jobs
    else Failure Path
        TD->>DS: Rollback transaction
        DS->>IJ: Return error status
        IJ->>AS: Job failed
        AS->>NS: Send failure notification
        AS->>AS: Put dependent jobs on hold
    end
```

### Stream Processing Dependencies

```mermaid
graph TB
    subgraph "File Arrival Layer"
        F1[BCFINSG Files]
        F2[BCMASTER Files]
        F3[BCMSTAUX Files]
        F4[AHL Files]
    end
    
    subgraph "Primary Processing Streams"
        S1[BCFINSG Stream]
        S2[BCMASTER Stream]
        S3[SEG01 Stream]
        S4[SEG02 Stream]
        S5[SEG03 Stream]
        S6[SEG04 Stream]
    end
    
    subgraph "Auxiliary Processing Streams"
        A1[SEG51 Stream]
        A2[SEG52 Stream]
        A3[SEG53 Stream]
        A4[SEG54 Stream]
    end
    
    subgraph "AHL Processing Streams"
        H1[AHLEXT Stream]
        H2[AHLFINSG Stream]
        H3[AHLMASTER Stream]
        H4[AHLNAME Stream]
        H5[AHLAUX2 Stream]
        H6[AHLAUX53 Stream]
        H7[AHLAUX54 Stream]
    end
    
    subgraph "Dependency Checker"
        DC[AHL Dependency Script]
        DC_QUERY[Check UTIL_PROS_ISAC table]
    end
    
    F1 --> S1
    F2 --> S2
    F3 --> S3
    F3 --> S4
    F3 --> S5
    F3 --> S6
    
    S1 --> A1
    S2 --> A2
    S3 --> A3
    S4 --> A4
    
    F4 --> H2
    F4 --> H3
    F4 --> H4
    F4 --> H5
    F4 --> H6
    F4 --> H7
    
    H2 --> DC
    H3 --> DC
    H4 --> DC
    H5 --> DC
    H6 --> DC
    H7 --> DC
    
    DC --> DC_QUERY
    DC_QUERY --> H1
```

### File Watcher Triggering Logic

```mermaid
flowchart TD
    START[File Watcher Started] --> INIT[Initialize Parameters]
    INIT --> PARAMS[Parse wait_time, wait_interval, file_pattern]
    PARAMS --> LOOP[Start Monitoring Loop]
    
    LOOP --> CHECK{Check for Files}
    CHECK -->|Files Found| VALIDATE[Validate File Format]
    CHECK -->|No Files| WAIT[Sleep for wait_interval]
    
    WAIT --> TIMEOUT{Timeout Reached?}
    TIMEOUT -->|No| CHECK
    TIMEOUT -->|Yes| ALERT[Send Missing File Alert]
    
    VALIDATE -->|Valid| TRIGGER[Trigger Autosys Job]
    VALIDATE -->|Invalid| REJECT[Move to Reject Directory]
    
    TRIGGER --> GLOBAL[Set Global Variable: file_arrived=Y]
    GLOBAL --> SUCCESS[Exit Success]
    
    REJECT --> ERROR_EMAIL[Send Error Notification]
    ALERT --> ERROR_EMAIL
    ERROR_EMAIL --> FAILURE[Exit Failure]
    
    SUCCESS --> END[End Process]
    FAILURE --> END
```

### Job Reset and Recovery Flow

```mermaid
graph TD
    subgraph "Reset Triggers"
        RT1[Manual Reset Request]
        RT2[System Failure Recovery]
        RT3[Daily Cycle Reset]
        RT4[Emergency Restart]
    end
    
    subgraph "Reset Operations"
        RO1[Put Jobs on Hold]
        RO2[Inactivate Jobs]
        RO3[Clear Global Variables]
        RO4[Reset File Watchers]
        RO5[Archive Previous Logs]
    end
    
    subgraph "Recovery Operations"
        RC1[Validate Data Integrity]
        RC2[Reactivate Jobs]
        RC3[Set New Processing Date]
        RC4[Restart File Watchers]
        RC5[Send Recovery Notifications]
    end
    
    subgraph "Autosys Commands"
        AC1[sendevent -E JOB_ON_HOLD]
        AC2[sendevent -E CHANGE_STATUS -s INACTIVE]
        AC3[sendevent -E SET_GLOBAL]
        AC4[sendevent -E JOB_OFF_HOLD]
        AC5[sendevent -E FORCE_STARTJOB]
    end
    
    RT1 --> RO1
    RT2 --> RO1
    RT3 --> RO1
    RT4 --> RO1
    
    RO1 --> AC1
    RO2 --> AC2
    RO3 --> AC3
    
    RO1 --> RO2
    RO2 --> RO3
    RO3 --> RO4
    RO4 --> RO5
    
    RO5 --> RC1
    RC1 --> RC2
    RC2 --> RC3
    RC3 --> RC4
    RC4 --> RC5
    
    RC2 --> AC4
    RC4 --> AC5
```

### Stream-Specific Job Execution Patterns

```mermaid
graph LR
    subgraph "BCFINSG Stream Jobs"
        BF_START[File Watcher: BCFINSG_C*]
        BF_ETL[DataStage: BCFINSG Load]
        BF_VALIDATE[Validate: PLAN_BALN_SEGM_MSTR]
        BF_ARCHIVE[Archive Source Files]
        BF_NOTIFY[Success Notification]
    end
    
    subgraph "BCMASTER Stream Jobs"
        BM_START[File Watcher: BCMASTER_C*]
        BM_ETL[DataStage: BCMASTER Load]
        BM_VALIDATE[Validate: CAHD_MST1]
        BM_ARCHIVE_TBL[Archive to CAHD_MST1_ARCH]
        BM_NOTIFY[Success Notification]
    end
    
    subgraph "SEG Stream Jobs"
        SEG_START[File Watcher: BCMSTAUX_SEG*]
        SEG_ETL[DataStage: SEG Load]
        SEG_VALIDATE[Validate: CAHD_MSTR_AUX_*]
        SEG_ERROR[Log to UTIL_TRSF_EROR_RQM3]
        SEG_NOTIFY[Business Notification]
    end
    
    subgraph "AHL Stream Jobs"
        AHL_DEP[Dependency Check]
        AHL_START[File Watcher: AHL Files]
        AHL_ETL[DataStage: AHL Load]
        AHL_VALIDATE[Validate: AHL Tables]
        AHL_NOTIFY[Analytics Team Notification]
    end
    
    BF_START --> BF_ETL --> BF_VALIDATE --> BF_ARCHIVE --> BF_NOTIFY
    BM_START --> BM_ETL --> BM_VALIDATE --> BM_ARCHIVE_TBL --> BM_NOTIFY
    SEG_START --> SEG_ETL --> SEG_VALIDATE --> SEG_ERROR --> SEG_NOTIFY
    AHL_DEP --> AHL_START --> AHL_ETL --> AHL_VALIDATE --> AHL_NOTIFY
```

### Error Handling and Recovery Patterns

```mermaid
stateDiagram-v2
    [*] --> FileWatching: Start file monitoring
    
    FileWatching --> FileFound: File detected
    FileWatching --> FileTimeout: Timeout reached
    FileWatching --> SystemError: System failure
    
    FileFound --> FileValidation: Validate file format
    FileValidation --> JobTriggered: File valid
    FileValidation --> FileRejected: File invalid
    
    JobTriggered --> ETLProcessing: DataStage execution
    ETLProcessing --> ETLSuccess: Processing complete
    ETLProcessing --> ETLFailure: Processing failed
    
    ETLSuccess --> Archival: Archive files
    ETLSuccess --> Notification: Send success email
    
    Archival --> [*]: Complete
    Notification --> [*]: Complete
    
    FileTimeout --> AlertSent: Send missing file alert
    FileRejected --> ManualReview: Human intervention
    ETLFailure --> ErrorLogging: Log error details
    SystemError --> AutoRecovery: Attempt recovery
    
    AlertSent --> ManualReview
    ErrorLogging --> ManualReview
    AutoRecovery --> FileWatching: Retry
    AutoRecovery --> ManualReview: Recovery failed
    
    ManualReview --> FileWatching: Issue resolved
    ManualReview --> [*]: Manual completion
```

## Data Streams and Processing

### Primary Data Streams Overview

```mermaid
graph LR
    subgraph "BCFINSG Stream"
        BF1[BCFINSG_CA Files] --> BFT[PLAN_BALN_SEGM_MSTR]
        BF2[BCFINSG_CB Files] --> BFT
        BF3[BCFINSG_CC Files] --> BFT
        BF4[BCFINSG_CD Files] --> BFT
        BF5[BCFINSG_CE Files] --> BFT
        BF6[BCFINSG_CF Files] --> BFT
    end
    
    subgraph "BCMASTER Stream"
        BM1[BCMASTER_C1 Files] --> BMT[CAHD_MST1]
        BM2[BCMASTER_C2 Files] --> BMT
        BM3[BCMASTER_C3 Files] --> BMT
        BM4[BCMASTER_C4 Files] --> BMT
        BM5[BCMASTER_C5 Files] --> BMT
        BM6[BCMASTER_C6 Files] --> BMT
    end
    
    subgraph "SEG Streams"
        S1[SEG01 Files] --> ST1[CAHD_MSTR_AUX_1]
        S2[SEG02 Files] --> ST2[CAHD_MSTR_AUX_2]
        S3[SEG03 Files] --> ST3[CAHD_MSTR_AUX_3]
        S4[SEG04 Files] --> ST4[CAHD_MSTR_AUX_4]
    end
    
    subgraph "AHL Streams"
        AHL1[AHLEXT Files] --> AHLT[AHL Tables]
        AHL2[AHLFINSG Files] --> AHLT
        AHL3[AHLMASTER Files] --> AHLT
        AHL4[AHLNAME Files] --> AHLT
    end
```

### Detailed Stream Processing

#### 1. BCFINSG Stream
- **Purpose**: Plan Balance Segment Master data processing
- **Target Table**: `PLAN_BALN_SEGM_MSTR`
- **File Pattern**: `BCFINSG_C*`
- **File Types**: BCFINSG_CA, BCFINSG_CB, BCFINSG_CC, BCFINSG_CD, BCFINSG_CE, BCFINSG_CF
- **Database User**: PUCCS001
- **Target Schema**: PDSRCCS

#### 2. BCMASTER Stream
- **Purpose**: Card Master data processing
- **Target Table**: `CAHD_MST1`
- **File Pattern**: `BCMASTER_C*`
- **File Types**: BCMASTER_C1, BCMASTER_C2, BCMASTER_C3, BCMASTER_C4, BCMASTER_C5, BCMASTER_C6
- **Database User**: PUCCS003
- **Archive Support**: `CAHD_MST1_ARCH`

#### 3. SEG Streams (SEG01-SEG04, SEG51-SEG54)
- **Purpose**: Auxiliary master data segments
- **Target Tables**: `CAHD_MSTR_AUX_1`, `CAHD_MSTR_AUX_2`, `CAHD_MSTR_AUX_3`
- **File Patterns**: Various `BCMSTAUX_SEG*` patterns
- **Database Users**: PUCCS004, PUCCS005, PUCCS006
- **Error Handling**: `UTIL_TRSF_EROR_RQM3` error table

#### 4. AHL Streams
- **Purpose**: Auxiliary Health and Lending data
- **Components**: AHLAUX2-4, AHLAUX51-54, AHLEXT, AHLFINSG, AHLMASTER, AHLNAME
- **Processing**: Specialized file watchers and dependency management

## Technical Infrastructure

### Database Schema Architecture

```mermaid
erDiagram
    PDSRCCS {
        string PLAN_BALN_SEGM_MSTR "Financial segment data"
        string CAHD_MST1 "Card master data"
        string CAHD_MSTR_AUX_1 "Auxiliary data 1"
        string CAHD_MSTR_AUX_2 "Auxiliary data 2"
        string CAHD_MSTR_AUX_3 "Auxiliary data 3"
        string CAHD_MST1_ARCH "Archive table"
        string CAHD_MSTR_AUX_1_ARCH "Archive table"
        string CAHD_MSTR_AUX_2_ARCH "Archive table"
    }
    
    PPSRCCS {
        string PROCESSING_TABLES "Data processing tables"
        string TRANSFORMATION_RULES "Business rules"
    }
    
    PVTECH {
        string CALENDAR_DATA "Business calendar"
        string HOLIDAY_TABLES "Holiday definitions"
    }
    
    PUTIL {
        string UTIL_TRSF_EROR_RQM3 "Error tracking"
        string UTIL_PROS_ISAC "Process status control"
        string SYNC_CONTROL "Synchronization"
        string METADATA_TABLES "System metadata"
    }
    
    PDSRCCS ||--o{ PPSRCCS : "processes"
    PDSRCCS ||--o{ PUTIL : "monitors"
    PPSRCCS ||--o{ PVTECH : "references"
```

### File Processing Workflow

```mermaid
stateDiagram-v2
    [*] --> FileArrival: Source file lands
    
    FileArrival --> FileValidation: File watcher detects
    
    FileValidation --> Processing: Valid file
    FileValidation --> Rejected: Invalid file
    
    Processing --> Staging: DataStage job starts
    Staging --> Loading: Transformations complete
    Loading --> Success: Load successful
    Loading --> Failed: Load failed
    
    Success --> Archived: Archive original
    Success --> Notification: Send success email
    
    Failed --> ErrorLog: Log error details
    Failed --> AlertSent: Send failure email
    
    Rejected --> ManualReview: Investigate issue
    ErrorLog --> ManualReview
    
    Archived --> [*]
    Notification --> [*]
    AlertSent --> [*]
    ManualReview --> [*]
```

### Database Connectivity
```
Primary ODS Server: teradata.gdw.cba
- Load Database: PDSRCCS (Primary Data Source Credit Card System)
- Process Database: PPSRCCS (Primary Process Credit Card System)  
- Calendar Database: PVTECH (Technical/Calendar data)
- Utility Database: PUTIL (Synchronization and utilities)
```

### File Processing Architecture
```
Directory Structure:
├── inbound/          # Incoming data files
├── inprocess/        # Files being processed
├── outbound/         # Processed output files
├── archive/          # Archived files (inbound/outbound)
├── staging/          # Temporary staging area
├── reject/           # Rejected/error files
├── log/              # System logs
├── scripts/          # Execution scripts
└── temp/             # Temporary processing files
```

### Performance Configuration
- **Teradata Sessions**: 1-4 concurrent sessions per player
- **Sync Timeout**: 10,800 seconds (3 hours) for long-running operations
- **Buffer Configuration**: 64K buffers enabled for optimal performance
- **Parallel Processing**: 4-node APT configuration

## Operational Processes

### File Monitoring and Processing

```mermaid
graph TD
    A[File Watcher Process] --> B{Check for Files}
    B -->|Files Found| C[Validate File Format]
    B -->|No Files| D[Wait Interval]
    D --> E{Timeout Reached?}
    E -->|No| B
    E -->|Yes| F[Send Alert Email]
    
    C -->|Valid| G[Move to In-Process]
    C -->|Invalid| H[Move to Reject]
    
    G --> I[Trigger DataStage Job]
    I --> J[Process Data]
    J -->|Success| K[Archive File]
    J -->|Failure| L[Log Error]
    
    K --> M[Send Success Email]
    L --> N[Send Failure Email]
    H --> O[Manual Investigation]
    F --> O
    N --> O
```

1. **File Watchers**
   - Configurable wait times and intervals
   - Automatic email notifications for missing files
   - Support for dated file patterns with control files

2. **Stream Processing**
   - Date-driven processing with `STREAM_ETL_DATE`
   - Stream-specific configuration files (`.cnf`)
   - Parameter-driven job execution

3. **Error Handling**
   - Comprehensive logging framework
   - Email notifications to business stakeholders
   - Error tracking in dedicated utility tables

### Data Archival and Retention

- **Archive Retention**: 2-day retention for inbound/outbound archives
- **Log Retention**: 3-day retention for system logs
- **In-Process Retention**: 1-day retention for processing files
- **Historical Data**: Archive tables with configurable intervals

## Notification and Monitoring

### Notification Flow

```mermaid
graph LR
    subgraph "Event Types"
        E1[File Missing]
        E2[Processing Failure]
        E3[Data Quality Issues]
        E4[Load Success]
    end
    
    subgraph "Email System"
        ES[SMTP Server: localhost]
        SF[From: CCODS_PROD@dsengprod.biloads.cba]
    end
    
    subgraph "Recipients"
        BR[Business: Cards_Analytics@cba.com.au]
        OPS[Operations: CCILoads@cba.com.au]
        TECH[Technical: AUSR_SM05900@cba.com.au]
    end
    
    E1 --> ES
    E2 --> ES
    E3 --> ES
    E4 --> ES
    
    ES --> SF
    SF --> BR
    SF --> OPS
    SF --> TECH
```

### Email Configuration
- **SMTP Server**: localhost
- **Sending Address**: CCODS_PROD@dsengprod.biloads.cba
- **Business Recipients**: Cards_Analytics@cba.com.au, CCILoads@cba.com.au
- **Technical Recipients**: AUSR_SM05900@cba.com.au

### Stakeholder Notifications
- File arrival delays
- Processing failures
- Load completion reports
- Data quality issues

## Integration Points

### System Integration Map

```mermaid
graph TB
    subgraph "Upstream Systems"
        US1[Mainframe Card Processing]
        US2[Customer Management Systems]
        US3[Financial Transaction Systems]
        US4[External Data Providers]
    end
    
    subgraph "CCODS Core"
        CC[CCODS ETL Platform]
    end
    
    subgraph "Downstream Systems"
        DS1[Group Data Warehouse - GDW]
        DS2[Analytical Reporting Systems]
        DS3[Risk Management Platforms]
        DS4[Customer Analytics Systems]
        DS5[Regulatory Reporting]
        DS6[Business Intelligence]
    end
    
    US1 -->|Card Data| CC
    US2 -->|Customer Data| CC
    US3 -->|Transaction Data| CC
    US4 -->|Reference Data| CC
    
    CC -->|Processed Data| DS1
    CC -->|Analytics Data| DS2
    CC -->|Risk Data| DS3
    CC -->|Customer Insights| DS4
    CC -->|Compliance Data| DS5
    CC -->|BI Data| DS6
```

## Development and Deployment

### Environment Structure
- **Project**: CCODS_PROD
- **Application Directory**: `/cba_app/CCODS/PROD`
- **DataStage Project**: IBM InfoSphere DataStage environment
- **Configuration Management**: Parameter files with environment-specific settings

### Key Scripts and Utilities
- `initiate_job`: Main job execution framework
- `file_watcher_*`: File monitoring utilities
- `UtilPros_*.sh`: Utility processing management
- `reset_jobs`: Job reset and cleanup utilities
- `ccods_publ_hldy_gen.ksh`: Holiday calendar generation

## Security and Access Control

### Security Architecture

```mermaid
graph TD
    subgraph "Authentication Layer"
        A1[Database Users: PUCCS001-006]
        A2[Technical Service Accounts]
        A3[Encrypted Password Files]
    end
    
    subgraph "Authorization Layer"
        B1[Schema-Level Permissions]
        B2[Table-Level Access Controls]
        B3[Role-Based Processing Rights]
    end
    
    subgraph "Data Protection"
        C1[File System Permissions]
        C2[Network Security]
        C3[Audit Logging]
    end
    
    A1 --> B1
    A2 --> B2
    A3 --> B3
    
    B1 --> C1
    B2 --> C2
    B3 --> C3
```

### Database Security
- Role-based access with specific technical users
- Password management through encrypted parameter files
- Separate users for different processing streams

### File System Security
- Controlled access to processing directories
- Secure transfer mechanisms
- Audit trails for all data movement

## Business Impact

CCODS serves as a critical component in CBA's data infrastructure, enabling:

- **Real-time Credit Card Operations**: Processing card transactions and customer data
- **Risk Management**: Providing timely data for credit risk assessment
- **Customer Analytics**: Supporting customer segmentation and behavior analysis
- **Regulatory Reporting**: Ensuring compliance with banking regulations
- **Business Intelligence**: Feeding analytical systems with clean, processed data

## Migration Considerations

### Current State vs Future State

```mermaid
graph TB
    subgraph "Current State (Legacy)"
        L1[IBM DataStage ETL]
        L2[Teradata ODS]
        L3[Autosys Scheduling]
        L4[On-Premise Infrastructure]
        L5[File-Based Integration]
    end
    
    subgraph "Future State (Cloud-Native)"
        F1[dbt + Snowflake ETL]
        F2[Cloud Data Warehouse]
        F3[Airflow/Cloud Scheduling]
        F4[Cloud Infrastructure]
        F5[API-First Integration]
    end
    
    subgraph "Migration Path"
        M1[Data Mapping & Validation]
        M2[ETL Logic Translation]
        M3[Testing & Validation]
        M4[Parallel Run & Cutover]
    end
    
    L1 --> M1 --> F1
    L2 --> M2 --> F2
    L3 --> M3 --> F3
    L4 --> M4 --> F4
    L5 --> M4 --> F5
```

As part of the GDW1 migration to cloud-native architecture:

1. **ETL Modernization**: Transition from DataStage to modern cloud ETL tools (dbt, Snowflake)
2. **Database Migration**: Move from Teradata to cloud data warehouse
3. **Orchestration**: Replace Autosys with cloud-native scheduling
4. **Monitoring**: Implement modern observability and alerting
5. **Security**: Adopt cloud security best practices and identity management

---

## 🔗 Detailed Job Analysis

For comprehensive analysis of individual DataStage jobs within the BCFINSG stream, see:

### **📋 BCFINSG Job-Specific Documentation**
**[BCFINSG Job Analysis](BCFINSG/job_specific_detailed/)** - Detailed current state analysis for all 12 DataStage jobs including:
- SQ10COMMONPreprocess, RunStreamStart (Initialization)  
- SQ20BCFINSGValidateFiles, ValidateBcFinsg (Validation)
- SQ40BCFINSGXfmPlanBalnSegmMstr, XfmPlanBalnSegmMstrFromBCFINSG (Transformation)
- SQ60BCFINSGLdPlnBalSegMstr, LdBCFINSGPlanBalnSegmMstr (Loading)
- GDWUtilProcessMetaDataFL, SQ70COMMONLdErr, CCODSLdErr (Utilities & Error Handling)
- SQ80COMMONAHLPostprocess (Post-processing)

### **📊 Technical Analysis**
- **[CCODS DataStage Analysis](CCODS_DataStage_Analysis.md)** - Technical ETL pipeline analysis with data flow architecture

### **🎯 Migration Documentation**  
- **Target State Design**: See `../target_state/` folder for modernization approach using dbt + Snowflake + DCF framework

---

*This documentation represents the legacy CCODS system architecture and will be updated as migration to modern cloud-native solutions progresses.*