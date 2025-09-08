# CSEL & CCODS DBT Projects - PlantUML Version

## Overview

This repository contains two complementary data pipeline projects implemented using DBT (Data Build Tool) and deployed within Snowflake:

- **CSEL (Commonwealth Bank Service Layer)**: Processes customer service data, appointments, products, and department information through 18 sequential transformations
- **CCODS (Commonwealth Bank Operations Data System)**: Processes BCFINSG data and populates the PLAN_BALN_SEGM_MSTR table through 2 sequential model groups

Both projects share the same Snowflake infrastructure and deployment framework while serving different business domains.

---

## 🏗️ **CSEL Project**

### CSEL Execution Flow (18 Steps)

```plantuml
@startuml CSEL_Execution_Flow
!define CSEL_COLOR #E1F5FE
!define SUCCESS_COLOR #C8E6C9
!define ERROR_COLOR #FFCDD2

title CSEL Execution Flow (18 Steps)

start

:CSEL Process Start;
note right: 3:00 AM Australia/Sydney

partition "Control Setup" CSEL_COLOR {
  :Step 1: Process Stream Status Check;
  :Step 2: Util Pros ISAC Prev Load Check;
  :Step 3: Load GDW Process Key Sequence;
  :Step 4: Load Mapping Lookups;
  :Step 5: Process Stream Finishing Point;
}

partition "Data Processing" CSEL_COLOR {
  :Step 6: Process Stream Status Check (CSE_CPL_BUS_APP);
  :Step 7: Extract Applications;
  :Step 8: Transform Application Data;
  :Step 9: Load Appointment Department Temp;
  :Step 10: Delta Processing (Appointment Departments);
  :Step 11: Update Appointment Department Data;
  :Step 12: Insert Appointment Department Data;
}

partition "Product Processing" CSEL_COLOR {
  :Step 13: Load Appointment Product Temp;
  :Step 14: Delta Processing (Appointment Products);
  :Step 15: Update Appointment Product Data;
  :Step 16: Insert Appointment Product Data;
}

partition "Final Loading" CSEL_COLOR {
  :Step 17: Update Department Appointment Data;
  :Step 18: Insert Department Appointment Data;
}

if (All Steps Success?) then (yes)
  :CSEL Complete;
  note right SUCCESS_COLOR: All 18 steps completed
else (no)
  :CSEL Failed;
  note right ERROR_COLOR: Log error and stop
  stop
endif

stop

@enduml
```

---

## 🏗️ **CCODS Project**

### CCODS Execution Flow (2 Steps)

```plantuml
@startuml CCODS_Execution_Flow
!define CCODS_COLOR #F3E5F5
!define SUCCESS_COLOR #C8E6C9
!define ERROR_COLOR #FFCDD2

title CCODS Execution Flow (2 Steps)

start

:CCODS Process Start;
note right: 4:00 AM Australia/Sydney

partition "CCODS Processing" CCODS_COLOR {
  :Step 1: Transform BCFINSG Data;
  note right: models/ccods/40_transform/
  
  if (Transform Success?) then (yes)
    :Step 2: Load to GDW;
    note right: models/ccods/60_load_gdw/
  else (no)
    :Transform Failed;
    stop
  endif
}

if (CCODS Success?) then (yes)
  :CCODS Complete;
  note right SUCCESS_COLOR: PLAN_BALN_SEGM_MSTR populated
else (no)
  :CCODS Failed;
  note right ERROR_COLOR: Log error details
  stop
endif

:Both Processes Complete;
stop

@enduml
```

---

## 🏛️ **Shared Infrastructure**

### Snowflake Database Structure

```plantuml
@startuml Infrastructure
!define SNOWFLAKE_COLOR #E3F2FD
!define CSEL_COLOR #E1F5FE
!define CCODS_COLOR #F3E5F5
!define AUDIT_COLOR #C8E6C9

package "Snowflake Environment" SNOWFLAKE_COLOR {
  
  package "NPD_D12_DMN_GDWMIG Database" {
    package "TMP Schema" {
      [P_EXECUTE_DBT_CSEL\n🔄 18 Steps] as CSEL_PROC CSEL_COLOR
      [P_EXECUTE_DBT_CCODS\n🔄 2 Steps] as CCODS_PROC CCODS_COLOR
      [GDW1_DBT\n📦 Shared Project] as DBT
      
      [T_EXECUTE_DBT_CSEL\n⏰ 3:00 AM] as CSEL_TASK CSEL_COLOR
      [T_EXECUTE_DBT_CCODS\n⏰ 4:00 AM] as CCODS_TASK CCODS_COLOR
    }
  }
  
  package "NPD_D12_DMN_GDWMIG_IBRG_V Database" {
    package "P_V_OUT_001_STD_0 Schema" {
      [DCF_T_EXEC_LOG\n📊 Unified Audit] as AUDIT AUDIT_COLOR
      [RUN_STRM_TMPL\n⚙️ Control Table] as CONTROL
      
      package "Model Outputs" {
        [📋 CSEL Models\nAppointments, Products, Departments] as CSEL_MODELS CSEL_COLOR
        [📈 CCODS Models\nBCFINSG, PLAN_BALN_SEGM_MSTR] as CCODS_MODELS CCODS_COLOR
      }
    }
  }
  
  package "External Systems" {
    [Snowflake\nDBT Workspace] as WORKSPACE
    [wh_usr_npd_d12_gdwmig_001\nCompute Warehouse] as WAREHOUSE
  }
}

' Connections
CSEL_TASK --> CSEL_PROC
CCODS_TASK --> CCODS_PROC
CSEL_PROC --> DBT
CCODS_PROC --> DBT
DBT --> WORKSPACE
DBT --> WAREHOUSE
CSEL_PROC --> AUDIT
CCODS_PROC --> AUDIT
DBT --> CSEL_MODELS
DBT --> CCODS_MODELS

@enduml
```

---

## 📊 **Monitoring and Troubleshooting**

### Monitoring Methods

```plantuml
@startuml Monitoring
!define EXEC_COLOR #E1F5FE
!define MONITOR_COLOR #FFF3E0
!define OUTPUT_COLOR #C8E6C9

package "Execution Monitoring" EXEC_COLOR {
  [CSEL Execution\nP_EXECUTE_DBT_CSEL] as CSEL_EXEC
  [CCODS Execution\nP_EXECUTE_DBT_CCODS] as CCODS_EXEC
}

package "Monitoring Methods" MONITOR_COLOR {
  package "1. DBT Workspace Monitoring" {
    [Snowflake DBT\nWorkspace] as WS
    [SHOW TASKS\nCommands] as WS_CMD
  }
  
  package "2. Query History Monitoring" {
    [Query History\nAnalysis] as QH
    [TASK_HISTORY\nFunction] as TASK_HIST
    [RESULT_SCAN\nFunction] as RESULT_SCAN
  }
  
  package "3. Unified Audit Monitoring" {
    [DCF_T_EXEC_LOG\nShared Audit Table] as AUDIT
    [Multi-Process Queries\nCSEL + CCODS Status] as LOG_QUERY
  }
}

package "Monitoring Outputs" OUTPUT_COLOR {
  [Real-time Status\nBoth Projects] as REAL_TIME
  [Execution History\nComparative Analysis] as HISTORY
  [Detailed Logs\nProcess-Specific Status] as DETAILED
}

' Connections
CSEL_EXEC --> WS
CCODS_EXEC --> WS
CSEL_EXEC --> QH
CCODS_EXEC --> QH
CSEL_EXEC --> AUDIT
CCODS_EXEC --> AUDIT

WS --> WS_CMD
WS_CMD --> REAL_TIME

QH --> TASK_HIST
QH --> RESULT_SCAN
TASK_HIST --> HISTORY
RESULT_SCAN --> HISTORY

AUDIT --> LOG_QUERY
LOG_QUERY --> DETAILED

@enduml
```

---

## 🔧 **Installation Sequence**

```plantuml
@startuml Installation
participant Admin as "Database Admin"
participant CSEL as "CSEL Installation"
participant CCODS as "CCODS Installation"
participant DBT as "DBT Workspace"

Admin->>CSEL: 1. Deploy CSEL Database Setup
CSEL->>Admin: ✅ Tables & Views Created

Admin->>CSEL: 2. Deploy CSEL Control Data
CSEL->>Admin: ✅ Reference Data Populated

Admin->>CSEL: 3. Deploy CSEL Procedure
CSEL->>Admin: ✅ P_EXECUTE_DBT_CSEL Created

Admin->>CSEL: 4. Deploy CSEL Task
CSEL->>Admin: ✅ T_EXECUTE_DBT_CSEL Created

Admin->>CCODS: 5. Deploy CCODS Procedure
CCODS->>Admin: ✅ P_EXECUTE_DBT_CCODS Created

Admin->>CCODS: 6. Deploy CCODS Task
CCODS->>Admin: ✅ T_EXECUTE_DBT_CCODS Created

Admin->>DBT: 7. Upload Shared DBT Project
DBT->>Admin: ✅ GDW1_DBT Workspace Ready

Admin->>CSEL: 8. Resume CSEL Task
Admin->>CCODS: 9. Resume CCODS Task

Note over Admin, DBT: Both projects now scheduled and operational

@enduml
```

---

*This README demonstrates PlantUML diagrams with professional styling and comprehensive coverage.* 