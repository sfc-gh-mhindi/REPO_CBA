# CSEL & CCODS DBT Projects - ASCII Art Version

## Overview

This repository contains two complementary data pipeline projects implemented using DBT (Data Build Tool) and deployed within Snowflake:

- **CSEL (Commonwealth Bank Service Layer)**: Processes customer service data, appointments, products, and department information through 18 sequential transformations
- **CCODS (Commonwealth Bank Operations Data System)**: Processes BCFINSG data and populates the PLAN_BALN_SEGM_MSTR table through 2 sequential model groups

Both projects share the same Snowflake infrastructure and deployment framework while serving different business domains.

---

## 🏗️ **CSEL Project**

### CSEL Execution Flow (18 Steps)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           CSEL EXECUTION FLOW                                  │
│                        3:00 AM Australia/Sydney                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐            │
│  │    Step 1-5     │───▶│    Step 6-12    │───▶│   Step 13-16    │            │
│  │ Control Setup   │    │ Data Processing │    │Product Processing│            │
│  │                 │    │                 │    │                 │            │
│  │ • Stream Status │    │ • Extract Apps  │    │ • Load Temp     │            │
│  │ • ISAC Check    │    │ • Transform     │    │ • Delta Process │            │
│  │ • Process Keys  │    │ • Load Temp     │    │ • Update Data   │            │
│  │ • Lookups       │    │ • Delta Process │    │ • Insert Data   │            │
│  │ • Finishing Pt  │    │ • Update/Insert │    │                 │            │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘            │
│           │                       │                       │                    │
│           ▼                       ▼                       ▼                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                        Step 17-18                                      │   │
│  │                     Final Loading                                      │   │
│  │                                                                         │   │
│  │              • Update Department Appointment Data                      │   │
│  │              • Insert Department Appointment Data                      │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                    │                                            │
│                                    ▼                                            │
│  ┌─────────────────┐    SUCCESS   ┌─────────────────┐    FAILURE               │
│  │ ✅ CSEL Complete│◄─────────────│  Decision Point │──────────────┐           │
│  │ All 18 Steps    │              │                 │              ▼           │
│  │   Successful    │              └─────────────────┘    ┌─────────────────┐   │
│  └─────────────────┘                                     │ ❌ CSEL Failed  │   │
│                                                           │ Log Error & Stop│   │
│                                                           └─────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🏗️ **CCODS Project**

### CCODS Execution Flow (2 Steps)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          CCODS EXECUTION FLOW                                  │
│                        4:00 AM Australia/Sydney                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────┐                    ┌─────────────────┐                    │
│  │    Step 1       │───────────────────▶│    Step 2       │                    │
│  │Transform BCFINSG│                    │  Load to GDW    │                    │
│  │                 │                    │                 │                    │
│  │ • 40_transform/ │                    │ • 60_load_gdw/  │                    │
│  │ • Data Cleansing│                    │ • Final Loading │                    │
│  │ • Validation    │                    │ • PLAN_BALN_    │                    │
│  │ • Structuring   │                    │   SEGM_MSTR     │                    │
│  └─────────────────┘                    └─────────────────┘                    │
│           │                                       │                            │
│           ▼                                       ▼                            │
│  ┌─────────────────┐    SUCCESS   ┌─────────────────┐    SUCCESS               │
│  │ Transform Check │─────────────▶│  Loading Check  │──────────────┐           │
│  │                 │              │                 │              ▼           │
│  └─────────────────┘              └─────────────────┘    ┌─────────────────┐   │
│           │                                              │✅ CCODS Complete│   │
│           │ FAILURE                                      │PLAN_BALN_SEGM_ │   │
│           ▼                                              │MSTR Populated   │   │
│  ┌─────────────────┐                                     └─────────────────┘   │
│  │❌ CCODS Failed  │                                                           │
│  │Transform Error  │                                                           │
│  │Log & Stop       │                                                           │
│  └─────────────────┘                                                           │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🏛️ **Shared Infrastructure**

### Snowflake Database Structure

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           SNOWFLAKE ENVIRONMENT                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    NPD_D12_DMN_GDWMIG Database                          │   │
│  │                                                                         │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │                        TMP Schema                               │   │   │
│  │  │                                                                 │   │   │
│  │  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │   │   │
│  │  │  │ P_EXECUTE_DBT_  │  │ P_EXECUTE_DBT_  │  │    GDW1_DBT     │ │   │   │
│  │  │  │      CSEL       │  │     CCODS       │  │  Shared Project │ │   │   │
│  │  │  │   🔄 18 Steps   │  │   🔄 2 Steps    │  │   📦 Models     │ │   │   │
│  │  │  └─────────────────┘  └─────────────────┘  └─────────────────┘ │   │   │
│  │  │                                                                 │   │   │
│  │  │  ┌─────────────────┐  ┌─────────────────┐                      │   │   │
│  │  │  │ T_EXECUTE_DBT_  │  │ T_EXECUTE_DBT_  │                      │   │   │
│  │  │  │      CSEL       │  │     CCODS       │                      │   │   │
│  │  │  │  ⏰ 3:00 AM     │  │  ⏰ 4:00 AM     │                      │   │   │
│  │  │  └─────────────────┘  └─────────────────┘                      │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                 NPD_D12_DMN_GDWMIG_IBRG_V Database                      │   │
│  │                                                                         │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │                   P_V_OUT_001_STD_0 Schema                      │   │   │
│  │  │                                                                 │   │   │
│  │  │  ┌─────────────────┐  ┌─────────────────┐                      │   │   │
│  │  │  │ DCF_T_EXEC_LOG  │  │  RUN_STRM_TMPL  │                      │   │   │
│  │  │  │ 📊 Unified Audit│  │ ⚙️ Control Table│                      │   │   │
│  │  │  │     Table       │  │                 │                      │   │   │
│  │  │  └─────────────────┘  └─────────────────┘                      │   │   │
│  │  │                                                                 │   │   │
│  │  │  ┌─────────────────────────────────────────────────────────┐   │   │   │
│  │  │  │                Model Outputs                            │   │   │   │
│  │  │  │                                                         │   │   │   │
│  │  │  │  📋 CSEL Models          📈 CCODS Models                │   │   │   │
│  │  │  │  • Appointments          • BCFINSG Transformations      │   │   │   │
│  │  │  │  • Products              • PLAN_BALN_SEGM_MSTR         │   │   │   │
│  │  │  │  • Departments                                          │   │   │   │
│  │  │  └─────────────────────────────────────────────────────────┘   │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘

Data Flow Timeline:
3:00 AM ──► CSEL (18 Steps) ──► 1 Hour Buffer ──► 4:00 AM ──► CCODS (2 Steps) ──► Complete
```

---

## 📊 **Monitoring and Troubleshooting**

### Monitoring Methods

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            MONITORING OVERVIEW                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐            │
│  │   Execution     │    │   Monitoring    │    │    Outputs      │            │
│  │   Processes     │───▶│    Methods      │───▶│   & Results     │            │
│  │                 │    │                 │    │                 │            │
│  │ • CSEL Process  │    │ • DBT Workspace │    │ • Real-time     │            │
│  │ • CCODS Process │    │ • Query History │    │ • Historical    │            │
│  │                 │    │ • Audit Logs    │    │ • Detailed      │            │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘            │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                        Monitoring Details                               │   │
│  │                                                                         │   │
│  │  1. DBT Workspace Monitoring                                           │   │
│  │     • SHOW TASKS commands                                              │   │
│  │     • Real-time status                                                 │   │
│  │                                                                         │   │
│  │  2. Query History Monitoring                                           │   │
│  │     • TASK_HISTORY() function                                          │   │
│  │     • RESULT_SCAN() function                                           │   │
│  │     • Execution history analysis                                       │   │
│  │                                                                         │   │
│  │  3. Unified Audit Monitoring                                           │   │
│  │     • DCF_T_EXEC_LOG table                                             │   │
│  │     • Multi-process queries                                            │   │
│  │     • Detailed process logs                                            │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔧 **Installation Sequence**

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          INSTALLATION SEQUENCE                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Step 1: CSEL Database Setup                                                   │
│  ┌─────────────────┐                                                           │
│  │ Database Admin  │──────┐                                                    │
│  └─────────────────┘      │                                                    │
│                           ▼                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ ✅ Tables & Views Created                                               │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  Step 2: CSEL Control Data                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ ✅ Reference Data Populated                                             │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  Step 3-4: CSEL Procedures & Tasks                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ ✅ P_EXECUTE_DBT_CSEL Created                                           │   │
│  │ ✅ T_EXECUTE_DBT_CSEL Created                                           │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  Step 5-6: CCODS Procedures & Tasks                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ ✅ P_EXECUTE_DBT_CCODS Created                                          │   │
│  │ ✅ T_EXECUTE_DBT_CCODS Created                                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  Step 7: Shared DBT Project                                                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ ✅ GDW1_DBT Workspace Ready                                             │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  Step 8-9: Task Activation                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ ✅ Both projects now scheduled and operational                          │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

*This README demonstrates ASCII art diagrams with universal compatibility and clear visual structure.* 