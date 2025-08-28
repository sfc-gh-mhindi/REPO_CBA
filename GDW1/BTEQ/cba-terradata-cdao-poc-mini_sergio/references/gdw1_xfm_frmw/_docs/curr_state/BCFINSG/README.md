# BCFINSG Processing Stream - Current State Documentation

## 📋 Overview

This folder contains current state documentation for the **BCFINSG (Balance Control File - Financial Services Group)** ETL processing stream within Commonwealth Bank's Credit Card Operations Data System (CCODS).

**Key Information:**
- **Business Domain**: Credit Card Operations
- **Source System**: Mainframe EBCDIC files  
- **Target System**: Teradata Data Warehouse
- **Processing Frequency**: Daily
- **Target Table**: `PLAN_BALN_SEGM_MSTR`

---

## 📂 Documentation Structure

### **📋 Job-Specific Detailed Analysis**
Comprehensive current state analysis for each DataStage job:

**📁 [job_specific_detailed/](job_specific_detailed/)**

| **Job Name** | **Type** | **Phase** | **Documentation** | **Criticality** |
|--------------|----------|-----------|-------------------|-----------------|
| **SQ10COMMONPreprocess** | Sequence | 10 | [SQ10COMMONPreprocess.md](job_specific_detailed/SQ10COMMONPreprocess.md) | 🔴 Critical |
| **RunStreamStart** | Server | Support | [RunStreamStart.md](job_specific_detailed/RunStreamStart.md) | 🔴 Critical |  
| **SQ20BCFINSGValidateFiles** | Sequence | 20 | [SQ20BCFINSGValidateFiles.md](job_specific_detailed/SQ20BCFINSGValidateFiles.md) | 🔴 Critical |
| **ValidateBcFinsg** | Parallel | 20 | [ValidateBcFinsg.md](job_specific_detailed/ValidateBcFinsg.md) | 🔴 Critical |
| **SQ40BCFINSGXfmPlanBalnSegmMstr** | Sequence | 40 | [SQ40BCFINSGXfmPlanBalnSegmMstr.md](job_specific_detailed/SQ40BCFINSGXfmPlanBalnSegmMstr.md) | 🔴 Critical |
| **XfmPlanBalnSegmMstrFromBCFINSG** | Parallel | 40 | [XfmPlanBalnSegmMstrFromBCFINSG.md](job_specific_detailed/XfmPlanBalnSegmMstrFromBCFINSG.md) | 🔴 Critical |
| **SQ60BCFINSGLdPlnBalSegMstr** | Sequence | 60 | [SQ60BCFINSGLdPlnBalSegMstr.md](job_specific_detailed/SQ60BCFINSGLdPlnBalSegMstr.md) | 🔴 Critical |
| **LdBCFINSGPlanBalnSegmMstr** | Parallel | 60 | [LdBCFINSGPlanBalnSegmMstr.md](job_specific_detailed/LdBCFINSGPlanBalnSegmMstr.md) | 🔴 Critical |
| **GDWUtilProcessMetaDataFL** | Parallel | Support | [GDWUtilProcessMetaDataFL.md](job_specific_detailed/GDWUtilProcessMetaDataFL.md) | 🟡 Important |
| **SQ70COMMONLdErr** | Sequence | 70 | [SQ70COMMONLdErr.md](job_specific_detailed/SQ70COMMONLdErr.md) | 🟡 Important |
| **CCODSLdErr** | Parallel | 70 | [CCODSLdErr.md](job_specific_detailed/CCODSLdErr.md) | 🟡 Important |
| **SQ80COMMONAHLPostprocess** | Sequence | 80 | [SQ80COMMONAHLPostprocess.md](job_specific_detailed/SQ80COMMONAHLPostprocess.md) | 🟢 Standard |

**Analysis Summary**: 12 DataStage jobs, 51,810+ lines of code, complete documentation coverage

---

## 🔗 Related Documentation

- **System Overview**: See `../CCODS_System_Overview.md` for comprehensive system architecture
- **Technical Analysis**: See `../CCODS_DataStage_Analysis.md` for ETL pipeline analysis  
- **Target State Design**: See `../../target_state/` for modernization approach using dbt + Snowflake + DCF framework

---

**Documentation Status**: ✅ Complete current state analysis  
**Migration Target**: dbt + DCF framework