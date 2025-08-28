/****************************************** DATA FLOW .START ******************************************/
--50 rows
1. select * from NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.CSE_CPL_BUS_APP;
   ↓ (inprocess_source_override reads from this)
 
--50 rows
2. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.SRCPLAPPSEQ__EXTPL_APP
   ↓ (dbt view)
 
--50 rows
3. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.CPYPLAPPSEQ__EXTPL_APP
   ↓ (dbt view)
 
--50 rows
4. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.XFMCHECKPLAPPIDNULLS__EXTPL_APP
   ↓ (dbt view)
 
--50 rows
5. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.JOINSRCSORTREJECT__EXTPL_APP
   ↓ (dbt view)
 
--50 rows
6. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.XFMSEPARATEREJECTWITHOUTSOURCEANDTHEREST__EXTPL_APP
   ↓ (dbt view)
 
--50 rows
7. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.FUNMERGEJOURNAL__EXTPL_APP
   ↓ (dbt view)
 
--50 rows
8. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.TGTPLAPPPREMAPDS__EXTPL_APP
   ↓ (dbt view with post_hook)
 
--50 rows
9. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.CBA_APP__CSEL4__CSEL4DEV__DATASET__CSE_CPL_BUS_APP_PREMAP__DS
   ↓ (dataset table created by post_hook)
 
--50 rows
10. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.SRCPLAPPPREMAPDS__XFMPL_APPFRMEXT
    ↓ (dbt view reading from dataset)
 
--50 rows
11. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.CPYRENAME__XFMPL_APPFRMEXT
    ↓ (dbt view)
 
--50 rows
12. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.LKPREFERENCES__XFMPL_APPFRMEXT
    ↓ (dbt view)
 
--50 rows
13. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.MODNULLHANDLING__XFMPL_APPFRMEXT
    ↓ (dbt view)
 
--50 rows
14. select svLoadApptPdct,* from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.XFMBUSINESSRULES__XFMPL_APPFRMEXT
    ↓ (dbt view - SPLITS here into multiple paths)
 
        --50 rows
    ├─→ 15A. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.TMPAPPTDEPTDS__XFMPL_APPFRMEXT
    │    ↓ (post_hook creates dataset)
    │   --50 rows
    │   15A1. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.CBA_APP__CSEL4__CSEL4DEV__DATASET__TMP_CSE_CPL_BUS_APP_APPT_DEPT__DS
    │
    │   --0 rows
    └─→ 15B. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.TMPAPPTPDCTDS__XFMPL_APPFRMEXT
        ↓ (post_hook creates dataset) ⚠️ **KEY FILTER: WHERE svLoadApptPdct = 'Y'**
       15B1. select * from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.CBA_APP__CSEL4__CSEL4DEV__DATASET__TMP_CSE_CPL_BUS_APP_APPT_PDCT__DS
        ↓ (read by)
 
16. NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.SRCAPPTPDCTDS__LDTMP_APPT_PDCTFRMXFM
    ↓ (dbt view)
 
17. NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.GTAPPTPDCTTERA__LDTMP_APPT_PDCTFRMXFM
    ↓ (dbt view with post_hook MERGE INTO)
   17A. NPD_D12_DMN_GDWMIG_IBRG_V.PDDSTG.TMP_APPT_PDCT (staging table)
 
[...continues through dltappt_pdctfrmtmp_appt_pdct pipeline...]
 
N. NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.TGTAPPTPDCTINSTERA__LDAPPTPDCTINS
   ↓ (post_hook MERGE INTO)
 
N+1. NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT (Final Target)
 
/****************************************** DATA FLOW .END ******************************************/
 
 
 
--step 1 justification - all records from PDDSTG.CSE_CPL_BUS_APP have PL_PACKAGE_CAT_ID as null, leading to no impact to NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT
 
--step 14 issue, PL_PACKAGE_CAT_ID is null for all records
create or replace view NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.xfmbusinessrules__xfmpl_appfrmext
   as (
    with xfmbusinessrules as (
    select
        PL_APP_ID,
        NOMINATED_BRANCH_ID,
        PL_PACKAGE_CAT_ID,
        ORIG_ETL_D,
        PDCT_N,
        CASE
            WHEN (length(trim(coalesce(PL_PACKAGE_CAT_ID, ''))) = 0)
                THEN NULL
            ELSE PDCT_N
        END AS svPdctN,
        CASE
            WHEN (length(trim(coalesce(NOMINATED_BRANCH_ID, ''))) = 0)
                THEN 'N'
            ELSE 'Y'
        END AS svLoadApptDept,
        CASE
            WHEN (length(trim(coalesce(PL_PACKAGE_CAT_ID, ''))) = 0)
                THEN 'N'
            ELSE 'Y'
        END AS svLoadApptPdct,
        CASE
            WHEN PDCT_N = '800999'
                THEN 'RPR2104'
            ELSE ''
        END AS svErrorCode,
        CASE
            WHEN svErrorCode IS NOT NULL
                AND svErrorCode <> ''
                THEN TRUE
            ELSE FALSE
        END AS svrejectflag
    from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.modnullhandling__xfmpl_appfrmext
)
 
select
    NOMINATED_BRANCH_ID,
    PL_PACKAGE_CAT_ID,
    PL_APP_ID,
    ORIG_ETL_D,
    PDCT_N,
    svLoadApptDept,
    svLoadApptPdct,
    svPdctN,
    svErrorCode,
    svrejectflag
from xfmbusinessrules
  );
------------------------------------------------------------------------------------------------
 
--step 15B issue, P_V_OUT_001_STD_0.xfmbusinessrules__xfmpl_appfrmext.svLoadApptPdct = 'Y' does not work
create or replace   view NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.tmpapptpdctds__xfmpl_appfrmext 
as (
with tmpapptpdctds as (
select
  'CSE' || 'PP' || PL_APP_ID as APPT_PDCT_I,
  'PP' as APPT_QLFY_C,
  null as ACQR_TYPE_C,
  null as ACQR_ADHC_X,
  null as ACQR_SRCE_C,
  PDCT_N,
  'CSE' || 'PL' || PL_APP_ID as APPT_I,
  'CSE' as SRCE_SYST_C,
  PL_PACKAGE_CAT_ID as SRCE_SYST_APPT_PDCT_I,
  null as LOAN_FNDD_METH_C,
  'N' as NEW_ACCT_F,
  null as BROK_PATY_I,
  'N' as COPY_FROM_OTHR_APPT_F,
  to_date('20250807', 'YYYYMMDD') as EFFT_D,
  to_date('99991231', 'YYYYMMDD') as EXPY_D,
  0 as PROS_KEY_EFFT_I,
  null as PROS_KEY_EXPY_I,
  null as EROR_SEQN_I,
  'CSE_CPL_BUS_APP' as RUN_STRM,
  null as APPT_PDCT_CATG_C,
  null as ASES_D,
  null as APPT_PDCT_DURT_C
from NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.xfmbusinessrules__xfmpl_appfrmext
where svLoadApptPdct = 'Y'
)
 
select
            APPT_PDCT_I,
            APPT_QLFY_C,
            ACQR_TYPE_C,
            ACQR_ADHC_X,
            ACQR_SRCE_C,
            PDCT_N,
            APPT_I,
            SRCE_SYST_C,
            SRCE_SYST_APPT_PDCT_I,
            LOAN_FNDD_METH_C,
            NEW_ACCT_F,
            BROK_PATY_I,
            COPY_FROM_OTHR_APPT_F,
            EFFT_D,
            EXPY_D,
            PROS_KEY_EFFT_I,
            PROS_KEY_EXPY_I,
            EROR_SEQN_I,
            RUN_STRM,
            APPT_PDCT_CATG_C,
            ASES_D,
            APPT_PDCT_DURT_C
from tmpapptpdctds
  );