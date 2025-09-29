-- Validation script for all views defined in ICEBERG Views-MERGED.sql
-- This query checks for the existence of each view in information_schema.views
-- Returns view name and true/false indicating whether the view exists

SELECT 'PVCBODS.ODS_RULE' AS view_name, 
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVCBODS' AND UPPER(table_name) = 'ODS_RULE'

UNION

SELECT 'PVSECURITY.ROW_LEVL_SECU_USER_PRFL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVSECURITY' AND UPPER(table_name) = 'ROW_LEVL_SECU_USER_PRFL'

UNION

SELECT 'PVDATA.GDW_EFFT_DATE' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDATA' AND UPPER(table_name) = 'GDW_EFFT_DATE'

UNION

SELECT 'PVTECH.INT_GRUP_DEPT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'INT_GRUP_DEPT'

UNION

SELECT 'PVDATA.INT_GRUP_DEPT_CURR' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDATA' AND UPPER(table_name) = 'INT_GRUP_DEPT_CURR'

UNION

SELECT 'PVTECH.PATY_INT_GRUP' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'PATY_INT_GRUP'

UNION

SELECT 'PVDATA.PATY_INT_GRUP_CURR' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDATA' AND UPPER(table_name) = 'PATY_INT_GRUP_CURR'

UNION

SELECT 'PVDSTG.ACCT_PATY_DEDUP' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'ACCT_PATY_DEDUP'

UNION

SELECT 'PVDSTG.ACCT_PATY_REL_THA' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'ACCT_PATY_REL_THA'

UNION

SELECT 'PVDSTG.ACCT_PATY_REL_WSS' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'ACCT_PATY_REL_WSS'

UNION

SELECT 'PVDSTG.ACCT_PATY_THA_NEW_RNGE' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'ACCT_PATY_THA_NEW_RNGE'

UNION

SELECT 'PVDSTG.ACCT_REL_WSS_DITPS' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'ACCT_REL_WSS_DITPS'

UNION

SELECT 'PVDSTG.DERV_ACCT_PATY_ADD' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'DERV_ACCT_PATY_ADD'

UNION

SELECT 'PVDSTG.DERV_ACCT_PATY_CHG' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'DERV_ACCT_PATY_CHG'

UNION

SELECT 'PVDSTG.DERV_ACCT_PATY_CURR' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'DERV_ACCT_PATY_CURR'

UNION

SELECT 'PVDSTG.DERV_ACCT_PATY_DEL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'DERV_ACCT_PATY_DEL'

UNION

SELECT 'PVDSTG.DERV_ACCT_PATY_FLAG' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'DERV_ACCT_PATY_FLAG'

UNION

SELECT 'PVDSTG.DERV_ACCT_PATY_NON_RM' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'DERV_ACCT_PATY_NON_RM'

UNION

SELECT 'PVDSTG.DERV_ACCT_PATY_RM' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'DERV_ACCT_PATY_RM'

UNION

SELECT 'PVDSTG.DERV_ACCT_PATY_ROW_SECU_FIX' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'DERV_ACCT_PATY_ROW_SECU_FIX'

UNION

SELECT 'PVDSTG.DERV_PRTF_ACCT_PATY_PSST' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'DERV_PRTF_ACCT_PATY_PSST'

UNION

SELECT 'PVDSTG.DERV_PRTF_ACCT_PATY_STAG' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'DERV_PRTF_ACCT_PATY_STAG'

UNION

SELECT 'PVDSTG.DERV_PRTF_ACCT_STAG' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'DERV_PRTF_ACCT_STAG'

UNION

SELECT 'PVDSTG.DERV_PRTF_PATY_STAG' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'DERV_PRTF_PATY_STAG'

UNION

SELECT 'PVDSTG.GRD_GNRC_MAP_BUSN_SEGM_PRTY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'GRD_GNRC_MAP_BUSN_SEGM_PRTY'

UNION

SELECT 'PVDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'GRD_GNRC_MAP_DERV_PATY_HOLD'

UNION

SELECT 'PVDSTG.GRD_GNRC_MAP_DERV_PATY_REL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'GRD_GNRC_MAP_DERV_PATY_REL'

UNION

SELECT 'PVDSTG.GRD_GNRC_MAP_DERV_UNID_PATY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'GRD_GNRC_MAP_DERV_UNID_PATY'

UNION

SELECT 'PVDSTG.GRD_GNRC_MAP_PATY_HOLD_PRTY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVDSTG' AND UPPER(table_name) = 'GRD_GNRC_MAP_PATY_HOLD_PRTY'

UNION

SELECT 'PVPATY.UTIL_PROS_ISAC' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVPATY' AND UPPER(table_name) = 'UTIL_PROS_ISAC'

UNION

SELECT 'PVTECH.ACCT_APPT_PDCT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'ACCT_APPT_PDCT'

UNION

SELECT 'PVTECH.ACCT_PATY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'ACCT_PATY'

UNION

SELECT 'PVTECH.ACCT_REL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'ACCT_REL'

UNION

SELECT 'PVTECH.ACCT_UNID_PATY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'ACCT_UNID_PATY'

UNION

SELECT 'PVTECH.ACCT_XREF_BPS_CBS' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'ACCT_XREF_BPS_CBS'

UNION

SELECT 'PVTECH.ACCT_XREF_MAS_DAR' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'ACCT_XREF_MAS_DAR'

UNION

SELECT 'PVTECH.APPT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'APPT'

UNION

SELECT 'PVTECH.APPT_DEPT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'APPT_DEPT'

UNION

SELECT 'PVTECH.APPT_PDCT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'APPT_PDCT'

UNION

SELECT 'PVTECH.APPT_PDCT_FEAT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'APPT_PDCT_FEAT'

UNION

SELECT 'PVTECH.APPT_PDCT_REL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'APPT_PDCT_REL'

UNION

SELECT 'PVTECH.APPT_REL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'APPT_REL'

UNION

SELECT 'PVTECH.APPT_TRNF_DETL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'APPT_TRNF_DETL'

UNION

SELECT 'PVTECH.BUSN_EVNT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'BUSN_EVNT'

UNION

SELECT 'CALBASICS.CALBASICS' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'CALBASICS' AND UPPER(table_name) = 'CALBASICS'

UNION

SELECT 'PVTECH.CALENDAR' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'CALENDAR'

UNION

SELECT 'PVTECH.CLS_FCLY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'CLS_FCLY'

UNION

SELECT 'PVTECH.CLS_UNID_PATY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'CLS_UNID_PATY'

UNION

SELECT 'PVTECH.DAR_ACCT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'DAR_ACCT'

UNION

SELECT 'PVTECH.DEPT_DIMN_NODE_ANCS_CURR' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'DEPT_DIMN_NODE_ANCS_CURR'

UNION

SELECT 'PVTECH.DERV_ACCT_PATY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'DERV_ACCT_PATY'

UNION

SELECT 'PVTECH.DERV_PRTF_ACCT_REL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'DERV_PRTF_ACCT_REL'

UNION

SELECT 'PVTECH.DERV_PRTF_ACCT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'DERV_PRTF_ACCT'

UNION

SELECT 'PVTECH.DERV_PRTF_OWN_REL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'DERV_PRTF_OWN_REL'

UNION

SELECT 'PVTECH.DERV_PRTF_PATY_REL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'DERV_PRTF_PATY_REL'

UNION

SELECT 'PVTECH.DERV_PRTF_PATY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'DERV_PRTF_PATY'

UNION

SELECT 'PVTECH.EVNT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'EVNT'

UNION

SELECT 'PVTECH.EVNT_EMPL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'EVNT_EMPL'

UNION

SELECT 'PVTECH.EVNT_INT_GRUP' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'EVNT_INT_GRUP'

UNION

SELECT 'PVTECH.GRD_DEPT_FLAT_CURR' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'GRD_DEPT_FLAT_CURR'

UNION

SELECT 'PVTECH.GRD_GNRC_MAP' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'GRD_GNRC_MAP'

UNION

SELECT 'PVTECH.GRD_GNRC_MAP_CURR' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'GRD_GNRC_MAP_CURR'

UNION

SELECT 'PVTECH.GRD_GNRC_MAP_DERV_PATY_HOLD' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'GRD_GNRC_MAP_DERV_PATY_HOLD'

UNION

SELECT 'PVTECH.GRD_GNRC_MAP_DERV_PATY_REL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'GRD_GNRC_MAP_DERV_PATY_REL'

UNION

SELECT 'PVTECH.GRD_GNRC_MAP_DERV_UNID_PATY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'GRD_GNRC_MAP_DERV_UNID_PATY'

UNION

SELECT 'PVTECH.INT_GRUP' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'INT_GRUP'

UNION

SELECT 'PVTECH.INT_GRUP_EMPL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'INT_GRUP_EMPL'

UNION

SELECT 'PVTECH.INT_GRUP_UNID_PATY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'INT_GRUP_UNID_PATY'

UNION

SELECT 'PVTECH.MAP_CMS_PDCT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CMS_PDCT'

UNION

SELECT 'PVTECH.MAP_CSE_ADRS_TYPE' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_ADRS_TYPE'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_ACQR_SRCE' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_ACQR_SRCE'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_C' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_C'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_CMPE' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_CMPE'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_CODE_HM' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_CODE_HM'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_COND' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_COND'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_DOCU_DELY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_DOCU_DELY'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_FEAT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_FEAT'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_FORM' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_FORM'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_ORIG' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_ORIG'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_PDCT_FEAT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_PDCT_FEAT'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_PDCT_PATY_ROLE' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_PDCT_PATY_ROLE'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_PURP_CL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_PURP_CL'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_PURP_CLAS_CL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_PURP_CLAS_CL'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_PURP_HL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_PURP_HL'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_PURP_PL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_PURP_PL'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_QLFY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_QLFY'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_QSTN_HL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_QSTN_HL'

UNION

SELECT 'PVTECH.MAP_CSE_APPT_QSTN_RESP_HL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_APPT_QSTN_RESP_HL'

UNION

SELECT 'PVTECH.MAP_CSE_CMPE_IDNN' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_CMPE_IDNN'

UNION

SELECT 'PVTECH.MAP_CSE_CNTY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_CNTY'

UNION

SELECT 'PVTECH.MAP_CSE_CRIS_PDCT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_CRIS_PDCT'

UNION

SELECT 'PVTECH.MAP_CSE_DOCU_METH' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_DOCU_METH'

UNION

SELECT 'PVTECH.MAP_CSE_ENV_CHLD_PATY_REL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_ENV_CHLD_PATY_REL'

UNION

SELECT 'PVTECH.MAP_CSE_ENV_EVNT_ACTV_TYPE' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_ENV_EVNT_ACTV_TYPE'

UNION

SELECT 'PVTECH.MAP_CSE_ENV_PATY_REL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_ENV_PATY_REL'

UNION

SELECT 'PVTECH.MAP_CSE_ENV_PATY_TYPE' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_ENV_PATY_TYPE'

UNION

SELECT 'PVTECH.MAP_CSE_FEAT_OVRD_REAS_HL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_FEAT_OVRD_REAS_HL'

UNION

SELECT 'PVTECH.MAP_CSE_FEAT_OVRD_REAS_HL_D' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_FEAT_OVRD_REAS_HL_D'

UNION

SELECT 'PVTECH.MAP_CSE_FEAT_OVRD_REAS_PL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_FEAT_OVRD_REAS_PL'

UNION

SELECT 'PVTECH.MAP_CSE_FEE_CAPL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_FEE_CAPL'

UNION

SELECT 'PVTECH.MAP_CSE_FNDD_METH' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_FNDD_METH'

UNION

SELECT 'PVTECH.MAP_CSE_JOB_COMM_CATG' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_JOB_COMM_CATG'

UNION

SELECT 'PVTECH.MAP_CSE_LOAN_FNDD_METH' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_LOAN_FNDD_METH'

UNION

SELECT 'PVTECH.MAP_CSE_LOAN_TERM_PL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_LOAN_TERM_PL'

UNION

SELECT 'PVTECH.MAP_CSE_LPC_DEPT_HL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_LPC_DEPT_HL'

UNION

SELECT 'PVTECH.MAP_CSE_ORIG_APPT_SRCE' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_ORIG_APPT_SRCE'

UNION

SELECT 'PVTECH.MAP_CSE_ORIG_APPT_SRCE_HM' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_ORIG_APPT_SRCE_HM'

UNION

SELECT 'PVTECH.MAP_CSE_OVRD_FEE_FRQ_CL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_OVRD_FEE_FRQ_CL'

UNION

SELECT 'PVTECH.MAP_CSE_PACK_PDCT_HL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_PACK_PDCT_HL'

UNION

SELECT 'PVTECH.MAP_CSE_PACK_PDCT_PL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_PACK_PDCT_PL'

UNION

SELECT 'PVTECH.MAP_CSE_PAYT_FREQ' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_PAYT_FREQ'

UNION

SELECT 'PVTECH.MAP_CSE_PDCT_REL_CL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_PDCT_REL_CL'

UNION

SELECT 'PVTECH.MAP_CSE_PL_ACQR_TYPE' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_PL_ACQR_TYPE'

UNION

SELECT 'PVTECH.MAP_CSE_SM_CASE_STUS' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_SM_CASE_STUS'

UNION

SELECT 'PVTECH.MAP_CSE_SM_CASE_STUS_REAS' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_SM_CASE_STUS_REAS'

UNION

SELECT 'PVTECH.MAP_CSE_STATE' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_STATE'

UNION

SELECT 'PVTECH.MAP_CSE_TRNF_OPTN' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_TRNF_OPTN'

UNION

SELECT 'PVTECH.MAP_CSE_TU_APPT_C' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_TU_APPT_C'

UNION

SELECT 'PVTECH.MAP_CSE_UNID_PATY_CATG_PL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MAP_CSE_UNID_PATY_CATG_PL'

UNION

SELECT 'PVTECH.MOS_FCLY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MOS_FCLY'

UNION

SELECT 'PVTECH.MOS_LOAN' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'MOS_LOAN'

UNION

SELECT 'PVTECH.NON_WORK_DAY' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'NON_WORK_DAY'

UNION

SELECT 'PVTECH.ODS_RULE' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'ODS_RULE'

UNION

SELECT 'PVTECH.PATY_APPT_PDCT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'PATY_APPT_PDCT'

UNION

SELECT 'PVTECH.PATY_REL' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'PATY_REL'

UNION

SELECT 'PVTECH.THA_ACCT' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'THA_ACCT'

UNION

SELECT 'PVTECH.UTIL_BTCH_ISAC' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'UTIL_BTCH_ISAC'

UNION

SELECT 'PVTECH.UTIL_PARM' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'UTIL_PARM'

UNION

SELECT 'PVTECH.UTIL_PROS_ISAC' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'PVTECH' AND UPPER(table_name) = 'UTIL_PROS_ISAC'

UNION

SELECT 'SYS_CALENDAR.CALENDAR' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_schema) = 'SYS_CALENDAR' AND UPPER(table_name) = 'CALENDAR'

UNION

SELECT 'CALENDARTMP' AS view_name,
       CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS view_exists
FROM information_schema.views 
WHERE UPPER(table_name) = 'CALENDARTMP'

ORDER BY view_name; 