-- =====================================================================
-- BACKUP ICEBERG TABLES DATA SCRIPT
-- =====================================================================
-- This script creates standard backup tables with data copied from 
-- iceberg tables using CREATE TABLE AS SELECT statements
-- 
-- Target schema: NPD_D12_DMN_GDWMIG.TMP
-- Table naming convention: {schema_name}_{table_name}
-- Example: "pdcbods"."ods_rule" becomes pdcbods_ods_rule
-- =====================================================================

USE ROLE r_dev_npd_d12_gdwmig;
USE DATABASE NPD_D12_DMN_GDWMIG;
USE WAREHOUSE wh_usr_npd_d12_gdwmig_001;

-- Create TMP schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS TMP;

-- Backup data from "pdcbods"."ods_rule"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdcbods_ods_rule AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdcbods"."ods_rule";

-- Backup data from "pdcms"."map_cms_pdct"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdcms_map_cms_pdct AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdcms"."map_cms_pdct";

-- Backup data from "pddstg"."acct_paty_dedup"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_acct_paty_dedup AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_dedup";

-- Backup data from "pddstg"."acct_paty_rel_tha"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_acct_paty_rel_tha AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_rel_tha";

-- Backup data from "pddstg"."acct_paty_rel_wss"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_acct_paty_rel_wss AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_rel_wss";

-- Backup data from "pddstg"."acct_paty_tha_new_rnge"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_acct_paty_tha_new_rnge AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_tha_new_rnge";

-- Backup data from "pddstg"."acct_rel_wss_ditps"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_acct_rel_wss_ditps AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_rel_wss_ditps";

-- Backup data from "pddstg"."derv_acct_paty_add"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_derv_acct_paty_add AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_add";

-- Backup data from "pddstg"."derv_acct_paty_chg"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_derv_acct_paty_chg AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_chg";

-- Backup data from "pddstg"."derv_acct_paty_curr"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_derv_acct_paty_curr AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_curr";

-- Backup data from "pddstg"."derv_acct_paty_del"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_derv_acct_paty_del AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_del";

-- Backup data from "pddstg"."derv_acct_paty_flag"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_derv_acct_paty_flag AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_flag";

-- Backup data from "pddstg"."derv_acct_paty_non_rm"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_derv_acct_paty_non_rm AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_non_rm";

-- Backup data from "pddstg"."derv_acct_paty_rm"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_derv_acct_paty_rm AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_rm";

-- Backup data from "pddstg"."derv_acct_paty_row_secu_fix"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_derv_acct_paty_row_secu_fix AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_row_secu_fix";

-- Backup data from "pddstg"."derv_prtf_acct_paty_psst"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_derv_prtf_acct_paty_psst AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_acct_paty_psst";

-- Backup data from "pddstg"."derv_prtf_acct_paty_stag"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_derv_prtf_acct_paty_stag AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_acct_paty_stag";

-- Backup data from "pddstg"."derv_prtf_acct_stag"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_derv_prtf_acct_stag AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_acct_stag";

-- Backup data from "pddstg"."derv_prtf_paty_stag"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_derv_prtf_paty_stag AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_paty_stag";

-- Backup data from "pddstg"."grd_gnrc_map_busn_segm_prty"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_grd_gnrc_map_busn_segm_prty AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_busn_segm_prty";

-- Backup data from "pddstg"."grd_gnrc_map_derv_paty_hold"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_grd_gnrc_map_derv_paty_hold AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_derv_paty_hold";

-- Backup data from "pddstg"."grd_gnrc_map_derv_paty_rel"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_grd_gnrc_map_derv_paty_rel AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_derv_paty_rel";

-- Backup data from "pddstg"."grd_gnrc_map_derv_unid_paty"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_grd_gnrc_map_derv_unid_paty AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_derv_unid_paty";

-- Backup data from "pddstg"."grd_gnrc_map_paty_hold_prty"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pddstg_grd_gnrc_map_paty_hold_prty AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_paty_hold_prty";

-- Backup data from "pdgrd"."dept_dimn_node_ancs_curr"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdgrd_dept_dimn_node_ancs_curr AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."dept_dimn_node_ancs_curr";

-- Backup data from "pdgrd"."grd_dept_flat_curr"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdgrd_grd_dept_flat_curr AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."grd_dept_flat_curr";

-- Backup data from "pdgrd"."grd_gnrc_map"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdgrd_grd_gnrc_map AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."grd_gnrc_map";

-- Backup data from "pdgrd"."non_work_day"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdgrd_non_work_day AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."non_work_day";

-- Backup data from "pdpaty"."acct_paty"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdpaty_acct_paty AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdpaty"."acct_paty";

-- Backup data from "pdpaty"."paty_rel"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdpaty_paty_rel AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdpaty"."paty_rel";

-- Backup data from "pdpaty"."util_pros_isac"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdpaty_util_pros_isac AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdpaty"."util_pros_isac";

-- Backup data from "pdsecurity"."row_levl_secu_user_prfl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdsecurity_row_levl_secu_user_prfl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsecurity"."row_levl_secu_user_prfl";

-- Backup data from "pdsrccs"."plan_baln_segm_mstr"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdsrccs_plan_baln_segm_mstr AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."plan_baln_segm_mstr";

-- Backup data from "pdsrccs"."plan_baln_segm_mstr_arch"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdsrccs_plan_baln_segm_mstr_arch AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."plan_baln_segm_mstr_arch";

-- Backup data from "pdsrccs"."util_btch_isac"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdsrccs_util_btch_isac AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_btch_isac";

-- Backup data from "pdsrccs"."util_parm"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdsrccs_util_parm AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_parm";

-- Backup data from "pdsrccs"."util_pros_isac"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdsrccs_util_pros_isac AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_pros_isac";

-- Backup data from "pdsrccs"."util_trsf_eror_rqm3"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdsrccs_util_trsf_eror_rqm3 AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_trsf_eror_rqm3";

-- Backup data from "pdtrpc"."wknd_pblc_hldy"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.pdtrpc_wknd_pblc_hldy AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdtrpc"."wknd_pblc_hldy";

-- Backup data from "putil"."pros_eror_log"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.putil_pros_eror_log AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."putil"."pros_eror_log";

-- Backup data from "putil"."terasync"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.putil_terasync AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."putil"."terasync";

-- Backup data from "starcadproddata"."acct_appt_pdct"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_acct_appt_pdct AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_appt_pdct";

-- Backup data from "starcadproddata"."acct_rel"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_acct_rel AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_rel";

-- Backup data from "starcadproddata"."acct_unid_paty"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_acct_unid_paty AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_unid_paty";

-- Backup data from "starcadproddata"."acct_xref_bps_cbs"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_acct_xref_bps_cbs AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_xref_bps_cbs";

-- Backup data from "starcadproddata"."acct_xref_mas_dar"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_acct_xref_mas_dar AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_xref_mas_dar";

-- Backup data from "starcadproddata"."appt"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt";

-- Backup data from "starcadproddata"."appt_dept"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_dept AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_dept";

-- Backup data from "starcadproddata"."appt_feat"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_feat AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_feat";

-- Backup data from "starcadproddata"."appt_pdct"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_pdct AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct";

-- Backup data from "starcadproddata"."appt_pdct_acct"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_pdct_acct AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_acct";

-- Backup data from "starcadproddata"."appt_pdct_amt"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_pdct_amt AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_amt";

-- Backup data from "starcadproddata"."appt_pdct_feat"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_pdct_feat AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_feat";

-- Backup data from "starcadproddata"."appt_pdct_paty"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_pdct_paty AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_paty";

-- Backup data from "starcadproddata"."appt_pdct_purp"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_pdct_purp AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_purp";

-- Backup data from "starcadproddata"."appt_pdct_rel"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_pdct_rel AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_rel";

-- Backup data from "starcadproddata"."appt_pdct_rpay"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_pdct_rpay AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_rpay";

-- Backup data from "starcadproddata"."appt_pdct_unid_paty"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_pdct_unid_paty AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_unid_paty";

-- Backup data from "starcadproddata"."appt_rel"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_rel AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_rel";

-- Backup data from "starcadproddata"."appt_trnf_detl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_appt_trnf_detl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_trnf_detl";

-- Backup data from "starcadproddata"."busn_evnt"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_busn_evnt AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."busn_evnt";

-- Backup data from "starcadproddata"."cls_fcly"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_cls_fcly AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."cls_fcly";

-- Backup data from "starcadproddata"."cls_unid_paty"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_cls_unid_paty AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."cls_unid_paty";

-- Backup data from "starcadproddata"."dar_acct"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_dar_acct AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."dar_acct";

-- Backup data from "starcadproddata"."dept_appt"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_dept_appt AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."dept_appt";

-- Backup data from "starcadproddata"."derv_acct_paty"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_derv_acct_paty AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_acct_paty";

-- Backup data from "starcadproddata"."derv_prtf_acct_rel"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_derv_prtf_acct_rel AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_prtf_acct_rel";

-- Backup data from "starcadproddata"."derv_prtf_own_rel"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_derv_prtf_own_rel AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_prtf_own_rel";

-- Backup data from "starcadproddata"."derv_prtf_paty_rel"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_derv_prtf_paty_rel AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_prtf_paty_rel";

-- Backup data from "starcadproddata"."evnt"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_evnt AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."evnt";

-- Backup data from "starcadproddata"."evnt_empl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_evnt_empl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."evnt_empl";

-- Backup data from "starcadproddata"."evnt_int_grup"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_evnt_int_grup AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."evnt_int_grup";

-- Backup data from "starcadproddata"."gdw_efft_date"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_gdw_efft_date AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."gdw_efft_date";

-- Backup data from "starcadproddata"."int_grup"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_int_grup AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup";

-- Backup data from "starcadproddata"."int_grup_dept"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_int_grup_dept AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup_dept";

-- Backup data from "starcadproddata"."int_grup_empl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_int_grup_empl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup_empl";

-- Backup data from "starcadproddata"."int_grup_unid_paty"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_int_grup_unid_paty AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup_unid_paty";

-- Backup data from "starcadproddata"."map_cse_adrs_type"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_adrs_type AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_adrs_type";

-- Backup data from "starcadproddata"."map_cse_appt_acqr_srce"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_acqr_srce AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_acqr_srce";

-- Backup data from "starcadproddata"."map_cse_appt_c"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_c AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_c";

-- Backup data from "starcadproddata"."map_cse_appt_cmpe"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_cmpe AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_cmpe";

-- Backup data from "starcadproddata"."map_cse_appt_code_hm"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_code_hm AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_code_hm";

-- Backup data from "starcadproddata"."map_cse_appt_cond"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_cond AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_cond";

-- Backup data from "starcadproddata"."map_cse_appt_docu_dely"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_docu_dely AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_docu_dely";

-- Backup data from "starcadproddata"."map_cse_appt_feat"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_feat AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_feat";

-- Backup data from "starcadproddata"."map_cse_appt_form"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_form AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_form";

-- Backup data from "starcadproddata"."map_cse_appt_orig"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_orig AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_orig";

-- Backup data from "starcadproddata"."map_cse_appt_pdct_feat"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_pdct_feat AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_pdct_feat";

-- Backup data from "starcadproddata"."map_cse_appt_pdct_paty_role"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_pdct_paty_role AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_pdct_paty_role";

-- Backup data from "starcadproddata"."map_cse_appt_purp_cl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_purp_cl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_cl";

-- Backup data from "starcadproddata"."map_cse_appt_purp_clas_cl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_purp_clas_cl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_clas_cl";

-- Backup data from "starcadproddata"."map_cse_appt_purp_hl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_purp_hl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_hl";

-- Backup data from "starcadproddata"."map_cse_appt_purp_pl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_purp_pl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_pl";

-- Backup data from "starcadproddata"."map_cse_appt_qlfy"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_qlfy AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_qlfy";

-- Backup data from "starcadproddata"."map_cse_appt_qstn_hl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_qstn_hl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_qstn_hl";

-- Backup data from "starcadproddata"."map_cse_appt_qstn_resp_hl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_appt_qstn_resp_hl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_qstn_resp_hl";

-- Backup data from "starcadproddata"."map_cse_cmpe_idnn"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_cmpe_idnn AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_cmpe_idnn";

-- Backup data from "starcadproddata"."map_cse_cnty"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_cnty AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_cnty";

-- Backup data from "starcadproddata"."map_cse_cris_pdct"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_cris_pdct AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_cris_pdct";

-- Backup data from "starcadproddata"."map_cse_docu_meth"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_docu_meth AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_docu_meth";

-- Backup data from "starcadproddata"."map_cse_env_chld_paty_rel"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_env_chld_paty_rel AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_chld_paty_rel";

-- Backup data from "starcadproddata"."map_cse_env_evnt_actv_type"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_env_evnt_actv_type AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_evnt_actv_type";

-- Backup data from "starcadproddata"."map_cse_env_paty_rel"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_env_paty_rel AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_paty_rel";

-- Backup data from "starcadproddata"."map_cse_env_paty_type"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_env_paty_type AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_paty_type";

-- Backup data from "starcadproddata"."map_cse_feat_ovrd_reas_hl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_feat_ovrd_reas_hl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_feat_ovrd_reas_hl";

-- Backup data from "starcadproddata"."map_cse_feat_ovrd_reas_hl_d"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_feat_ovrd_reas_hl_d AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_feat_ovrd_reas_hl_d";

-- Backup data from "starcadproddata"."map_cse_feat_ovrd_reas_pl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_feat_ovrd_reas_pl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_feat_ovrd_reas_pl";

-- Backup data from "starcadproddata"."map_cse_fee_capl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_fee_capl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_fee_capl";

-- Backup data from "starcadproddata"."map_cse_fndd_meth"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_fndd_meth AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_fndd_meth";

-- Backup data from "starcadproddata"."map_cse_job_comm_catg"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_job_comm_catg AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_job_comm_catg";

-- Backup data from "starcadproddata"."map_cse_loan_fndd_meth"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_loan_fndd_meth AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_loan_fndd_meth";

-- Backup data from "starcadproddata"."map_cse_loan_term_pl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_loan_term_pl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_loan_term_pl";

-- Backup data from "starcadproddata"."map_cse_lpc_dept_hl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_lpc_dept_hl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_lpc_dept_hl";

-- Backup data from "starcadproddata"."map_cse_orig_appt_srce"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_orig_appt_srce AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_orig_appt_srce";

-- Backup data from "starcadproddata"."map_cse_orig_appt_srce_hm"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_orig_appt_srce_hm AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_orig_appt_srce_hm";

-- Backup data from "starcadproddata"."map_cse_ovrd_fee_frq_cl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_ovrd_fee_frq_cl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_ovrd_fee_frq_cl";

-- Backup data from "starcadproddata"."map_cse_pack_pdct_hl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_pack_pdct_hl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pack_pdct_hl";

-- Backup data from "starcadproddata"."map_cse_pack_pdct_pl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_pack_pdct_pl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pack_pdct_pl";

-- Backup data from "starcadproddata"."map_cse_payt_freq"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_payt_freq AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_payt_freq";

-- Backup data from "starcadproddata"."map_cse_pdct_rel_cl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_pdct_rel_cl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pdct_rel_cl";

-- Backup data from "starcadproddata"."map_cse_pl_acqr_type"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_pl_acqr_type AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pl_acqr_type";

-- Backup data from "starcadproddata"."map_cse_sm_case_stus"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_sm_case_stus AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_sm_case_stus";

-- Backup data from "starcadproddata"."map_cse_sm_case_stus_reas"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_sm_case_stus_reas AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_sm_case_stus_reas";

-- Backup data from "starcadproddata"."map_cse_state"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_state AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_state";

-- Backup data from "starcadproddata"."map_cse_trnf_optn"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_trnf_optn AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_trnf_optn";

-- Backup data from "starcadproddata"."map_cse_tu_appt_c"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_tu_appt_c AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_tu_appt_c";

-- Backup data from "starcadproddata"."map_cse_unid_paty_catg_pl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_map_cse_unid_paty_catg_pl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_unid_paty_catg_pl";

-- Backup data from "starcadproddata"."mos_fcly"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_mos_fcly AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."mos_fcly";

-- Backup data from "starcadproddata"."mos_loan"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_mos_loan AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."mos_loan";

-- Backup data from "starcadproddata"."paty_appt_pdct"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_paty_appt_pdct AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."paty_appt_pdct";

-- Backup data from "starcadproddata"."paty_int_grup"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_paty_int_grup AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."paty_int_grup";

-- Backup data from "starcadproddata"."tha_acct"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_tha_acct AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."tha_acct";

-- Backup data from "starcadproddata"."util_btch_isac"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_util_btch_isac AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_btch_isac";

-- Backup data from "starcadproddata"."util_eti_conv"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_util_eti_conv AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_eti_conv";

-- Backup data from "starcadproddata"."util_parm"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_util_parm AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_parm";

-- Backup data from "starcadproddata"."util_pros_isac"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.starcadproddata_util_pros_isac AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_pros_isac";

-- Backup data from "sysadmin"."tpumpstatustbl"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.sysadmin_tpumpstatustbl AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."sysadmin"."tpumpstatustbl";

-- Backup data from "syscalendar"."caldates"
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG.TMP.syscalendar_caldates AS 
SELECT * FROM "NPD_D12_DMN_GDWMIG_IBRG"."syscalendar"."caldates";
