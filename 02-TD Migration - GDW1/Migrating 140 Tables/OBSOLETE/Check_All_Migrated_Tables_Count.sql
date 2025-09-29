-- Count check for all migrated tables from Generate_All_Migration_Calls.sql
-- This query will show the row count for each successfully migrated table

use role r_dev_npd_d12_gdwmig;
use database npd_d12_dmn_gdwmig;
use schema migration_tracking;
use warehouse wh_usr_npd_d12_gdwmig_001;

-- Count all migrated tables
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdcbods"."ods_rule"' as table_name, COUNT(1) as row_count FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdcbods"."ods_rule"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdcms"."map_cms_pdct"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdcms"."map_cms_pdct"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."dept_dimn_node_ancs_curr"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."dept_dimn_node_ancs_curr"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."grd_dept_flat_curr"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."grd_dept_flat_curr"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."grd_gnrc_map"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."grd_gnrc_map"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."non_work_day"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."non_work_day"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdpaty"."acct_paty"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdpaty"."acct_paty"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdpaty"."paty_rel"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdpaty"."paty_rel"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdpaty"."util_pros_isac"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdpaty"."util_pros_isac"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdsecurity"."row_levl_secu_user_prfl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsecurity"."row_levl_secu_user_prfl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."plan_baln_segm_mstr"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."plan_baln_segm_mstr"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."plan_baln_segm_mstr_arch"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."plan_baln_segm_mstr_arch"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_btch_isac"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_btch_isac"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_parm"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_parm"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_pros_isac"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_pros_isac"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_trsf_eror_rqm3"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_trsf_eror_rqm3"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pdtrpc"."wknd_pblc_hldy"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pdtrpc"."wknd_pblc_hldy"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."putil"."terasync"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."putil"."terasync"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."putil"."pros_eror_log"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."putil"."pros_eror_log"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_appt_pdct"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_appt_pdct"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_rel"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_rel"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_unid_paty"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_unid_paty"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_xref_bps_cbs"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_xref_bps_cbs"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_xref_mas_dar"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_xref_mas_dar"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_dept"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_dept"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_feat"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_feat"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_acct"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_acct"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_amt"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_amt"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_feat"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_feat"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_paty"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_paty"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_purp"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_purp"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_rel"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_rel"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_rpay"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_rpay"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_unid_paty"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_unid_paty"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_rel"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_rel"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_trnf_detl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_trnf_detl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."busn_evnt"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."busn_evnt"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."cls_fcly"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."cls_fcly"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."cls_unid_paty"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."cls_unid_paty"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."dar_acct"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."dar_acct"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."dept_appt"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."dept_appt"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_acct_paty"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_acct_paty"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_prtf_acct_rel"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_prtf_acct_rel"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_prtf_own_rel"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_prtf_own_rel"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_prtf_paty_rel"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_prtf_paty_rel"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."evnt"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."evnt"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."evnt_empl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."evnt_empl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."evnt_int_grup"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."evnt_int_grup"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."gdw_efft_date"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."gdw_efft_date"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup_dept"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup_dept"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup_empl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup_empl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup_unid_paty"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup_unid_paty"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_adrs_type"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_adrs_type"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_acqr_srce"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_acqr_srce"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_c"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_c"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_cmpe"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_cmpe"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_code_hm"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_code_hm"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_cond"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_cond"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_docu_dely"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_docu_dely"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_feat"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_feat"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_form"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_form"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_orig"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_orig"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_pdct_feat"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_pdct_feat"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_pdct_paty_role"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_pdct_paty_role"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_cl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_cl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_clas_cl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_clas_cl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_hl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_hl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_pl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_pl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_qlfy"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_qlfy"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_qstn_hl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_qstn_hl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_qstn_resp_hl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_qstn_resp_hl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_cmpe_idnn"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_cmpe_idnn"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_cnty"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_cnty"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_cris_pdct"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_cris_pdct"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_docu_meth"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_docu_meth"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_chld_paty_rel"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_chld_paty_rel"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_evnt_actv_type"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_evnt_actv_type"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_paty_rel"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_paty_rel"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_paty_type"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_paty_type"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_feat_ovrd_reas_hl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_feat_ovrd_reas_hl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_feat_ovrd_reas_hl_d"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_feat_ovrd_reas_hl_d"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_feat_ovrd_reas_pl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_feat_ovrd_reas_pl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_fee_capl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_fee_capl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_fndd_meth"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_fndd_meth"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_job_comm_catg"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_job_comm_catg"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_loan_fndd_meth"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_loan_fndd_meth"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_loan_term_pl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_loan_term_pl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_lpc_dept_hl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_lpc_dept_hl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_orig_appt_srce"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_orig_appt_srce"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_orig_appt_srce_hm"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_orig_appt_srce_hm"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_ovrd_fee_frq_cl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_ovrd_fee_frq_cl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pack_pdct_hl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pack_pdct_hl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pack_pdct_pl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pack_pdct_pl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_payt_freq"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_payt_freq"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pdct_rel_cl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pdct_rel_cl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pl_acqr_type"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pl_acqr_type"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_sm_case_stus"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_sm_case_stus"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_sm_case_stus_reas"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_sm_case_stus_reas"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_state"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_state"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_trnf_optn"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_trnf_optn"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_tu_appt_c"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_tu_appt_c"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_unid_paty_catg_pl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_unid_paty_catg_pl"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."mos_fcly"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."mos_fcly"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."mos_loan"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."mos_loan"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."paty_appt_pdct"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."paty_appt_pdct"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."paty_int_grup"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."paty_int_grup"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."tha_acct"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."tha_acct"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_btch_isac"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_btch_isac"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_eti_conv"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_eti_conv"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_parm"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_parm"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_pros_isac"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_pros_isac"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_non_rm"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_non_rm"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_tha_new_rnge"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_tha_new_rnge"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_rel_wss"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_rel_wss"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_paty_stag"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_paty_stag"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_derv_paty_rel"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_derv_paty_rel"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_row_secu_fix"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_row_secu_fix"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_flag"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_flag"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_dedup"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_dedup"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_rel_wss_ditps"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_rel_wss_ditps"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_paty_hold_prty"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_paty_hold_prty"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_acct_stag"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_acct_stag"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_rel_tha"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_rel_tha"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_derv_paty_hold"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_derv_paty_hold"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_busn_segm_prty"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_busn_segm_prty"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_acct_paty_psst"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_acct_paty_psst"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_curr"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_curr"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_derv_unid_paty"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_derv_unid_paty"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_del"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_del"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_chg"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_chg"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_acct_paty_stag"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_acct_paty_stag"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_rm"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_rm"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_add"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_add"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."syscalendar"."caldates"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."syscalendar"."caldates"
UNION
SELECT '"NPD_D12_DMN_GDWMIG_IBRG"."sysadmin"."tpumpstatustbl"', COUNT(1) FROM "NPD_D12_DMN_GDWMIG_IBRG"."sysadmin"."tpumpstatustbl"
ORDER BY table_name; 