use role r_dev_npd_d12_gdwmig;
use database npd_d12_dmn_gdwmig;
use schema migration_tracking;
use warehouse wh_usr_npd_d12_gdwmig_001;



CALL P_MIGRATE_TERADATA_TABLE('K_PDCBODS', 'ODS_RULE', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdcbods', 'ods_rule', 'N', '', '', '', 'N');
CALL P_MIGRATE_TERADATA_TABLE('K_PDCMS', 'MAP_CMS_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdcms', 'map_cms_pdct', 'N', '', '', '', 'Y');

CALL P_MIGRATE_TERADATA_TABLE('K_PDGRD', 'DEPT_DIMN_NODE_ANCS_CURR', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdgrd', 'dept_dimn_node_ancs_curr', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDGRD', 'GRD_DEPT_FLAT_CURR', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdgrd', 'grd_dept_flat_curr', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDGRD', 'GRD_GNRC_MAP', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdgrd', 'grd_gnrc_map', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDGRD', 'NON_WORK_DAY', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdgrd', 'non_work_day', 'N', '', '', '', 'Y');

CALL P_MIGRATE_TERADATA_TABLE('K_PDPATY', 'ACCT_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdpaty', 'acct_paty', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDPATY', 'PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdpaty', 'paty_rel', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDPATY', 'UTIL_PROS_ISAC', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdpaty', 'util_pros_isac', 'N', '', '', '', 'Y');

CALL P_MIGRATE_TERADATA_TABLE('K_PDSECURITY', 'ROW_LEVL_SECU_USER_PRFL', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdsecurity', 'row_levl_secu_user_prfl', 'N', '', '', '', 'Y');

CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'PLAN_BALN_SEGM_MSTR', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdsrccs', 'plan_baln_segm_mstr', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'PLAN_BALN_SEGM_MSTR_ARCH', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdsrccs', 'plan_baln_segm_mstr_arch', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'UTIL_BTCH_ISAC', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdsrccs', 'util_btch_isac', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'UTIL_PARM', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdsrccs', 'util_parm', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'UTIL_PROS_ISAC', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdsrccs', 'util_pros_isac', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'UTIL_TRSF_EROR_RQM3', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdsrccs', 'util_trsf_eror_rqm3', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDTRPC', 'WKND_PBLC_HLDY', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdtrpc', 'wknd_pblc_hldy', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PUTIL', 'TERASYNC', 'NPD_D12_DMN_GDWMIG_IBRG', 'putil', 'terasync', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PUTIL', 'PROS_EROR_LOG', 'NPD_D12_DMN_GDWMIG_IBRG', 'putil', 'pros_eror_log', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_APPT_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'acct_appt_pdct', 'N', '', '', '', 'Y');

CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'acct_rel', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'acct_unid_paty', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_XREF_BPS_CBS', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'acct_xref_bps_cbs', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_XREF_MAS_DAR', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'acct_xref_mas_dar', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_DEPT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_dept', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_FEAT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_feat', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_pdct', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_ACCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_pdct_acct', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_AMT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_pdct_amt', 'N', '', '', '', 'Y');

CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_FEAT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_pdct_feat', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_pdct_paty', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_PURP', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_pdct_purp', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_pdct_rel', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_RPAY', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_pdct_rpay', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_pdct_unid_paty', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_rel', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_TRNF_DETL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'appt_trnf_detl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'busn_evnt', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'CLS_FCLY', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'cls_fcly', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'CLS_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'cls_unid_paty', 'N', '', '', '', 'Y');

CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DAR_ACCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'dar_acct', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DEPT_APPT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'dept_appt', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_ACCT_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'derv_acct_paty', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_PRTF_ACCT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'derv_prtf_acct_rel', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_PRTF_OWN_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'derv_prtf_own_rel', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_PRTF_PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'derv_prtf_paty_rel', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'EVNT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'evnt', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'EVNT_EMPL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'evnt_empl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'EVNT_INT_GRUP', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'evnt_int_grup', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'GDW_EFFT_DATE', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'gdw_efft_date', 'N', '', '', '', 'Y');

CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'INT_GRUP', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'int_grup', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'INT_GRUP_DEPT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'int_grup_dept', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'INT_GRUP_EMPL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'int_grup_empl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'INT_GRUP_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'int_grup_unid_paty', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ADRS_TYPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_adrs_type', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_ACQR_SRCE', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_acqr_srce', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_C', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_c', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_CMPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_cmpe', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_CODE_HM', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_code_hm', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_COND', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_cond', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_DOCU_DELY', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_docu_dely', 'N', '', '', '', 'Y');

CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_FEAT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_feat', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_FORM', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_form', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_ORIG', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_orig', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PDCT_FEAT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_pdct_feat', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PDCT_PATY_ROLE', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_pdct_paty_role', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PURP_CL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_purp_cl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PURP_CLAS_CL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_purp_clas_cl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PURP_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_purp_hl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PURP_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_purp_pl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_QLFY', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_qlfy', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_QSTN_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_qstn_hl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_QSTN_RESP_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_appt_qstn_resp_hl', 'N', '', '', '', 'Y');

CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_CMPE_IDNN', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_cmpe_idnn', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_CNTY', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_cnty', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_CRIS_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_cris_pdct', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_DOCU_METH', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_docu_meth', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ENV_CHLD_PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_env_chld_paty_rel', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ENV_EVNT_ACTV_TYPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_env_evnt_actv_type', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ENV_PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_env_paty_rel', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ENV_PATY_TYPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_env_paty_type', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FEAT_OVRD_REAS_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_feat_ovrd_reas_hl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FEAT_OVRD_REAS_HL_D', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_feat_ovrd_reas_hl_d', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FEAT_OVRD_REAS_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_feat_ovrd_reas_pl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FEE_CAPL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_fee_capl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FNDD_METH', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_fndd_meth', 'N', '', '', '', 'Y');


CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_JOB_COMM_CATG', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_job_comm_catg', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_LOAN_FNDD_METH', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_loan_fndd_meth', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_LOAN_TERM_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_loan_term_pl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_LPC_DEPT_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_lpc_dept_hl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ORIG_APPT_SRCE', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_orig_appt_srce', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ORIG_APPT_SRCE_HM', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_orig_appt_srce_hm', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_OVRD_FEE_FRQ_CL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_ovrd_fee_frq_cl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PACK_PDCT_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_pack_pdct_hl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PACK_PDCT_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_pack_pdct_pl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PAYT_FREQ', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_payt_freq', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PDCT_REL_CL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_pdct_rel_cl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PL_ACQR_TYPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_pl_acqr_type', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_SM_CASE_STUS', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_sm_case_stus', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_SM_CASE_STUS_REAS', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_sm_case_stus_reas', 'N', '', '', '', 'Y');


CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_STATE', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_state', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_TRNF_OPTN', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_trnf_optn', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_TU_APPT_C', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_tu_appt_c', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_UNID_PATY_CATG_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'map_cse_unid_paty_catg_pl', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MOS_FCLY', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'mos_fcly', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MOS_LOAN', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'mos_loan', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'PATY_APPT_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'paty_appt_pdct', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'PATY_INT_GRUP', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'paty_int_grup', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'THA_ACCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'tha_acct', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'UTIL_BTCH_ISAC', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'util_btch_isac', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'UTIL_ETI_CONV', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'util_eti_conv', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'UTIL_PARM', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'util_parm', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'UTIL_PROS_ISAC', 'NPD_D12_DMN_GDWMIG_IBRG', 'starcadproddata', 'util_pros_isac', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_NON_RM', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'derv_acct_paty_non_rm', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'ACCT_PATY_THA_NEW_RNGE', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'acct_paty_tha_new_rnge', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'ACCT_PATY_REL_WSS', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'acct_paty_rel_wss', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_PRTF_PATY_STAG', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'derv_prtf_paty_stag', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'GRD_GNRC_MAP_DERV_PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'grd_gnrc_map_derv_paty_rel', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_ROW_SECU_FIX', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'derv_acct_paty_row_secu_fix', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_FLAG', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'derv_acct_paty_flag', 'N', '', '', '', 'Y');

CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'ACCT_PATY_DEDUP', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'acct_paty_dedup', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'ACCT_REL_WSS_DITPS', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'acct_rel_wss_ditps', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'GRD_GNRC_MAP_PATY_HOLD_PRTY', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'grd_gnrc_map_paty_hold_prty', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_PRTF_ACCT_STAG', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'derv_prtf_acct_stag', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'ACCT_PATY_REL_THA', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'acct_paty_rel_tha', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'GRD_GNRC_MAP_DERV_PATY_HOLD', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'grd_gnrc_map_derv_paty_hold', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'GRD_GNRC_MAP_BUSN_SEGM_PRTY', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'grd_gnrc_map_busn_segm_prty', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_PRTF_ACCT_PATY_PSST', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'derv_prtf_acct_paty_psst', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_CURR', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'derv_acct_paty_curr', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'GRD_GNRC_MAP_DERV_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'grd_gnrc_map_derv_unid_paty', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_DEL', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'derv_acct_paty_del', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_CHG', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'derv_acct_paty_chg', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_PRTF_ACCT_PATY_STAG', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'derv_prtf_acct_paty_stag', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_RM', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'derv_acct_paty_rm', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_ADD', 'NPD_D12_DMN_GDWMIG_IBRG', 'pddstg', 'derv_acct_paty_add', 'N', '', '', '', 'Y');

-- Final system tables
CALL P_MIGRATE_TERADATA_TABLE('Sys_Calendar', 'CALDATES', 'NPD_D12_DMN_GDWMIG_IBRG', 'syscalendar', 'caldates', 'N', '', '', '', 'Y');
CALL P_MIGRATE_TERADATA_TABLE('SysAdmin', 'TPUMPSTATUSTBL', 'NPD_D12_DMN_GDWMIG_IBRG', 'sysadmin', 'tpumpstatustbl', 'N', '', '', '', 'Y');
