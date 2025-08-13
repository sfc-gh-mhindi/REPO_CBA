use database gdw1_ibrg;

create schema if not exists gdw1_ibrg.starcadproddata;

create iceberg table "GDW1_IBRG"."starcadproddata"."util_pros_isac" (
	"pros_key_i" NUMBER(10,0) NOT NULL,
	"conv_m" STRING,
	"conv_type_m" STRING,
	"pros_rqst_s" TIMESTAMP_NTZ(6),
	"pros_last_rqst_s" TIMESTAMP_NTZ(6),
	"pros_rqst_q" NUMBER(38,0),
	"btch_run_d" DATE,
	"btch_key_i" NUMBER(10,0),
	"srce_syst_m" STRING,
	"srce_m" STRING,
	"trgt_m" STRING,
	"succ_f" STRING,
	"comt_f" STRING,
	"comt_s" TIMESTAMP_NTZ(6),
	"mlti_load_efft_d" DATE,
	"syst_s" TIMESTAMP_NTZ(6),
	"mlti_load_comt_s" TIMESTAMP_NTZ(6),
	"syst_et_q" NUMBER(38,0),
	"syst_uv_q" NUMBER(38,0),
	"syst_ins_q" NUMBER(38,0),
	"syst_upd_q" NUMBER(38,0),
	"syst_del_q" NUMBER(38,0),
	"syst_et_tabl_m" STRING,
	"syst_uv_tabl_m" STRING,
	"syst_head_et_tabl_m" STRING,
	"syst_head_uv_tabl_m" STRING,
	"syst_trlr_et_tabl_m" STRING,
	"syst_trlr_uv_tabl_m" STRING,
	"prev_pros_key_i" NUMBER(10,0),
	"head_recd_type_c" STRING,
	"head_file_m" STRING,
	"head_btch_run_d" DATE,
	"head_file_crat_s" TIMESTAMP_NTZ(6),
	"head_genr_prgm_m" STRING,
	"head_btch_key_i" NUMBER(10,0),
	"head_pros_key_i" NUMBER(10,0),
	"head_pros_prev_key_i" NUMBER(10,0),
	"trlr_recd_type_c" STRING,
	"trlr_recd_q" NUMBER(38,0),
	"trlr_hash_totl_a" NUMBER(18,4),
	"trlr_colm_hash_totl_m" STRING,
	"trlr_eror_recd_q" NUMBER(38,0),
	"trlr_file_comt_s" TIMESTAMP_NTZ(6),
	"trlr_recd_isrt_q" NUMBER(38,0),
	"trlr_recd_updt_q" NUMBER(38,0),
	"trlr_recd_delt_q" NUMBER(38,0)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
 
 
create iceberg table "GDW1_IBRG"."starcadproddata"."appt_dept" (
	"appt_i" STRING NOT NULL COMMENT 'Application Identifier',
	"dept_role_c" STRING NOT NULL COMMENT 'Department Role Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"dept_i" STRING NOT NULL COMMENT 'Department Identifier',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	"brch_n" NUMBER(38,0) COMMENT 'Branch Number',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"chng_reas_c" STRING COMMENT 'Change Reason Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
 
 
create iceberg table "GDW1_IBRG"."starcadproddata"."dept_appt" (
	"dept_i" STRING NOT NULL COMMENT 'Department Identifier',
	"appt_i" STRING NOT NULL COMMENT 'Application Identifier',
	"dept_role_c" STRING NOT NULL COMMENT 'Department Role Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	"brch_n" NUMBER(38,0) COMMENT 'Branch Number',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"chng_reas_c" STRING COMMENT 'Change Reason Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
 
create iceberg table "GDW1_IBRG"."starcadproddata"."appt_pdct" (
	"appt_pdct_i" STRING NOT NULL COMMENT 'Application Product Identifier',
	"appt_qlfy_c" STRING COMMENT 'Application Qualifier Code',
	"acqr_type_c" STRING COMMENT 'Acquisition Type Code',
	"acqr_adhc_x" STRING COMMENT 'Acquisition Ad Hoc Comment',
	"acqr_srce_c" STRING COMMENT 'Acquisition Source Code',
	"pdct_n" NUMBER(38,0) NOT NULL COMMENT 'Product Number',
	"appt_i" STRING NOT NULL COMMENT 'Application Identifier',
	"srce_syst_c" STRING NOT NULL COMMENT 'Source System Code',
	"srce_syst_appt_pdct_i" STRING NOT NULL COMMENT 'Source System Application Product Identifier',
	"loan_fndd_meth_c" STRING COMMENT 'Loan Funding Method Code',
	"new_acct_f" STRING COMMENT 'New Account Flag',
	"brok_paty_i" STRING COMMENT 'Broker Party Identifier',
	"copy_from_othr_appt_f" STRING COMMENT 'Copy From Other Application Flag',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	"job_comm_catg_c" STRING COMMENT 'JOB COMMISSION CATEGORY CODE',
	"debt_abn_x" STRING,
	"debt_busn_m" STRING,
	"smpl_appt_f" STRING,
	"appt_pdct_catg_c" STRING,
	"appt_pdct_durt_c" STRING,
	"ases_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;