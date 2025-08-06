use role r_dev_npd_d12_gdwmig;
use database npd_d12_dmn_gdwmig;
use schema migration_tracking;
use warehouse wh_usr_npd_d12_gdwmig_001;

USE DATABASE NPD_D12_DMN_GDWMIG_IBRG;

CREATE SCHEMA IF NOT EXISTS "pdcbods";
CREATE SCHEMA IF NOT EXISTS "pdcms";
CREATE SCHEMA IF NOT EXISTS "pddstg";
CREATE SCHEMA IF NOT EXISTS "pdgrd";
CREATE SCHEMA IF NOT EXISTS "pdpaty";
CREATE SCHEMA IF NOT EXISTS "pdsecurity";
CREATE SCHEMA IF NOT EXISTS "pdsrccs";
CREATE SCHEMA IF NOT EXISTS "pdtrpc";
CREATE SCHEMA IF NOT EXISTS "putil";
CREATE SCHEMA IF NOT EXISTS "starcadproddata";
CREATE SCHEMA IF NOT EXISTS "syscalendar";
CREATE SCHEMA IF NOT EXISTS "sysadmin";



use role r_dev_npd_d12_gdwmig;
use database npd_d12_dmn_gdwmig;
use schema migration_tracking;
use warehouse wh_usr_npd_d12_gdwmig_001;

drop iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdcbods"."ods_rule";

create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdcbods"."ods_rule" (
	"rule_code" STRING NOT NULL COMMENT 'Rule Code',
	"rule_step_seqn" NUMBER(38,0) NOT NULL COMMENT 'Rule Step Sequence',
	"prty" NUMBER(38,0) NOT NULL COMMENT 'Priority',
	"vald_from" DATE NOT NULL COMMENT 'Valid From Date',
	"vald_to" DATE NOT NULL COMMENT 'Valid To Date',
	"rule_desn" STRING COMMENT 'Rule Description',
	"rule_step_desn" STRING COMMENT 'Rule Step Description',
	"lkup1_text" STRING COMMENT 'Lookup1 Text',
	"lkup1_numb" NUMBER(38,0) COMMENT 'Lookup1 Number',
	"lkup1_date" DATE COMMENT 'Lookup1 Date',
	"lkup1_add_attr" STRING COMMENT 'Lookup1 Additional Attribute',
	"lkup2_text" STRING COMMENT 'Lookup2 Text',
	"lkup2_numb" NUMBER(38,0) COMMENT 'Lookup2 Number',
	"lkup2_date" DATE COMMENT 'Lookup2 Date',
	"lkup2_add_attr" STRING COMMENT 'Lookup2 Additional Attribute',
	"rule_cmmt" STRING COMMENT 'Rule Comment',
	"updt_dtts" TIMESTAMP_NTZ(6) COMMENT 'Update Timestamp',
	"crat_dtts" TIMESTAMP_NTZ(6) COMMENT 'Create Timestamp'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;



CALL P_MIGRATE_TERADATA_TABLE('K_PDCBODS', 'ODS_RULE', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdcbods', 'ods_rule', 'N', '', '', '', 'N');
CALL P_MIGRATE_TERADATA_TABLE('K_PDCBODS', 'ODS_RULE', 'NPD_D12_DMN_GDWMIG_IBRG', 'pdcbods', 'ods_rule', 'N', '', '', '', 'Y');

select * from "NPD_D12_DMN_GDWMIG_IBRG"."pdcbods"."ods_rule";

select * from dmva_object_info order by created_ts desc;
select * from dmva_checksum_tasks_to_insert;






create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdcbods"."ods_rule" (
	"rule_code" STRING NOT NULL COMMENT 'Rule Code',
	"rule_step_seqn" NUMBER(38,0) NOT NULL COMMENT 'Rule Step Sequence',
	"prty" NUMBER(38,0) NOT NULL COMMENT 'Priority',
	"vald_from" DATE NOT NULL COMMENT 'Valid From Date',
	"vald_to" DATE NOT NULL COMMENT 'Valid To Date',
	"rule_desn" STRING COMMENT 'Rule Description',
	"rule_step_desn" STRING COMMENT 'Rule Step Description',
	"lkup1_text" STRING COMMENT 'Lookup1 Text',
	"lkup1_numb" NUMBER(38,0) COMMENT 'Lookup1 Number',
	"lkup1_date" DATE COMMENT 'Lookup1 Date',
	"lkup1_add_attr" STRING COMMENT 'Lookup1 Additional Attribute',
	"lkup2_text" STRING COMMENT 'Lookup2 Text',
	"lkup2_numb" NUMBER(38,0) COMMENT 'Lookup2 Number',
	"lkup2_date" DATE COMMENT 'Lookup2 Date',
	"lkup2_add_attr" STRING COMMENT 'Lookup2 Additional Attribute',
	"rule_cmmt" STRING COMMENT 'Rule Comment',
	"updt_dtts" TIMESTAMP_NTZ(6) COMMENT 'Update Timestamp',
	"crat_dtts" TIMESTAMP_NTZ(6) COMMENT 'Create Timestamp'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;

create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdcms"."map_cms_pdct" (
	"cris_pdct_cat_id" STRING,
	"cris_pdct_c" STRING,
	"cris_desc" STRING,
	"acct_i_prfx" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_dedup" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"assc_acct_i" STRING NOT NULL COMMENT 'Associated Account Identifier',
	"paty_acct_rel_c" STRING NOT NULL COMMENT 'Party Account Relationship Code',
	"prfr_paty_f" STRING COMMENT 'Preferred Party Flag',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_rel_tha" (
	"tha_acct_i" STRING NOT NULL COMMENT 'Thaler Account Identifier',
	"trad_acct_i" STRING NOT NULL COMMENT 'Trading Account Identifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_rel_wss" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"assc_acct_i" STRING NOT NULL COMMENT 'Associated Account Identifier',
	"paty_acct_rel_c" STRING NOT NULL COMMENT 'Party Account Relationship Code',
	"prfr_paty_f" STRING COMMENT 'Preferred Party Flag',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_paty_tha_new_rnge" (
	"tha_acct_i" STRING NOT NULL COMMENT 'Thaler Account Identifier',
	"trad_acct_i" STRING NOT NULL COMMENT 'Trading Account Identifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"new_expy_d" DATE COMMENT 'New Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."acct_rel_wss_ditps" (
	"acct_i" STRING NOT NULL COMMENT 'ACCOUNT IDENTIFIER'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_add" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"assc_acct_i" STRING NOT NULL COMMENT 'Associated Account Identifier',
	"paty_acct_rel_c" STRING NOT NULL COMMENT 'Party Account Relationship Code',
	"prfr_paty_f" STRING COMMENT 'Preferred Party Flag',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_chg" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"assc_acct_i" STRING NOT NULL COMMENT 'Associated Account Identifier',
	"paty_acct_rel_c" STRING NOT NULL COMMENT 'Party Account Relationship Code',
	"prfr_paty_f" STRING COMMENT 'Preferred Party Flag',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_curr" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"assc_acct_i" STRING NOT NULL COMMENT 'Associated Account Identifier',
	"paty_acct_rel_c" STRING NOT NULL COMMENT 'Party Account Relationship Code',
	"prfr_paty_f" STRING COMMENT 'Preferred Party Flag',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_del" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"assc_acct_i" STRING NOT NULL COMMENT 'Associated Account Identifier',
	"paty_acct_rel_c" STRING NOT NULL COMMENT 'Party Account Relationship Code',
	"prfr_paty_f" STRING COMMENT 'Preferred Party Flag',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_flag" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"assc_acct_i" STRING NOT NULL COMMENT 'Associated Account Identifier',
	"paty_acct_rel_c" STRING NOT NULL COMMENT 'Party Account Relationship Code',
	"prfr_paty_f" STRING COMMENT 'Preferred Party Flag',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_non_rm" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"assc_acct_i" STRING NOT NULL COMMENT 'Associated Account Identifier',
	"paty_acct_rel_c" STRING NOT NULL COMMENT 'Party Account Relationship Code',
	"prfr_paty_f" STRING COMMENT 'Preferred Party Flag',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE',
	"rank_i" NUMBER(38,0)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_rm" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"assc_acct_i" STRING NOT NULL COMMENT 'Associated Account Identifier',
	"paty_acct_rel_c" STRING NOT NULL COMMENT 'Party Account Relationship Code',
	"prfr_paty_f" STRING COMMENT 'Preferred Party Flag',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_acct_paty_row_secu_fix" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"derv_acct_paty_row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'DERV ACCT_PATY ROW SECURITY ACCESS CODE',
	"acct_paty_row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ACCT_PATY ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_acct_paty_psst" (
	"acct_i" STRING COMMENT 'Account Identifier',
	"paty_i" STRING COMMENT 'Party Identifier',
	"acct_prtf_c" STRING COMMENT 'Account Portfolio Code',
	"rank_i" NUMBER(38,0),
	"paty_acct_rel_c" STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_acct_paty_stag" (
	"acct_i" STRING COMMENT 'Account Identifier',
	"paty_i" STRING COMMENT 'Party Identifier',
	"acct_prtf_c" STRING COMMENT 'Account Portfolio Code',
	"paty_prtf_c" STRING COMMENT 'Party Portfolio Code',
	"paty_acct_rel_c" STRING COMMENT 'Paty Account Relationship',
	"rank_i" NUMBER(38,0) COMMENT 'RANK'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_acct_stag" (
	"acct_i" STRING COMMENT 'Account Identifier',
	"prtf_code_x" STRING COMMENT 'Derived Portfolio Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."derv_prtf_paty_stag" (
	"paty_i" STRING COMMENT 'Party Identifier',
	"prtf_code_x" STRING COMMENT 'Derived Portfolio Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_busn_segm_prty" (
	"map_type_c" STRING NOT NULL COMMENT 'MAPPING TYPE CODE',
	"busn_segm_c" STRING COMMENT 'Business Segment Code',
	"busn_segm_x" STRING COMMENT 'Business Segment Description',
	"busn_segm_prty" NUMBER(38,0) COMMENT 'Business Segment Priority',
	"efft_d" DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	"expy_d" DATE NOT NULL COMMENT 'EXPIRY DATE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_derv_paty_hold" (
	"map_type_c" STRING NOT NULL COMMENT 'MAPPING TYPE CODE',
	"paty_acct_rel_c" STRING COMMENT 'Account Party Relationship Code',
	"paty_acct_rel_x" STRING COMMENT 'Account Party Relationship Code Description',
	"efft_d" DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	"expy_d" DATE NOT NULL COMMENT 'EXPIRY DATE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_derv_paty_rel" (
	"map_type_c" STRING NOT NULL COMMENT 'MAPPING TYPE CODE',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"rel_c" STRING COMMENT 'Relationship Code',
	"acct_i_c" STRING COMMENT 'Account Code',
	"efft_d" DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	"expy_d" DATE NOT NULL COMMENT 'EXPIRY DATE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_derv_unid_paty" (
	"map_type_c" STRING NOT NULL COMMENT 'MAPPING TYPE CODE',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"unid_paty_srce_syst_c" STRING COMMENT 'Unidentified Party Source System Code',
	"unid_paty_acct_rel_c" STRING COMMENT 'Unidentified Party Account Relationship Code',
	"efft_d" DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	"expy_d" DATE NOT NULL COMMENT 'EXPIRY DATE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pddstg"."grd_gnrc_map_paty_hold_prty" (
	"map_type_c" STRING NOT NULL COMMENT 'MAPPING TYPE CODE',
	"paty_acct_rel_c" STRING COMMENT 'Account Party Relationship Code',
	"paty_acct_rel_x" STRING COMMENT 'Account Party Relationship Code Description',
	"paty_acct_rel_prty" NUMBER(38,0) COMMENT 'Account Party Relationship Code Priority',
	"efft_d" DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	"expy_d" DATE NOT NULL COMMENT 'EXPIRY DATE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."dept_dimn_node_ancs_curr" (
	"dept_i" STRING NOT NULL,
	"ancs_dept_i" STRING,
	"ancs_levl_n" NUMBER(38,0),
	"as_at_d" DATE NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."grd_dept_flat_curr" (
	"dept_l1_node_c" STRING,
	"dept_l1_node_m" STRING,
	"dept_l1_levl_n" NUMBER(38,0),
	"dept_l1_disp_seqn_n" NUMBER(38,0),
	"dept_l1_rept_f" STRING,
	"dept_l1_rprt_type_c" STRING,
	"dept_l2_node_c" STRING,
	"dept_l2_node_m" STRING,
	"dept_l2_levl_n" NUMBER(38,0),
	"dept_l2_disp_seqn_n" NUMBER(38,0),
	"dept_l2_rept_f" STRING,
	"dept_l2_rprt_type_c" STRING,
	"dept_l3_node_c" STRING,
	"dept_l3_node_m" STRING,
	"dept_l3_levl_n" NUMBER(38,0),
	"dept_l3_disp_seqn_n" NUMBER(38,0),
	"dept_l3_rept_f" STRING,
	"dept_l3_rprt_type_c" STRING,
	"dept_l4_node_c" STRING,
	"dept_l4_node_m" STRING,
	"dept_l4_levl_n" NUMBER(38,0),
	"dept_l4_disp_seqn_n" NUMBER(38,0),
	"dept_l4_rept_f" STRING,
	"dept_l4_rprt_type_c" STRING,
	"dept_l5_node_c" STRING,
	"dept_l5_node_m" STRING,
	"dept_l5_levl_n" NUMBER(38,0),
	"dept_l5_disp_seqn_n" NUMBER(38,0),
	"dept_l5_rept_f" STRING,
	"dept_l5_rprt_type_c" STRING,
	"dept_l6_node_c" STRING,
	"dept_l6_node_m" STRING,
	"dept_l6_levl_n" NUMBER(38,0),
	"dept_l6_disp_seqn_n" NUMBER(38,0),
	"dept_l6_rept_f" STRING,
	"dept_l6_rprt_type_c" STRING,
	"dept_l7_node_c" STRING,
	"dept_l7_node_m" STRING,
	"dept_l7_levl_n" NUMBER(38,0),
	"dept_l7_disp_seqn_n" NUMBER(38,0),
	"dept_l7_rept_f" STRING,
	"dept_l7_rprt_type_c" STRING,
	"dept_l8_node_c" STRING,
	"dept_l8_node_m" STRING,
	"dept_l8_levl_n" NUMBER(38,0),
	"dept_l8_disp_seqn_n" NUMBER(38,0),
	"dept_l8_rept_f" STRING,
	"dept_l8_rprt_type_c" STRING,
	"dept_l9_node_c" STRING,
	"dept_l9_node_m" STRING,
	"dept_l9_levl_n" NUMBER(38,0),
	"dept_l9_disp_seqn_n" NUMBER(38,0),
	"dept_l9_rept_f" STRING,
	"dept_l9_rprt_type_c" STRING,
	"dept_l10_node_c" STRING,
	"dept_l10_node_m" STRING,
	"dept_l10_levl_n" NUMBER(38,0),
	"dept_l10_disp_seqn_n" NUMBER(38,0),
	"dept_l10_rept_f" STRING,
	"dept_l10_rprt_type_c" STRING,
	"dept_leaf_node_c" STRING,
	"dept_leaf_node_m" STRING,
	"dept_leaf_levl_n" NUMBER(38,0),
	"dept_leaf_disp_seqn_n" NUMBER(38,0),
	"dept_leaf_rprt_type_c" STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."grd_gnrc_map" (
	"gnrc_map_genr_i" NUMBER(38,0),
	"map_type_c" STRING NOT NULL COMMENT 'MAPPING TYPE CODE',
	"efft_d" DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	"targ_numc_c" NUMBER(38,0) COMMENT 'TARGET NUMERIC CODE',
	"targ_char_c" STRING COMMENT 'TARGET CHARACTER CODE',
	"srce_numc_1_c" NUMBER(38,0) COMMENT 'SOURCE NUMERIC 1 CODE',
	"srce_char_1_c" STRING COMMENT 'SOURCE CHARACTER 1 CODE',
	"srce_numc_2_c" NUMBER(38,0) COMMENT 'SOURCE NUMERIC 2 CODE',
	"srce_char_2_c" STRING COMMENT 'SOURCE CHARACTER 2 CODE',
	"srce_numc_3_c" NUMBER(38,0) COMMENT 'SOURCE NUMERIC 3 CODE',
	"srce_char_3_c" STRING COMMENT 'SOURCE CHARACTER 3 CODE',
	"srce_numc_4_c" NUMBER(38,0) COMMENT 'SOURCE NUMERIC 4 CODE',
	"srce_char_4_c" STRING COMMENT 'SOURCE CHARACTER 4 CODE',
	"srce_numc_5_c" NUMBER(38,0) COMMENT 'SOURCE NUMERIC 5 CODE',
	"srce_char_5_c" STRING COMMENT 'SOURCE CHARACTER 5 CODE',
	"srce_numc_6_c" NUMBER(38,0) COMMENT 'SOURCE NUMERIC 6 CODE',
	"srce_char_6_c" STRING COMMENT 'SOURCE CHARACTER 6 CODE',
	"srce_numc_7_c" NUMBER(38,0) COMMENT 'SOURCE NUMERIC 7 CODE',
	"srce_char_7_c" STRING COMMENT 'SOURCE CHARACTER 7 CODE',
    "expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdgrd"."non_work_day" (
	"geoa_type_c" STRING NOT NULL COMMENT 'GEOGRAPHICAL AREA TYPE CODE',
	"geoa_c" STRING NOT NULL COMMENT 'GEOGRAPHICAL AREA CODE',
	"non_work_d" DATE NOT NULL COMMENT 'NON-WORKING DATE',
	"non_work_day_type_c" STRING NOT NULL COMMENT 'NON-WORKING DAY TYPE CODE',
	"non_work_day_m" STRING NOT NULL COMMENT 'NON-WORKING DAY NAME',
	"hldy_stus_x" STRING COMMENT 'Holiday Status Description'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdpaty"."acct_paty" (
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"paty_acct_rel_c" STRING NOT NULL COMMENT 'Party Account Relationship Code',
	"rel_levl_c" STRING COMMENT 'Relation Level Code',
	"rel_reas_c" STRING COMMENT 'Relation Reason Code',
	"rel_stus_c" STRING COMMENT 'Relation Status Code',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(38,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(38,0) COMMENT 'Process Key Expiry Identifier',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdpaty"."paty_rel" (
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"reld_paty_i" STRING NOT NULL COMMENT 'Related Party Identifier',
	"rel_i" STRING NOT NULL COMMENT 'Relationship Identifier',
	"rel_reas_c" STRING NOT NULL COMMENT 'Relation Reason Code',
	"rel_type_c" STRING NOT NULL COMMENT 'Relation TYPE Code',
	"srce_syst_c" STRING NOT NULL COMMENT 'Source System Code',
	"paty_role_c" STRING NOT NULL COMMENT 'Party Role Code',
	"rel_stus_c" STRING NOT NULL COMMENT 'Relation Status Code',
	"rel_levl_c" STRING NOT NULL COMMENT 'Relation level Code',
	"rel_efft_d" DATE COMMENT 'Relationship Effective Date',
	"rel_expy_d" DATE COMMENT 'Relationship Expiry Date',
	"srce_syst_rel_i" STRING COMMENT 'Source System Relationship Identifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(38,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(38,0) COMMENT 'Process Key Expiry Identifier',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code',
	"risk_aggr_f" STRING COMMENT 'Risk Aggregation Flag'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdpaty"."util_pros_isac" (
	"pros_key_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Identifier',
	"conv_m" STRING COMMENT 'Convert Name',
	"conv_type_m" STRING COMMENT 'Convert Type Name',
	"pros_rqst_s" TIMESTAMP_NTZ(6) COMMENT 'Process Request Timestamp',
	"pros_last_rqst_s" TIMESTAMP_NTZ(6) COMMENT 'Process Last Request Timestamp',
	"pros_rqst_q" NUMBER(38,0) COMMENT 'Process Request Count',
	"btch_run_d" DATE COMMENT 'Batch Run Date',
	"btch_key_i" NUMBER(10,0) COMMENT 'Batch Key Identifier',
	"srce_syst_m" STRING COMMENT 'Source System Name',
	"srce_m" STRING COMMENT 'Source Name',
	"trgt_m" STRING COMMENT 'Trgt Name',
	"succ_f" STRING COMMENT 'Success Flag',
	"comt_f" STRING COMMENT 'Complete Flag',
	"comt_s" TIMESTAMP_NTZ(6) COMMENT 'Complete Timestamp',
	"mlti_load_efft_d" DATE COMMENT 'Multiple Load Effect Date',
	"syst_s" TIMESTAMP_NTZ(6) COMMENT 'System Timestamp',
	"mlti_load_comt_s" TIMESTAMP_NTZ(6) COMMENT 'Multiple Load Complete Timestamp',
	"syst_et_q" NUMBER(38,0) COMMENT 'System Et Count',
	"syst_uv_q" NUMBER(38,0) COMMENT 'System Uv Count',
	"syst_ins_q" NUMBER(38,0) COMMENT 'System Ins Count',
	"syst_upd_q" NUMBER(38,0) COMMENT 'System Upd Count',
	"syst_del_q" NUMBER(38,0) COMMENT 'System Del Count',
	"syst_et_tabl_m" STRING COMMENT 'System Et Table Name',
	"syst_uv_tabl_m" STRING COMMENT 'System Uv Table Name',
	"syst_head_et_tabl_m" STRING COMMENT 'System Head Et Table Name',
	"syst_head_uv_tabl_m" STRING COMMENT 'System Head Uv Table Name',
	"syst_trlr_et_tabl_m" STRING COMMENT 'System Trailer Et Table Name',
	"syst_trlr_uv_tabl_m" STRING COMMENT 'System Trailer Uv Table Name',
	"prev_pros_key_i" NUMBER(10,0) COMMENT 'Previous Process Key Identifier',
	"head_recd_type_c" STRING COMMENT 'Head Record Type Code',
	"head_file_m" STRING COMMENT 'Head File Name',
	"head_btch_run_d" DATE COMMENT 'Head Batch Run Date',
	"head_file_crat_s" TIMESTAMP_NTZ(6) COMMENT 'Head File Create Timestamp',
	"head_genr_prgm_m" STRING COMMENT 'Head Generate Program Name',
	"head_btch_key_i" NUMBER(10,0) COMMENT 'Head Batch Key Identifier',
	"head_pros_key_i" NUMBER(10,0) COMMENT 'Head Process Key Identifier',
	"head_pros_prev_key_i" NUMBER(10,0) COMMENT 'Head Process Previous Key Identifier',
	"trlr_recd_type_c" STRING COMMENT 'Trailer Record Type Code',
	"trlr_recd_q" NUMBER(38,0) COMMENT 'Trailer Record Count',
	"trlr_hash_totl_a" NUMBER(18,4) COMMENT 'Trailer Hash Total Amount',
	"trlr_colm_hash_totl_m" STRING COMMENT 'Trailer Column Hash Total Name',
	"trlr_eror_recd_q" NUMBER(38,0) COMMENT 'Trailer Error Record Count',
	"trlr_file_comt_s" TIMESTAMP_NTZ(6) COMMENT 'Trailer File Complete Timestamp',
	"trlr_recd_isrt_q" NUMBER(38,0) COMMENT 'Trailer Record Insert Count',
	"trlr_recd_updt_q" NUMBER(38,0) COMMENT 'Trailer Record Update Count',
	"trlr_recd_delt_q" NUMBER(38,0) COMMENT 'Trailer Record Delete Count'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdsecurity"."row_levl_secu_user_prfl" (
	"username" STRING NOT NULL COMMENT 'User Name',
	"row_secu_prfl_c" BINARY(130) NOT NULL COMMENT 'Row Security Profile Code',
	"my_service_no" STRING COMMENT 'MyServiceNo',
	"req_no" STRING COMMENT 'REQNo',
	"ritm_no" STRING COMMENT 'RITMNo',
	"sar_no" STRING COMMENT 'SARNo',
	"cmmt" STRING COMMENT 'Comments',
	"updt_username" STRING NOT NULL COMMENT 'User Name',
	"updt_date" DATE NOT NULL COMMENT 'Update Date',
	"updt_dtts" TIMESTAMP_NTZ(6) NOT NULL COMMENT 'Update DateTime'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."plan_baln_segm_mstr" (
	"corp_idnn" STRING NOT NULL COMMENT 'Corporation Identification',
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"plan_idnn" STRING NOT NULL COMMENT 'Plan Identification',
	"plan_sqno" NUMBER(3,0) COMMENT 'Plan Sequence Number',
	"srce_syst_acct_numb" STRING COMMENT 'Source System Account Number',
	"stor_idnn" STRING COMMENT 'Store Identification',
	"orig_stat" STRING COMMENT 'Original State',
	"plan_type" STRING COMMENT 'Plan Type',
	"plan_catg" STRING COMMENT 'Plan Category',
	"post_pur_stus" STRING COMMENT 'Post Purchase Status',
	"term_stus" STRING COMMENT 'Terms Status',
	"int_defr_stus" STRING COMMENT 'Interest Defer Status',
	"payt_defr_stus" STRING COMMENT 'Payment Defer Status',
	"time_bill" NUMBER(3,0) COMMENT 'Times Billed',
	"spec_term_cycl" NUMBER(3,0) COMMENT 'Special Terms Cycle',
	"int_defr_cycl" NUMBER(3,0) COMMENT 'Interest Defer Cycle',
	"payt_defr_cycl" NUMBER(3,0) COMMENT 'Payment Defer Cycle',
	"date_spec_term_end" DATE COMMENT 'Date Special Terms End',
	"date_int_defr_end" DATE COMMENT 'Date Interest Defer End',
	"date_payt_defr_end" DATE COMMENT 'Date Payment Defer End',
	"date_frst_tran" DATE COMMENT 'Date First Transaction',
	"date_paid_off" DATE COMMENT 'Date Paid Off',
	"date_last_payt" DATE COMMENT 'Date Last Payment',
	"date_last_mntn" DATE COMMENT 'Date Last Maintenance',
	"lftd_high_baln" NUMBER(13,2) COMMENT 'Life To Date High Balance',
	"payt_tabl_high_baln" NUMBER(13,2) COMMENT 'Payment Table High Balance',
	"lftd_ichg" NUMBER(13,2) COMMENT 'Life to Date Interest Charged',
	"lftd_int_wavr" NUMBER(13,2) COMMENT 'Life to Date Interest Waived',
	"fix_payt_amt" NUMBER(13,2) COMMENT 'Fixed Payment Amount',
	"fix_pacy" NUMBER(3,0) COMMENT 'Fixed Payment Cycle',
	"last_irte" NUMBER(7,2) COMMENT 'Last Interest Rate',
	"last_rate_code" STRING COMMENT 'Last Rate Code',
	"last_rate_code_seqn" NUMBER(4,0) COMMENT 'Last Rate Code Sequence',
	"last_min_payt_code" STRING COMMENT 'Last Minimum Payment Code',
	"last_min_payt_code_seqn" NUMBER(4,0) COMMENT 'Last Minimum Payment Code Sequence',
	"last_min_payt_amt" NUMBER(13,2) COMMENT 'Last Minimum Payment Amount',
	"curr_min_payt_due" NUMBER(13,2) COMMENT 'Current Minimum Payment Due',
	"min_payt_past_due" NUMBER(13,2) COMMENT 'Minimum Payment Past Due',
	"last_appy_payt_amt" NUMBER(13,2) COMMENT 'Last Applied Payment Amount',
	"payt_bfor_grce" NUMBER(13,2) COMMENT 'Payments Before Grace',
	"orig_baln" NUMBER(13,2) COMMENT 'Original Balance',
	"int_save_rate" NUMBER(2,2) COMMENT 'Interest Saver Rate',
	"plan_due_date" DATE COMMENT 'Plan Due Date',
	"int_fee_end_date" DATE COMMENT 'Interest Fee End Date',
	"int_free_baln" NUMBER(13,2) COMMENT 'Interest Free Balance',
	"open_baln_curr_cycl" NUMBER(13,2) COMMENT 'Opening Balance Current Cycle',
	"ctd_sale_tran" NUMBER(13,2) COMMENT 'Cycle To Date Sales Transactions',
	"ctd_dr_tran" NUMBER(13,2) COMMENT 'Cycle To Date Debit Transactions',
	"ctd_cr_tran" NUMBER(13,2) COMMENT 'Cycle To Date Credit Transactions',
	"ctd_payt_tran" NUMBER(13,2) COMMENT 'Cycle To Date Payment Transactions',
	"int_unpd_open_not_acrl_baln" NUMBER(13,2) COMMENT 'Interest Unpaid Opening Not Accrued Balance',
	"int_unpd_ctd_not_acrl_baln" NUMBER(13,2) COMMENT 'Interest Unpaid Cycle To Date Not Accrued Balance',
	"int_unpd_curr_cycl_baln" NUMBER(13,2) COMMENT 'Interest Unpaid Current Cycle Balance',
	"int_unpd_1_cycl_ago_baln" NUMBER(13,2) COMMENT 'Interest Unpaid 1 Cycle AGO Balance',
	"int_unpd_2_cycl_ago_baln" NUMBER(13,2) COMMENT 'Interest Unpaid 2 Cycle AGO Balance',
	"fee_unpd_open_not_acrl_baln" NUMBER(13,2) COMMENT 'Fees Unpaid Opening Not Accrued Balance',
	"fee_unpd_ctd_not_acrl_baln" NUMBER(13,2) COMMENT 'Fees Unpaid Cycle To Date Not Accrued Balance',
	"fee_unpd_curr_cycl_baln" NUMBER(13,2) COMMENT 'Fees Unpaid Current Cycle Balance',
	"fee_unpd_1_cycl_ago_baln" NUMBER(13,2) COMMENT 'Fees Unpaid 1 Cycle AGO Balance',
	"fee_unpd_2_cycl_ago_baln" NUMBER(13,2) COMMENT 'Fees Unpaid 2 Cycle AGO Balance',
	"amf_unpd_open_not_acrl_baln" NUMBER(13,2) COMMENT 'Annual Membership Fee Unpaid Opening Not Accrued Balance',
	"amf_unpd_ctd_not_acrl_baln" NUMBER(13,2) COMMENT 'Annual Membership Fee Unpaid Cycle To Date Not Accrued Balan',
	"amf_unpd_curr_cycl_baln" NUMBER(13,2) COMMENT 'Annual Membership Fee Unpaid Current Cycle Balance',
	"amf_unpd_1_cycl_ago_baln" NUMBER(13,2) COMMENT 'Annual Membership Fee Unpaid 1 Cycle AGO Balance',
	"amf_unpd_2_cycl_ago_baln" NUMBER(13,2) COMMENT 'Annual Membership Fee Unpaid 2 Cycle AGO Balance',
	"insr_unpd_open_not_acrl_baln" NUMBER(13,2) COMMENT 'Insurance Unpaid Opening Not Accrued Balance',
	"insr_unpd_ctd_not_acrl_baln" NUMBER(13,2) COMMENT 'Insurance Unpaid Cycle To Date Not Accrued Balance',
	"insr_unpd_curr_cycl_baln" NUMBER(13,2) COMMENT 'Insurance Unpaid Current Cycle Balance',
	"insr_unpd_1_cycl_ago_baln" NUMBER(13,2) COMMENT 'Insurance Unpaid 1 Cycle AGO Balance',
	"insr_unpd_2_cycl_ago_baln" NUMBER(13,2) COMMENT 'Insurance Unpaid 2 Cycle AGO Balance',
	"prin_unpd_open_not_acrl_baln" NUMBER(13,2) COMMENT 'Principal Unpaid Opening Not Accrued Balance',
	"prin_unpd_ctd_not_acrl_baln" NUMBER(13,2) COMMENT 'Principal Unpaid Cycle To Date Not Accrued Balance',
	"prin_unpd_curr_cycl_baln" NUMBER(13,2) COMMENT 'Principal Unpaid Current Cycle Balance',
	"prin_unpd_1_cycl_ago_baln" NUMBER(13,2) COMMENT 'Principal Unpaid 1 Cycle AGO Balance',
	"prin_unpd_2_cycl_ago_baln" NUMBER(13,2) COMMENT 'Principal Unpaid 2 Cycle AGO Balance',
	"aggr_curr_cycl_baln" NUMBER(15,2) COMMENT 'Aggregate Current Cycle Balance',
	"aggr_1_cycl_ago_baln" NUMBER(15,2) COMMENT 'Aggregate 1 Cycle AGO Balance',
	"aggr_2_cycl_ago_baln" NUMBER(15,2) COMMENT 'Aggregate 2 Cycle AGO Balance',
	"aggr_dlay_baln" NUMBER(15,2) COMMENT 'Aggregate Delay Balance',
	"dlay_day_qnty" NUMBER(3,0) COMMENT 'Delayed Day Quantity',
	"ctd_csad_fee_amt" NUMBER(13,2) COMMENT 'Cycle To Date Cash Advance Fee Amount',
	"ctd_syst_genr_csad_fee_amt" NUMBER(13,2) COMMENT 'Cycle To Date System Generated Cash Advance Fee Amount',
	"int_defr_day_qnty" NUMBER(3,0) COMMENT 'Interest Deferred Days Quantity',
	"int_defr_amt" NUMBER(13,2) COMMENT 'Interest Deferred Amount',
	"int_defr_aggr_curr_baln" NUMBER(15,2) COMMENT 'Interest Deferred Aggregate Current Balance',
	"user_code" STRING COMMENT 'User Code',
	"migr_to_plan_idnn" STRING COMMENT 'Migrate To Plan Identification',
	"migr_to_plan_sqno" NUMBER(3,0) COMMENT 'Migrate To Plan Sequence Number',
	"date_migr" DATE COMMENT 'Date Migrate',
	"dspt_amt" NUMBER(13,2) COMMENT 'Disputed Amount',
	"excl_dspt_amt" NUMBER(13,2) COMMENT 'Excluded Disputed Amount',
	"acrl_dspt_amt" NUMBER(13,2) COMMENT 'Accrued Disputed Amount',
	"frnt_load_ismt_bill" NUMBER(3,0) COMMENT 'Front Load Install Billed',
	"frnt_load_last_ismt_date" DATE COMMENT 'Front Load Last Install Date',
	"frnt_load_orig_int_amt" NUMBER(13,2) COMMENT 'Front Load Original Interest Amount',
	"frnt_load_earn_int_amt" NUMBER(13,2) COMMENT 'Front Load Earned Interest Amount',
	"frnt_load_rebt_int_amt" NUMBER(13,2) COMMENT 'Front Load Rebate Interest Amount',
	"frnt_load_irte" NUMBER(6,6) COMMENT 'Front Load Interest Rate',
	"frnt_load_orig_insr_amt" NUMBER(13,2) COMMENT 'Front Load Original Insurance Amount',
	"frnt_load_earn_insr_amt" NUMBER(13,2) COMMENT 'Front Load Earned Insurance Amount',
	"frnt_load_upfr_insr_amt" NUMBER(13,2) COMMENT 'Front Load Up Front Insurance Amount',
	"frnt_load_rebt_insr_amt" NUMBER(13,2) COMMENT 'Front Load Rebate Insurance Amount',
	"sche_payf_date" DATE COMMENT 'Scheduled Payoff Date',
	"sche_payf_amt" NUMBER(13,2) COMMENT 'Scheduled Payoff Amount',
	"sche_payf_fee" NUMBER(13,2) COMMENT 'Scheduled Payoff Fee',
	"sche_payf_reas" STRING COMMENT 'Scheduled Payoff Reason',
	"actl_payf_date" DATE COMMENT 'Actual Payoff Date',
	"actl_payf_amt" NUMBER(13,2) COMMENT 'Actual Payoff Amount',
	"frnt_load_insr_paid" NUMBER(13,2) COMMENT 'Front Load Insurance Paid',
	"frnt_load_rebt_insr_paid" NUMBER(13,2) COMMENT 'Front Load Rebate Insurance Paid',
	"govt_chrg_not_acrl_open_amt" NUMBER(13,2) COMMENT 'Government Charges Unpaid Non Accruing Open Amount',
	"govt_chrg_not_acrl_ctd_amt" NUMBER(13,2) COMMENT 'Government Charges Non-Accruing Cycle-To-Date Amount',
	"govt_chrg_curr_cycl_amt" NUMBER(13,2) COMMENT 'Government Charges Current Cycle Amount',
	"govt_chrg_1_cycl_ago_amt" NUMBER(13,2) COMMENT 'Government Charges 1 Cycle Ago Amount',
	"govt_chrg_2_cycl_ago_amt" NUMBER(13,2) COMMENT 'Government Charges 2 Cycle Ago Amount',
	"ctd_fncl_chrg_rev" NUMBER(7,2) COMMENT 'Cycle To Date Finance Charge Reversals',
	"ismt_qnty" NUMBER(3,0) COMMENT 'Number Of Installments',
	"ismt_prev_qnty" NUMBER(3,0) COMMENT 'Previous Number Of Installments',
	"date_ismt_term_chng" DATE COMMENT 'Date Instalment Term Changed',
	"prev_min_payt_amt" NUMBER(13,2) COMMENT 'Previous Minimum Payment Amount',
	"orig_loan_baln" NUMBER(13,2) COMMENT 'Original Loan Balance',
	"lftd_all_cr" NUMBER(13,2) COMMENT 'Life To Date All Credits',
	"lftd_int_save" NUMBER(13,2) COMMENT 'Life To Date Interest Saved',
	"prev_cycl_int_save" NUMBER(13,2) COMMENT 'Previous Cycle Interest Saved',
	"date_loan_paid_out" DATE COMMENT 'Date Loan Paid Out',
	"prjc_pay_off_date" DATE COMMENT 'Projected Pay-Off Date',
	"dspt_qnty" NUMBER(3,0) COMMENT 'Disputed Quantity',
	"dspt_old_date" DATE COMMENT 'Disputed Old Date',
	"term" NUMBER(13,2) COMMENT 'Term',
	"row_i" NUMBER(38,0) COMMENT 'Row Identifier',
	"efft_s" TIMESTAMP_NTZ(6) NOT NULL COMMENT 'Effective Timestamp',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"efft_t" TIME(6) COMMENT 'Effective Time',
	"pros_key_efft_i" NUMBER(38,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."plan_baln_segm_mstr_arch" (
	"corp_idnn" STRING NOT NULL COMMENT 'Corporation Identification',
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"plan_idnn" STRING NOT NULL COMMENT 'Plan Identification',
	"plan_sqno" NUMBER(3,0) COMMENT 'Plan Sequence Number',
	"srce_syst_acct_numb" STRING COMMENT 'Source System Account Number',
	"stor_idnn" STRING COMMENT 'Store Identification',
	"orig_stat" STRING COMMENT 'Original State',
	"plan_type" STRING COMMENT 'Plan Type',
	"plan_catg" STRING COMMENT 'Plan Category',
	"post_pur_stus" STRING COMMENT 'Post Purchase Status',
	"term_stus" STRING COMMENT 'Terms Status',
	"int_defr_stus" STRING COMMENT 'Interest Defer Status',
	"payt_defr_stus" STRING COMMENT 'Payment Defer Status',
	"time_bill" NUMBER(3,0) COMMENT 'Times Billed',
	"spec_term_cycl" NUMBER(3,0) COMMENT 'Special Terms Cycle',
	"int_defr_cycl" NUMBER(3,0) COMMENT 'Interest Defer Cycle',
	"payt_defr_cycl" NUMBER(3,0) COMMENT 'Payment Defer Cycle',
	"date_spec_term_end" DATE COMMENT 'Date Special Terms End',
	"date_int_defr_end" DATE COMMENT 'Date Interest Defer End',
	"date_payt_defr_end" DATE COMMENT 'Date Payment Defer End',
	"date_frst_tran" DATE COMMENT 'Date First Transaction',
	"date_paid_off" DATE COMMENT 'Date Paid Off',
	"date_last_payt" DATE COMMENT 'Date Last Payment',
	"date_last_mntn" DATE COMMENT 'Date Last Maintenance',
	"lftd_high_baln" NUMBER(13,2) COMMENT 'Life To Date High Balance',
	"payt_tabl_high_baln" NUMBER(13,2) COMMENT 'Payment Table High Balance',
	"lftd_ichg" NUMBER(13,2) COMMENT 'Life to Date Interest Charged',
	"lftd_int_wavr" NUMBER(13,2) COMMENT 'Life to Date Interest Waived',
	"fix_payt_amt" NUMBER(13,2) COMMENT 'Fixed Payment Amount',
	"fix_pacy" NUMBER(3,0) COMMENT 'Fixed Payment Cycle',
	"last_irte" NUMBER(7,2) COMMENT 'Last Interest Rate',
	"last_rate_code" STRING COMMENT 'Last Rate Code',
	"last_rate_code_seqn" NUMBER(4,0) COMMENT 'Last Rate Code Sequence',
	"last_min_payt_code" STRING COMMENT 'Last Minimum Payment Code',
	"last_min_payt_code_seqn" NUMBER(4,0) COMMENT 'Last Minimum Payment Code Sequence',
	"last_min_payt_amt" NUMBER(13,2) COMMENT 'Last Minimum Payment Amount',
	"curr_min_payt_due" NUMBER(13,2) COMMENT 'Current Minimum Payment Due',
	"min_payt_past_due" NUMBER(13,2) COMMENT 'Minimum Payment Past Due',
	"last_appy_payt_amt" NUMBER(13,2) COMMENT 'Last Applied Payment Amount',
	"payt_bfor_grce" NUMBER(13,2) COMMENT 'Payments Before Grace',
	"orig_baln" NUMBER(13,2) COMMENT 'Original Balance',
	"int_save_rate" NUMBER(2,2) COMMENT 'Interest Saver Rate',
	"plan_due_date" DATE COMMENT 'Plan Due Date',
	"int_fee_end_date" DATE COMMENT 'Interest Fee End Date',
	"int_free_baln" NUMBER(13,2) COMMENT 'Interest Free Balance',
	"open_baln_curr_cycl" NUMBER(13,2) COMMENT 'Opening Balance Current Cycle',
	"ctd_sale_tran" NUMBER(13,2) COMMENT 'Cycle To Date Sales Transactions',
	"ctd_dr_tran" NUMBER(13,2) COMMENT 'Cycle To Date Debit Transactions',
	"ctd_cr_tran" NUMBER(13,2) COMMENT 'Cycle To Date Credit Transactions',
	"ctd_payt_tran" NUMBER(13,2) COMMENT 'Cycle To Date Payment Transactions',
	"int_unpd_open_not_acrl_baln" NUMBER(13,2) COMMENT 'Interest Unpaid Opening Not Accrued Balance',
	"int_unpd_ctd_not_acrl_baln" NUMBER(13,2) COMMENT 'Interest Unpaid Cycle To Date Not Accrued Balance',
	"int_unpd_curr_cycl_baln" NUMBER(13,2) COMMENT 'Interest Unpaid Current Cycle Balance',
	"int_unpd_1_cycl_ago_baln" NUMBER(13,2) COMMENT 'Interest Unpaid 1 Cycle AGO Balance',
	"int_unpd_2_cycl_ago_baln" NUMBER(13,2) COMMENT 'Interest Unpaid 2 Cycle AGO Balance',
	"fee_unpd_open_not_acrl_baln" NUMBER(13,2) COMMENT 'Fees Unpaid Opening Not Accrued Balance',
	"fee_unpd_ctd_not_acrl_baln" NUMBER(13,2) COMMENT 'Fees Unpaid Cycle To Date Not Accrued Balance',
	"fee_unpd_curr_cycl_baln" NUMBER(13,2) COMMENT 'Fees Unpaid Current Cycle Balance',
	"fee_unpd_1_cycl_ago_baln" NUMBER(13,2) COMMENT 'Fees Unpaid 1 Cycle AGO Balance',
	"fee_unpd_2_cycl_ago_baln" NUMBER(13,2) COMMENT 'Fees Unpaid 2 Cycle AGO Balance',
	"amf_unpd_open_not_acrl_baln" NUMBER(13,2) COMMENT 'Annual Membership Fee Unpaid Opening Not Accrued Balance',
	"amf_unpd_ctd_not_acrl_baln" NUMBER(13,2) COMMENT 'Annual Membership Fee Unpaid Cycle To Date Not Accrued Balan',
	"amf_unpd_curr_cycl_baln" NUMBER(13,2) COMMENT 'Annual Membership Fee Unpaid Current Cycle Balance',
	"amf_unpd_1_cycl_ago_baln" NUMBER(13,2) COMMENT 'Annual Membership Fee Unpaid 1 Cycle AGO Balance',
	"amf_unpd_2_cycl_ago_baln" NUMBER(13,2) COMMENT 'Annual Membership Fee Unpaid 2 Cycle AGO Balance',
	"insr_unpd_open_not_acrl_baln" NUMBER(13,2) COMMENT 'Insurance Unpaid Opening Not Accrued Balance',
	"insr_unpd_ctd_not_acrl_baln" NUMBER(13,2) COMMENT 'Insurance Unpaid Cycle To Date Not Accrued Balance',
	"insr_unpd_curr_cycl_baln" NUMBER(13,2) COMMENT 'Insurance Unpaid Current Cycle Balance',
	"insr_unpd_1_cycl_ago_baln" NUMBER(13,2) COMMENT 'Insurance Unpaid 1 Cycle AGO Balance',
	"insr_unpd_2_cycl_ago_baln" NUMBER(13,2) COMMENT 'Insurance Unpaid 2 Cycle AGO Balance',
	"prin_unpd_open_not_acrl_baln" NUMBER(13,2) COMMENT 'Principal Unpaid Opening Not Accrued Balance',
	"prin_unpd_ctd_not_acrl_baln" NUMBER(13,2) COMMENT 'Principal Unpaid Cycle To Date Not Accrued Balance',
	"prin_unpd_curr_cycl_baln" NUMBER(13,2) COMMENT 'Principal Unpaid Current Cycle Balance',
	"prin_unpd_1_cycl_ago_baln" NUMBER(13,2) COMMENT 'Principal Unpaid 1 Cycle AGO Balance',
	"prin_unpd_2_cycl_ago_baln" NUMBER(13,2) COMMENT 'Principal Unpaid 2 Cycle AGO Balance',
	"aggr_curr_cycl_baln" NUMBER(15,2) COMMENT 'Aggregate Current Cycle Balance',
	"aggr_1_cycl_ago_baln" NUMBER(15,2) COMMENT 'Aggregate 1 Cycle AGO Balance',
	"aggr_2_cycl_ago_baln" NUMBER(15,2) COMMENT 'Aggregate 2 Cycle AGO Balance',
	"aggr_dlay_baln" NUMBER(15,2) COMMENT 'Aggregate Delay Balance',
	"dlay_day_qnty" NUMBER(3,0) COMMENT 'Delayed Day Quantity',
	"ctd_csad_fee_amt" NUMBER(13,2) COMMENT 'Cycle To Date Cash Advance Fee Amount',
	"ctd_syst_genr_csad_fee_amt" NUMBER(13,2) COMMENT 'Cycle To Date System Generated Cash Advance Fee Amount',
	"int_defr_day_qnty" NUMBER(3,0) COMMENT 'Interest Deferred Days Quantity',
	"int_defr_amt" NUMBER(13,2) COMMENT 'Interest Deferred Amount',
	"int_defr_aggr_curr_baln" NUMBER(15,2) COMMENT 'Interest Deferred Aggregate Current Balance',
	"user_code" STRING COMMENT 'User Code',
	"migr_to_plan_idnn" STRING COMMENT 'Migrate To Plan Identification',
	"migr_to_plan_sqno" NUMBER(3,0) COMMENT 'Migrate To Plan Sequence Number',
	"date_migr" DATE COMMENT 'Date Migrate',
	"dspt_amt" NUMBER(13,2) COMMENT 'Disputed Amount',
	"excl_dspt_amt" NUMBER(13,2) COMMENT 'Excluded Disputed Amount',
	"acrl_dspt_amt" NUMBER(13,2) COMMENT 'Accrued Disputed Amount',
	"frnt_load_ismt_bill" NUMBER(3,0) COMMENT 'Front Load Install Billed',
	"frnt_load_last_ismt_date" DATE COMMENT 'Front Load Last Install Date',
	"frnt_load_orig_int_amt" NUMBER(13,2) COMMENT 'Front Load Original Interest Amount',
	"frnt_load_earn_int_amt" NUMBER(13,2) COMMENT 'Front Load Earned Interest Amount',
	"frnt_load_rebt_int_amt" NUMBER(13,2) COMMENT 'Front Load Rebate Interest Amount',
	"frnt_load_irte" NUMBER(6,6) COMMENT 'Front Load Interest Rate',
	"frnt_load_orig_insr_amt" NUMBER(13,2) COMMENT 'Front Load Original Insurance Amount',
	"frnt_load_earn_insr_amt" NUMBER(13,2) COMMENT 'Front Load Earned Insurance Amount',
	"frnt_load_upfr_insr_amt" NUMBER(13,2) COMMENT 'Front Load Up Front Insurance Amount',
	"frnt_load_rebt_insr_amt" NUMBER(13,2) COMMENT 'Front Load Rebate Insurance Amount',
	"sche_payf_date" DATE COMMENT 'Scheduled Payoff Date',
	"sche_payf_amt" NUMBER(13,2) COMMENT 'Scheduled Payoff Amount',
	"sche_payf_fee" NUMBER(13,2) COMMENT 'Scheduled Payoff Fee',
	"sche_payf_reas" STRING COMMENT 'Scheduled Payoff Reason',
	"actl_payf_date" DATE COMMENT 'Actual Payoff Date',
	"actl_payf_amt" NUMBER(13,2) COMMENT 'Actual Payoff Amount',
	"frnt_load_insr_paid" NUMBER(13,2) COMMENT 'Front Load Insurance Paid',
	"frnt_load_rebt_insr_paid" NUMBER(13,2) COMMENT 'Front Load Rebate Insurance Paid',
	"govt_chrg_not_acrl_open_amt" NUMBER(13,2) COMMENT 'Government Charges Unpaid Non Accruing Open Amount',
	"govt_chrg_not_acrl_ctd_amt" NUMBER(13,2) COMMENT 'Government Charges Non-Accruing Cycle-To-Date Amount',
	"govt_chrg_curr_cycl_amt" NUMBER(13,2) COMMENT 'Government Charges Current Cycle Amount',
	"govt_chrg_1_cycl_ago_amt" NUMBER(13,2) COMMENT 'Government Charges 1 Cycle Ago Amount',
	"govt_chrg_2_cycl_ago_amt" NUMBER(13,2) COMMENT 'Government Charges 2 Cycle Ago Amount',
	"ctd_fncl_chrg_rev" NUMBER(7,2) COMMENT 'Cycle To Date Finance Charge Reversals',
	"ismt_qnty" NUMBER(3,0) COMMENT 'Number Of Installments',
	"ismt_prev_qnty" NUMBER(3,0) COMMENT 'Previous Number Of Installments',
	"date_ismt_term_chng" DATE COMMENT 'Date Instalment Term Changed',
	"prev_min_payt_amt" NUMBER(13,2) COMMENT 'Previous Minimum Payment Amount',
	"orig_loan_baln" NUMBER(13,2) COMMENT 'Original Loan Balance',
	"lftd_all_cr" NUMBER(13,2) COMMENT 'Life To Date All Credits',
	"lftd_int_save" NUMBER(13,2) COMMENT 'Life To Date Interest Saved',
	"prev_cycl_int_save" NUMBER(13,2) COMMENT 'Previous Cycle Interest Saved',
	"date_loan_paid_out" DATE COMMENT 'Date Loan Paid Out',
	"prjc_pay_off_date" DATE COMMENT 'Projected Pay-Off Date',
	"dspt_qnty" NUMBER(3,0) COMMENT 'Disputed Quantity',
	"dspt_old_date" DATE COMMENT 'Disputed Old Date',
	"term" NUMBER(13,2) COMMENT 'Term',
	"row_i" NUMBER(38,0) COMMENT 'Row Identifier',
	"efft_s" TIMESTAMP_NTZ(6) NOT NULL COMMENT 'Effective Timestamp',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"efft_t" TIME(6) COMMENT 'Effective Time',
	"pros_key_efft_i" NUMBER(38,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_btch_isac" (
	"btch_key_i" NUMBER(10,0) NOT NULL,
	"btch_rqst_s" TIMESTAMP_NTZ(6),
	"btch_run_d" DATE NOT NULL,
	"btch_run_t" TIME(6) NOT NULL,
	"srce_syst_m" STRING NOT NULL,
	"srce_sub_syst_m" STRING NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_parm" (
	"parm_m" STRING NOT NULL,
	"parm_ltrl_n" NUMBER(38,0),
	"parm_ltrl_d" DATE,
	"parm_ltrl_strg_x" STRING,
	"parm_ltrl_a" NUMBER(18,4)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_pros_isac" (
	"pros_key_i" NUMBER(10,0) NOT NULL,
	"conv_m" STRING,
	"conv_type_m" STRING,
	"pros_rqst_s" TIMESTAMP_NTZ(6),
	"pros_last_rqst_s" TIMESTAMP_NTZ(6),
	"pros_rqst_q" NUMBER(38,0),
	"btch_key_i" NUMBER(10,0),
	"pros_efft_s" TIMESTAMP_NTZ(6),
	"stus_c" STRING,
	"stus_chng_s" TIMESTAMP_NTZ(6),
	"srce_syst_m" STRING,
	"srce_m" STRING,
	"trgt_m" STRING,
	"comt_f" STRING,
	"comt_s" TIMESTAMP_NTZ(6),
	"syst_s" TIMESTAMP_NTZ(6),
	"syst_et_q" NUMBER(38,0),
	"syst_uv_q" NUMBER(38,0),
	"syst_ins_q" NUMBER(38,0),
	"syst_upd_q" NUMBER(38,0),
	"syst_del_q" NUMBER(38,0),
	"syst_et_tabl_m" STRING,
	"syst_uv_tabl_m" STRING,
	"srce_recd_cnt" NUMBER(38,0),
	"srce_load_cnt" NUMBER(38,0),
	"srce_btch_load_cnt" NUMBER(38,0)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdsrccs"."util_trsf_eror_rqm3" (
	"srce_key_i" STRING NOT NULL,
	"conv_m" STRING NOT NULL,
	"conv_map_rule_m" STRING,
	"trsf_tabl_m" STRING,
	"srce_efft_d" DATE NOT NULL,
	"valu_chng_bfor_x" STRING,
	"valu_chng_aftr_x" STRING,
	"trsf_x" STRING,
	"trsf_colm_m" STRING,
	"eror_seqn_i" NUMBER(10,0),
	"srce_file_m" STRING,
	"pros_key_efft_i" NUMBER(10,0),
	"trsf_key_i" STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."pdtrpc"."wknd_pblc_hldy" (
	"calr_d" DATE,
	"wknd_pblc_hldy_f" STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."putil"."pros_eror_log" (
	"srce_syst_c" STRING,
	"srce_file_m" STRING,
	"calg_pros_db_m" STRING,
	"calg_pros_m" STRING,
	"eror_time_s" TIMESTAMP_NTZ(6),
	"sql_stat_c" NUMBER(6,0),
	"sql_code_c" NUMBER(6,0),
	"actv_cnt_q" NUMBER(6,0),
	"abrt_pros_f" STRING,
	"eror_msge_x" STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."putil"."terasync" (
	"cookie" NUMBER(38,0) NOT NULL,
	"optype" NUMBER(38,0) NOT NULL,
	"ctlstate" NUMBER(38,0) NOT NULL,
	"eventcount" NUMBER(38,0) NOT NULL,
	"lsn" NUMBER(38,0),
	"blockcount" NUMBER(38,0),
	"query" STRING,
	"username" STRING,
	"dbname" STRING,
	"recordcount" double,
	"bytecount" double,
	"rejectcount" double,
	"starttime" NUMBER(38,0),
	"endtime" NUMBER(38,0)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_appt_pdct" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"appt_pdct_i" STRING NOT NULL COMMENT 'Application Product Identifier',
	"rel_type_c" STRING NOT NULL COMMENT 'Relation Type Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_rel" (
	"subj_acct_i" STRING NOT NULL,
	"objc_acct_i" STRING NOT NULL,
	"rel_c" STRING NOT NULL,
	"efft_d" DATE NOT NULL,
	"expy_d" DATE NOT NULL,
	"strt_d" DATE,
	"rel_expy_d" DATE,
	"pros_key_efft_i" NUMBER(10,0),
	"pros_key_expy_i" NUMBER(10,0),
	"eror_seqn_i" NUMBER(10,0),
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE',
	"rel_stus_c" STRING COMMENT 'RELATIONSHIP STATUS TYPECODE',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"subj_acct_levl_n" NUMBER(38,0) COMMENT 'Subject Account Level Number',
	"objc_acct_levl_n" NUMBER(38,0) COMMENT 'Object Account Level Number',
	"crin_shre_p" NUMBER(5,2) COMMENT 'Credit Interest Share Percentage',
	"dr_int_shre_p" NUMBER(5,2) COMMENT 'Debit Interest Share Percentage',
	"record_deleted_flag" NUMBER(38,0) NOT NULL COMMENT 'Record Deleted Flag',
	"ctl_id" NUMBER(38,0) NOT NULL COMMENT 'Ctl Id',
	"process_name" STRING NOT NULL COMMENT 'Process Name',
	"process_id" NUMBER(38,0) NOT NULL COMMENT 'Process Id',
	"update_process_name" STRING COMMENT 'Update Process Name',
	"update_process_id" NUMBER(38,0) COMMENT 'Update Process Id'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_unid_paty" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"srce_syst_paty_i" STRING NOT NULL COMMENT 'Source System Party Identifier',
	"srce_syst_c" STRING NOT NULL COMMENT 'SOURCE SYSTEM CODE',
	"paty_acct_rel_c" STRING NOT NULL COMMENT 'PARTY ACCOUNT RELATIONSHIP CODE',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE',
	"orig_srce_syst_c" STRING COMMENT 'Original Source System Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_xref_bps_cbs" (
	"cbs_acct_i" STRING NOT NULL COMMENT 'Cbs AccountIdentifier',
	"bps_acct_i" STRING NOT NULL COMMENT 'Bps AccountIdentifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) COMMENT 'Process Key EffectiveIdentifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key ExpiryIdentifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error SequenceIdentifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."acct_xref_mas_dar" (
	"mas_merc_acct_i" STRING NOT NULL COMMENT 'MerchantIdentifier',
	"dar_acct_i" STRING NOT NULL COMMENT 'Cbs AccountIdentifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) COMMENT 'Process Key EffectiveIdentifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key ExpiryIdentifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error SequenceIdentifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt" (
	"appt_i" STRING NOT NULL COMMENT 'Application Identifier',
	"appt_c" STRING COMMENT 'Application Code',
	"appt_form_c" STRING COMMENT 'Application Form Code',
	"appt_qlfy_c" STRING NOT NULL COMMENT 'Application Qualifier Code',
	"stus_trak_i" STRING COMMENT 'Status Tracking Identifier',
	"appt_orig_c" STRING COMMENT 'Application Origin Code',
	"appt_n" STRING COMMENT 'Application Number',
	"srce_syst_c" STRING NOT NULL COMMENT 'Source System Code',
	"srce_syst_appt_i" STRING NOT NULL COMMENT 'Source System Application Identifier',
	"appt_crat_d" DATE COMMENT 'Application Create Date',
	"rate_seek_f" STRING COMMENT 'Rate Seeker Flag',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	"orig_appt_srce_c" STRING COMMENT 'Original Application Source Code',
	"rel_mgr_stat_c" STRING COMMENT 'Relationship Manager State Code',
	"appt_recv_s" TIMESTAMP_NTZ(6) COMMENT 'Application Received Timestamp',
	"appt_recv_d" DATE COMMENT 'Application Received Date',
	"appt_recv_t" TIME(6) COMMENT 'Application Received Time',
	"appt_entr_poit_m" STRING COMMENT 'Entry Point Name',
	"ext_to_pdct_syst_d" DATE COMMENT 'Extract To Product System Date',
	"efft_d" DATE COMMENT 'Effective Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_dept" (
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
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_feat" (
	"appt_i" STRING NOT NULL COMMENT 'Application Identifier',
	"feat_i" STRING NOT NULL COMMENT 'Feature Identifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"srce_syst_c" STRING NOT NULL COMMENT 'Source System Code',
	"srce_syst_appt_feat_i" STRING COMMENT 'Source System Application Feature Identifier',
	"srce_syst_stnd_valu_q" NUMBER(15,6) COMMENT 'Source System Standard Value Quantity',
	"srce_syst_stnd_valu_r" NUMBER(18,6) COMMENT 'Source System Standard Value Rate',
	"srce_syst_stnd_valu_a" NUMBER(18,2) COMMENT 'Source System Standard Value Amount',
	"actl_valu_q" NUMBER(15,6) COMMENT 'Actual Value Quantity',
	"actl_valu_r" NUMBER(18,6) COMMENT 'Actual Value Rate',
	"actl_valu_a" NUMBER(18,2) COMMENT 'Actual Value Amount',
	"cncy_c" STRING COMMENT 'Currency Code',
	"ovrd_feat_i" STRING COMMENT 'Override Feature Identifier',
	"ovrd_reas_c" STRING COMMENT 'Override Reason Code',
	"feat_seqn_n" NUMBER(38,0) COMMENT 'Feature Sequence Number',
	"feat_strt_d" DATE COMMENT 'Feature Start Date',
	"fee_chrg_d" DATE COMMENT 'Fee Charged Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct" (
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
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_acct" (
	"appt_pdct_i" STRING NOT NULL COMMENT 'Application Product Identifier',
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"rel_type_c" STRING NOT NULL COMMENT 'Relation Type Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_amt" (
	"appt_pdct_i" STRING NOT NULL COMMENT 'Application Product Identifier',
	"amt_type_c" STRING NOT NULL COMMENT 'Amount Type Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"cncy_c" STRING COMMENT 'Currency Code',
	"appt_pdct_a" NUMBER(18,2) COMMENT 'Application Product Amount',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	"xces_amt_reas_x" STRING,
	"srce_syst_c" STRING,
	"appt_pdct_aud_eqal_a" NUMBER(18,2) COMMENT 'Application Product Australian Dollar Equivalent Amount',
	"cncy_conv_r" NUMBER(15,8) COMMENT 'Currency Conversion Rate',
	"disc_cncy_conv_r" NUMBER(15,8) COMMENT 'Discounted Currency Conversion Rate',
	"disc_cncy_deal_autn_n" NUMBER(38,0) COMMENT 'Discounted Currency Dealer Authorisation number',
	"srce_syst_appt_pdct_amt_i" STRING COMMENT 'Source System Application Identifier',
	"payt_meth_type_c" STRING COMMENT 'Payment Method Type Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_feat" (
	"appt_pdct_i" STRING NOT NULL COMMENT 'Application Product Identifier',
	"feat_i" STRING NOT NULL COMMENT 'Feature Identifier',
	"srce_syst_appt_feat_i" STRING NOT NULL COMMENT 'Source System Application Feature Identifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"srce_syst_c" STRING NOT NULL COMMENT 'Source System Code',
	"srce_syst_appt_ovrd_i" STRING COMMENT 'Source System Application Override Identifier',
	"ovrd_feat_i" STRING COMMENT 'Overridden Feature Identifier',
	"srce_syst_stnd_valu_q" NUMBER(15,6) COMMENT 'Source System Standard Value Quantity',
	"srce_syst_stnd_valu_r" NUMBER(18,6) COMMENT 'SourceSystem Standard Value Rate',
	"srce_syst_stnd_valu_a" NUMBER(18,2) COMMENT 'Source System Standard Value Amount',
	"cncy_c" STRING COMMENT 'Currency Code',
	"actl_valu_q" NUMBER(15,6) COMMENT ' Actual Value Quantity',
	"actl_valu_r" NUMBER(18,6) COMMENT 'Actual Value Rate',
	"actl_valu_a" NUMBER(18,2) COMMENT 'Actual Value Amount',
	"feat_seqn_n" NUMBER(38,0) COMMENT 'Feature Sequence Number',
	"feat_strt_d" DATE COMMENT 'Feature Start Date',
	"fee_chrg_d" DATE COMMENT 'Fee Charge Date',
	"ovrd_reas_c" STRING COMMENT 'Override Reason Code',
	"fee_add_to_totl_f" STRING COMMENT 'Fee Added To Total Flag',
	"fee_capl_f" STRING COMMENT 'Fee Capitalisation Flag',
	"expy_d" DATE NOT NULL COMMENT 'Expiry date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	"feat_valu_c" STRING COMMENT 'Feature Value Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_paty" (
	"appt_pdct_i" STRING NOT NULL COMMENT 'Application Product Identifier',
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"paty_role_c" STRING NOT NULL COMMENT 'Party Role Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"srce_syst_c" STRING NOT NULL COMMENT 'Source System Code',
	"srce_syst_appt_pdct_paty_i" STRING NOT NULL COMMENT 'Source System Application Product Party Identifier',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_purp" (
	"appt_pdct_i" STRING NOT NULL COMMENT 'Application Product Identifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"srce_syst_appt_pdct_purp_i" STRING NOT NULL COMMENT 'Source System Application Product Purpose Identifier',
	"purp_type_c" STRING NOT NULL COMMENT 'Purpose Type Code',
	"purp_clas_c" STRING COMMENT 'Purpose Class Code',
	"srce_syst_c" STRING NOT NULL COMMENT 'Source System Code',
	"purp_a" NUMBER(18,2) COMMENT 'Purpose Amount',
	"cncy_c" STRING COMMENT 'Currency Code',
	"main_purp_f" STRING COMMENT 'Main Purpose Flag',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_rel" (
	"appt_pdct_i" STRING NOT NULL COMMENT 'Application Product Identifier',
	"reld_appt_pdct_i" STRING NOT NULL COMMENT 'Related Application Product Identifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"rel_type_c" STRING NOT NULL COMMENT 'Relation Type Code',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_rpay" (
	"appt_pdct_i" STRING NOT NULL COMMENT 'Application Product Identifier',
	"rpay_type_c" STRING NOT NULL COMMENT 'Repayment Type Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"payt_freq_c" STRING COMMENT 'Payment Frequency Code',
	"strt_rpay_d" DATE COMMENT 'Start Repayment Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	"srce_syst_c" STRING,
	"rpay_srce_c" STRING,
	"rpay_srce_othr_x" STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_pdct_unid_paty" (
	"appt_pdct_i" STRING NOT NULL COMMENT 'Application Product Identifier',
	"paty_role_c" STRING NOT NULL COMMENT 'Party Role Code',
	"srce_syst_paty_i" STRING NOT NULL COMMENT 'Source System Party Identifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"srce_syst_c" STRING NOT NULL COMMENT 'Source System Code',
	"unid_paty_catg_c" STRING COMMENT 'Unidentified Party Category Code',
	"paty_m" STRING COMMENT 'Party Name',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_rel" (
	"appt_i" STRING NOT NULL COMMENT 'Application Identifier',
	"reld_appt_i" STRING NOT NULL COMMENT 'Related Application Identifier',
	"rel_type_c" STRING NOT NULL COMMENT 'Relationship Type Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."appt_trnf_detl" (
	"appt_i" STRING NOT NULL COMMENT 'Application Identifier',
	"appt_trnf_i" STRING NOT NULL COMMENT 'Application Transfer Identifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"trnf_optn_c" STRING COMMENT 'Transfer Option Code',
	"trnf_a" NUMBER(18,2) COMMENT 'Transfer Amount',
	"cncy_c" STRING COMMENT 'Currency Code',
	"cmpe_i" STRING COMMENT 'Competitor Identifier',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."busn_evnt" (
	"evnt_i" STRING NOT NULL COMMENT 'EVENT IDENTIFIER',
	"srce_syst_evnt_i" STRING COMMENT 'SOURCE SYSTEM EVENT IDENTIFIER',
	"evnt_actl_d" DATE COMMENT 'EVENT ACTUAL DATE',
	"srce_syst_c" STRING NOT NULL COMMENT 'SOURCE SYSTEM CODE',
	"pros_key_efft_i" NUMBER(10,0) COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'ERROR SEQUENCE IDENTIFIER',
	"srce_syst_evnt_type_i" STRING COMMENT 'SOURCE SYSTEM EVENT TYPE IDENTIFIER',
	"evnt_actl_t" TIME(6),
	"row_secu_accs_c" NUMBER(38,0) NOT NULL,
	"evnt_actv_type_c" STRING COMMENT 'Event Activity Type Code',
	"efft_d" DATE COMMENT 'Effective Date',
	"expy_d" DATE COMMENT 'Expiry Date',
	"record_deleted_flag" NUMBER(38,0) NOT NULL COMMENT 'Record Deleted Flag',
	"ctl_id" NUMBER(38,0) NOT NULL COMMENT 'Ctl Id',
	"process_name" STRING NOT NULL COMMENT 'Process Name',
	"process_id" NUMBER(38,0) NOT NULL COMMENT 'Process Id',
	"update_process_name" STRING COMMENT 'Update Process Name',
	"update_process_id" NUMBER(38,0) COMMENT 'Update Process Id'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."cls_fcly" (
	"acct_i" STRING NOT NULL COMMENT 'ACCOUNT IDENTIFIER',
	"libl_n" STRING COMMENT 'LIABILITY NUMBER',
	"srce_syst_acct_i" STRING COMMENT 'SOURCE SYSTEM ACCOUNT IDENTIFIER',
	"srce_syst_paty_i" STRING COMMENT 'SOURCE SYSTEM PARTY IDENTIFIER',
	"cif_acct_i" STRING COMMENT 'CLIENT INFORMATION FACILITY ACCOUNT IDENTIFIER',
	"fcly_type_c" STRING COMMENT 'FACILITY TYPE CODE',
	"pdct_n" NUMBER(38,0) COMMENT 'PRODUCT NUMBER',
	"cncy_c" STRING COMMENT 'CURRENCY CODE',
	"fcly_a" NUMBER(18,2) COMMENT 'FACILITY AMOUNT',
	"strt_d" DATE COMMENT 'START DATE',
	"cncl_d" DATE COMMENT 'CANCELLATION DATE',
	"fee_in_advn_f" STRING COMMENT 'FEE IN ADVANCE FLAG',
	"fee_scal_f" STRING COMMENT 'FEE SCALE FLAG',
	"cfrm_fee_f" STRING COMMENT 'CONFIRMATION FEE INDICATOR',
	"min_fee_chrg_a" NUMBER(18,2) COMMENT 'MINIMUM FEE CHARGE AMOUNT',
	"max_fee_chrg_a" NUMBER(18,2) COMMENT 'MAXIMUM FEE CHARGE AMOUNT',
	"yrly_chrg_a" NUMBER(18,2) COMMENT 'YEARLY CHARGE AMOUNT',
	"int_1_r" NUMBER(15,6) COMMENT 'INTEREST RATE 1',
	"int_2_r" NUMBER(15,6) COMMENT 'INTEREST RATE 2',
	"irte_2_limt_a" NUMBER(18,2) COMMENT 'INTEREST RATE 2 LIMIT AMOUNT',
	"pyat_freq_c" STRING COMMENT 'PAYMENT FREQUENCY CODE',
	"last_payt_d" DATE COMMENT 'LAST PAYMENT DATE',
	"next_payt_d" DATE COMMENT 'NEXT PAYMENT DATE',
	"payt_fi_i" STRING COMMENT 'PAYMENT FINANCIAL INSTITUTION IDENTIFIER',
	"payt_fi_c" STRING COMMENT 'PAYMENT FINANCIAL INSTITUTION CODE',
	"payt_fi_brch_n" STRING COMMENT 'PAYMENT FINANCIAL INSTITUTION BRANCH  NUMBER',
	"payt_fi_acct_n" STRING COMMENT 'PAYMENT FINANCIAL INSTITUTION  ACCOUNT NUMBER',
	"fee_due_a" NUMBER(18,2) COMMENT 'FEE DUE AMOUNT',
	"fee_acrl_a" NUMBER(18,2) COMMENT 'FEE ACCRUAL AMOUNT',
	"cash_covr_f" STRING COMMENT 'CASH COVERED FLAG',
	"cash_covr_acct_n" STRING COMMENT 'CASH COVER ACCOUNT NUMBER',
	"efft_d" DATE NOT NULL COMMENT 'EFFECTIVE  DATE',
	"expy_d" DATE NOT NULL COMMENT 'EXPIRY DATE',
	"pros_key_efft_i" NUMBER(10,0) COMMENT 'PROCESS KEY EFFECTIVE  IDENTIFIER',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."cls_unid_paty" (
	"srce_syst_paty_i" STRING NOT NULL COMMENT 'SOURCE SYSTEM PARTY IDENTIFIER',
	"srce_syst_c" STRING NOT NULL COMMENT 'SOURCE SYSTEM CODE',
	"paty_m" STRING COMMENT 'PARTY NAME',
	"cris_debt_i" STRING COMMENT 'CUSTOMER RELATIONSHIP INFORMATION SYSTEM DEBTOR IDENTIFIER',
	"efft_d" DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	"expy_d" DATE NOT NULL COMMENT 'EXPIRY DATE',
	"pros_key_efft_i" NUMBER(10,0) COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."dar_acct" (
	"merc_acct_i" STRING NOT NULL COMMENT 'Merchant Identifier',
	"srce_syst_merc_acct_i" STRING COMMENT 'Source System Merchant Account Identifier',
	"srce_syst_merc_grup_i" STRING COMMENT 'Source System Merchant Group Identifier',
	"merc_trad_m" STRING COMMENT 'Merchant Trading Name',
	"acct_i" STRING COMMENT 'Account Identifier',
	"acct_x" STRING COMMENT 'Account Text',
	"pdct_n" NUMBER(38,0) COMMENT 'Product Number',
	"dar_stus_c" STRING COMMENT 'DAR Status Code',
	"open_d" DATE COMMENT 'Open Date',
	"clse_d" DATE COMMENT 'Closed                            Date',
	"ovrd_cris_x" STRING COMMENT 'Override Cris Text',
	"dar_merc_catg_c" STRING COMMENT 'Dare Merchant Category code',
	"merc_clas_x" STRING COMMENT 'Merchant Class Text',
	"brch_n" STRING COMMENT 'Branch Number',
	"serv_reps_x" STRING COMMENT 'Service Representative Text',
	"mrkt_reps_x" STRING COMMENT 'Marketing RepresentativeText',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."dept_appt" (
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
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_acct_paty" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"assc_acct_i" STRING NOT NULL COMMENT 'Associated Account Identifier',
	"paty_acct_rel_c" STRING NOT NULL COMMENT 'Party Account Relationship Code',
	"prfr_paty_f" STRING COMMENT 'Preferred Party Flag',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(38,0) NOT NULL COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	"pros_key_expy_i" NUMBER(38,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_prtf_acct_rel" (
	"acct_i" STRING NOT NULL COMMENT 'Account Identifier',
	"int_grup_i" STRING NOT NULL COMMENT 'Interest Group Identifier',
	"derv_prtf_catg_c" STRING COMMENT 'Derived Portfolio Category Code',
	"derv_prtf_clas_c" STRING COMMENT 'Derived Portfolio Class Code',
	"derv_prtf_type_c" STRING COMMENT 'Derived Portfolio Type Code',
	"vald_from_d" DATE COMMENT 'Valid From Date',
	"vald_to_d" DATE COMMENT 'Valid To Date',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"ptcl_n" NUMBER(38,0) COMMENT 'Point Of Control Number',
	"rel_mnge_i" STRING COMMENT 'Relationship Manager Identifier',
	"prtf_code_x" STRING COMMENT 'Portfolio Code Description',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'Row Secuirty Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_prtf_own_rel" (
	"int_grup_i" STRING NOT NULL COMMENT 'Interest Group Identifier',
	"derv_prtf_type_c" STRING COMMENT 'Derived Portfolio Type Code',
	"derv_prtf_catg_c" STRING COMMENT 'Derived Portfolio Category Code',
	"derv_prtf_clas_c" STRING COMMENT 'Derived Portfolio Class Code',
	"vald_from_d" DATE COMMENT 'Valid From Date',
	"vald_to_d" DATE COMMENT 'Valid To Date',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"ptcl_n" NUMBER(38,0) COMMENT 'Point Of Control Number',
	"rel_mnge_i" STRING COMMENT 'Relationship Manager Identifier',
	"prtf_code_x" STRING COMMENT 'Portfolio Code Description',
	"derv_prtf_role_c" STRING COMMENT 'Derived Portfolio Role Code',
	"role_play_type_x" STRING COMMENT 'Role Player Type Description',
	"role_play_i" STRING NOT NULL COMMENT 'Role Player Identifier',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."derv_prtf_paty_rel" (
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"int_grup_i" STRING NOT NULL COMMENT 'Interest Group Identifier',
	"derv_prtf_catg_c" STRING COMMENT 'Derived Portfolio Category Code',
	"derv_prtf_clas_c" STRING COMMENT 'Derived Portfolio Class Code',
	"derv_prtf_type_c" STRING COMMENT 'Derived Portfolio Type Code',
	"vald_from_d" DATE COMMENT 'Valid From Date',
	"vald_to_d" DATE COMMENT 'Valid To Date',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"ptcl_n" NUMBER(38,0) COMMENT 'Point Of Control Number',
	"rel_mnge_i" STRING COMMENT 'Relationship Manager Identifier',
	"prtf_code_x" STRING COMMENT 'Portfolio Code Description',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."evnt" (
	"evnt_i" STRING NOT NULL COMMENT 'Event Identifier',
	"evnt_actv_type_c" STRING COMMENT 'Event Activity Type Code',
	"invt_evnt_f" STRING NOT NULL COMMENT 'Investment Event Indicator',
	"fncl_acct_evnt_f" STRING NOT NULL COMMENT 'Financial Account Event Indicator',
	"ctct_evnt_f" STRING NOT NULL COMMENT 'Contact Event Indicator',
	"busn_evnt_f" STRING NOT NULL COMMENT 'Business Event Indicator',
	"pros_key_efft_i" NUMBER(10,0) COMMENT 'Process Key Effective Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	"fncl_nval_evnt_f" STRING NOT NULL COMMENT 'Financial Non Value Event Indicator',
	"incd_f" STRING NOT NULL COMMENT 'Incident Indicator',
	"insr_evnt_f" STRING NOT NULL COMMENT 'Insurance Event Indicator',
	"insr_nval_evnt_f" STRING NOT NULL COMMENT 'Insurance Non Value Event Indicator',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code',
	"fncl_gl_evnt_f" STRING NOT NULL COMMENT 'Financial General Ledger Event Indicator',
	"autt_autn_evnt_f" STRING NOT NULL COMMENT 'Authentication Authorisation Event Indicator',
	"coll_evnt_f" STRING NOT NULL COMMENT 'Collection Event Indicator',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"evnt_reas_c" STRING COMMENT 'Event Reason Code',
	"efft_d" DATE COMMENT 'Effective Date',
	"expy_d" DATE COMMENT 'Expiry Date',
	"record_deleted_flag" NUMBER(38,0) NOT NULL COMMENT 'Record Deleted Flag',
	"ctl_id" NUMBER(38,0) NOT NULL COMMENT 'Ctl Id',
	"process_name" STRING NOT NULL COMMENT 'Process Name',
	"process_id" NUMBER(38,0) NOT NULL COMMENT 'Process Id',
	"update_process_name" STRING COMMENT 'Update Process Name',
	"update_process_id" NUMBER(38,0) COMMENT 'Update Process Id'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."evnt_empl" (
	"evnt_i" STRING NOT NULL COMMENT 'EVENT IDENTIFIER',
	"empl_i" STRING NOT NULL COMMENT 'EMPLOYEE IDENTIFIER',
	"evnt_paty_role_type_c" STRING NOT NULL COMMENT 'EVENT PARTY ROLE TYPE CODE',
	"efft_d" DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	"expy_d" DATE NOT NULL COMMENT 'EXPIRY DATE',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'ERROR SEQUENCE IDENTIFIER',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."evnt_int_grup" (
	"evnt_i" STRING NOT NULL COMMENT 'Event Identifier',
	"int_grup_i" STRING NOT NULL COMMENT 'Interest Group Identifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."gdw_efft_date" (
	"gdw_efft_d" DATE NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup" (
	"int_grup_i" STRING NOT NULL COMMENT 'Interest Group Identifier',
	"int_grup_type_c" STRING NOT NULL COMMENT 'Event Group Type Code',
	"srce_syst_int_grup_i" STRING NOT NULL COMMENT 'Source System Interest Group Identifier',
	"srce_syst_c" STRING NOT NULL COMMENT 'Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	"int_grup_m" STRING COMMENT 'INTEREST GROUP NAME',
	"orig_srce_syst_int_grup_i" STRING COMMENT 'ORIGINAL SOURCE SYSTEM INTEREST GROUP IDENTIFIER',
	"crat_d" DATE COMMENT 'CREATED DATE',
	"qlfy_c" STRING COMMENT 'Qualifier Code',
	"ptcl_n" STRING COMMENT 'Point Of Control Number',
	"rel_mnge_i" STRING COMMENT 'Relationship Manager Identifier',
	"vald_to_d" DATE COMMENT 'Valid To Date',
	"row_secu_accs_c" NUMBER(38,0) COMMENT 'Row Security Access Code',
	"int_grup_catg_c" STRING COMMENT 'Interest Group Category Code',
	"iso_cnty_c" STRING COMMENT 'ISO Country Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup_dept" (
	"int_grup_i" STRING NOT NULL COMMENT 'Interest Group Identifier',
	"dept_i" STRING NOT NULL COMMENT 'Department Identifier',
	"dept_role_c" STRING NOT NULL COMMENT 'Department Role Code',
	"srce_syst_c" STRING COMMENT 'Source System Code',
	"vald_from_d" DATE COMMENT 'Valid From Date',
	"vald_to_d" DATE COMMENT 'Valid To Date',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"row_secu_accs_c" NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup_empl" (
	"int_grup_i" STRING NOT NULL COMMENT 'Interest Group Identifier',
	"empl_i" STRING NOT NULL COMMENT 'Employee Identifier',
	"empl_role_c" STRING NOT NULL COMMENT 'Employee Role Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier',
	"rel_c" STRING COMMENT 'Relationship Code',
	"vald_from_d" DATE COMMENT 'Valid From Date',
	"vald_to_d" DATE COMMENT 'Valid To Date',
	"row_secu_accs_c" NUMBER(38,0) COMMENT 'Row Security Access Code',
	"srce_syst_c" STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."int_grup_unid_paty" (
	"int_grup_i" STRING NOT NULL COMMENT 'Interest Group Identifier',
	"srce_syst_paty_i" STRING NOT NULL COMMENT 'Source System Party Identifier',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"orig_srce_syst_paty_i" STRING COMMENT 'Original Source System Party Identifier',
	"srce_syst_c" STRING NOT NULL COMMENT 'Source System Code',
	"unid_paty_m" STRING COMMENT 'Unidentified Party Name',
	"paty_type_c" STRING NOT NULL COMMENT 'Party Type Code',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Process Key Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_adrs_type" (
	"adrs_type_id" STRING,
	"pyad_type_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_acqr_srce" (
	"pl_mrkt_srce_cat_id" STRING,
	"acqr_srce_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_c" (
	"ccl_app_cat_id" STRING,
	"appt_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_cmpe" (
	"bal_xfer_insn_cat_id" STRING,
	"cmpe_i" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_code_hm" (
	"hlm_appt_type_catg_i" STRING NOT NULL COMMENT 'Hlm Application Type Category Identifier',
	"appt_c" STRING COMMENT 'Application Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_cond" (
	"cond_appt_cat_id" STRING,
	"appt_cond_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_docu_dely" (
	"exec_docu_recv_type" STRING NOT NULL COMMENT 'Executive Documents Receiver Type',
	"docu_dely_recv_c" STRING COMMENT 'Document Delivery Receiver Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_feat" (
	"map_type_c" STRING NOT NULL,
	"targ_numc_c" NUMBER(38,0),
	"targ_char_c" STRING,
	"srce_numc_1_c" NUMBER(38,0),
	"srce_char_1_c" STRING,
	"srce_numc_2_c" NUMBER(38,0),
	"srce_char_2_c" STRING,
	"srce_numc_3_c" NUMBER(38,0),
	"srce_char_3_c" STRING,
	"srce_numc_4_c" NUMBER(38,0),
	"srce_char_4_c" STRING,
	"srce_numc_5_c" NUMBER(38,0),
	"srce_char_5_c" STRING,
	"srce_numc_6_c" NUMBER(38,0),
	"srce_char_6_c" STRING,
	"srce_numc_7_c" NUMBER(38,0),
	"srce_char_7_c" STRING,
	"efft_d" DATE NOT NULL,
	"expy_d" DATE NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_form" (
	"ccl_form_cat_id" STRING,
	"appt_form_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_orig" (
	"chnl_cat_id" STRING,
	"appt_orig_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_pdct_feat" (
	"pexa_flag" STRING NOT NULL COMMENT 'Pexa Flag',
	"feat_valu_c" STRING COMMENT 'Feature Value Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_pdct_paty_role" (
	"role_cat_id" STRING,
	"paty_role_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_cl" (
	"ccl_loan_purp_cat_id" STRING,
	"purp_type_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_clas_cl" (
	"loan_purp_clas_code" STRING,
	"purp_clas_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_hl" (
	"hl_loan_purp_cat_id" STRING,
	"purp_type_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_purp_pl" (
	"pl_prod_purp_cat_id" STRING,
	"purp_type_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_qlfy" (
	"sbty_code" STRING,
	"appt_qlfy_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_qstn_hl" (
	"qa_question_id" STRING NOT NULL,
	"qstn_c" STRING,
	"efft_d" DATE NOT NULL,
	"expy_d" DATE NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_appt_qstn_resp_hl" (
	"qa_answer_id" STRING NOT NULL,
	"resp_c" STRING,
	"efft_d" DATE NOT NULL,
	"expy_d" DATE NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_cmpe_idnn" (
	"insn_id" STRING,
	"cmpe_i" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_cnty" (
	"docu_coll_cnty_id" STRING,
	"iso_cnty_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_cris_pdct" (
	"cris_pdct_id" STRING NOT NULL COMMENT 'Customer Relationship Information SYSTEM Product Identifier',
	"acct_qlfy_c" STRING COMMENT 'ACCOUNT Qualifier Code',
	"srce_syst_c" STRING COMMENT 'SOURCE SYSTEM Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective DATE',
	"expy_d" DATE NOT NULL COMMENT 'Expiry DATE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_docu_meth" (
	"docu_coll_cat_id" STRING,
	"docu_dely_meth_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_chld_paty_rel" (
	"fa_chld_stat_cat_id" STRING,
	"rel_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_evnt_actv_type" (
	"fa_env_evnt_cat_id" STRING,
	"evnt_actv_type_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_paty_rel" (
	"clnt_reln_type_id" STRING,
	"clnt_posn" STRING,
	"rel_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_env_paty_type" (
	"fa_enty_cat_id" STRING,
	"paty_type_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_feat_ovrd_reas_hl" (
	"hl_prod_int_marg_cat_id" STRING,
	"ovrd_reas_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_feat_ovrd_reas_hl_d" (
	"hl_fee_discount_cat_id" STRING,
	"ovrd_reas_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_feat_ovrd_reas_pl" (
	"marg_reas_cat_id" STRING,
	"ovrd_reas_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_fee_capl" (
	"pl_capl_fee_cat_id" STRING,
	"fee_capl_f" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_fndd_meth" (
	"fndd_meth_cat_id" STRING,
	"fndd_inss_meth_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_job_comm_catg" (
	"clp_job_family_cat_id" STRING,
	"job_comm_catg_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_loan_fndd_meth" (
	"pl_targ_cat_id" STRING,
	"loan_fndd_meth_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_loan_term_pl" (
	"pl_prod_term_cat_id" NUMBER(38,0),
	"actl_valu_q" NUMBER(15,6),
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_lpc_dept_hl" (
	"lpc_office" STRING,
	"dept_i" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_orig_appt_srce" (
	"orig_srce_syst_i" STRING NOT NULL COMMENT 'Original Source System Identifier',
	"orig_appt_srce_c" STRING COMMENT 'Original Application Source Code',
	"efft_d" DATE COMMENT 'Effective Date',
	"expy_d" DATE COMMENT 'Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_orig_appt_srce_hm" (
	"hl_busn_chnl_cat_i" NUMBER(38,0) NOT NULL COMMENT 'Homeloan Business Channel Cat Id',
	"orig_appt_srce_syst_c" STRING COMMENT 'Original Application Source System Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_ovrd_fee_frq_cl" (
	"ovrd_fee_pct_freq" STRING,
	"freq_in_mths" NUMBER(15,6),
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pack_pdct_hl" (
	"hl_pack_cat_id" STRING,
	"pdct_n" NUMBER(38,0),
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pack_pdct_pl" (
	"pl_pack_cat_id" STRING,
	"pdct_n" NUMBER(38,0),
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_payt_freq" (
	"hl_rpay_perd_cat_id" STRING,
	"payt_freq_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pdct_rel_cl" (
	"parn_pdct_lvl_cat_id" STRING,
	"chld_pdct_lvl_cat_id" STRING,
	"rel_type_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_pl_acqr_type" (
	"pl_cmpn_cat_id" STRING,
	"acqr_type_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_sm_case_stus" (
	"sm_stat_cat_id" STRING,
	"stus_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_sm_case_stus_reas" (
	"sm_reas_cat_id" STRING,
	"stus_reas_type_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_state" (
	"state_id" STRING,
	"stat_x" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_trnf_optn" (
	"bal_xfer_optn_cat_id" STRING,
	"trnf_optn_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_tu_appt_c" (
	"sbty_code" STRING,
	"appt_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."map_cse_unid_paty_catg_pl" (
	"tp_brok_grup_cat_id" STRING,
	"unid_paty_catg_c" STRING,
	"efft_d" DATE,
	"expy_d" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."mos_fcly" (
	"fcly_i" STRING NOT NULL COMMENT 'FACILITY IDENTIFIER',
	"srce_syst_paty_i" STRING COMMENT 'SOURCE SYSTEM PARTY IDENTIFIER',
	"brch_c" STRING COMMENT 'BRANCH CODE',
	"appt_c" STRING COMMENT 'APPLICATION CODE',
	"crat_d" DATE COMMENT 'CREATION DATE',
	"aset_libl_c" STRING COMMENT 'ASSET LIABILITY CODE',
	"stus_c" STRING COMMENT 'STATUS CODE',
	"cncy_c" STRING COMMENT 'CURRENCY CODE',
	"orig_a" NUMBER(18,2) COMMENT 'ORIGINAL AMOUNT',
	"curr_baln_a" NUMBER(18,2) COMMENT 'CURRENT BALANCE AMOUNT',
	"issu_d" DATE COMMENT 'ISSUE DATE',
	"mtur_d" DATE COMMENT 'MATURITY DATE',
	"orig_issu_d" DATE COMMENT 'ORIGINAL ISSUE      DATE',
	"efft_d" DATE COMMENT 'EFFECTIVE DATE',
	"expy_d" DATE COMMENT 'EXPIRY DATE',
	"pros_key_efft_i" NUMBER(10,0) COMMENT 'PROCESS KEY EFFECTIVE      IDENTIFIER',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'PROCESS KEY EXPIRY      IDENTIFIER'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."mos_loan" (
	"loan_i" STRING NOT NULL COMMENT 'LOAN IDENTIFIER',
	"fcly_i" STRING COMMENT 'FACILITY IDENTIFIER',
	"mos_appt_c" STRING COMMENT 'MOS APPLICATION CODE',
	"mos_aset_libl_c" STRING COMMENT 'MOS ASET LIABILITY CODE',
	"mos_stus_c" STRING COMMENT 'MOS STATUS CODE',
	"acrl_x" STRING COMMENT 'MIDAS OFFSHORE ACCRUAL DESCRIPTION',
	"trad_cncy_orig_a" NUMBER(18,2) COMMENT 'TRADING CURRENCY ORIGINAL AMOUNT',
	"trad_cncy_setl_a" NUMBER(18,2) COMMENT 'TRADING CURRENCY SETTLEMENTAMOUNT',
	"trad_cncy_curr_baln_a" NUMBER(18,2) COMMENT 'TRADING CURRENCY CURRENTBALANCE AMOUNT',
	"int_r" NUMBER(18,8) COMMENT 'INTEREST RATE',
	"fix_int_r" NUMBER(18,8) COMMENT 'FIXED INTEREST RATE',
	"vary_int_r" NUMBER(18,8) COMMENT 'VARIABLE INTEREST RATE',
	"issu_d" DATE COMMENT 'ISSUE DATE',
	"mtur_d" DATE COMMENT 'MATURITY DATE',
	"rolv_d" DATE COMMENT 'ROLLOVER DATE',
	"int_payt_freq_c" STRING COMMENT 'PAYMENT FREQUENCY CODE',
	"next_int_d" DATE COMMENT 'NEXT INTEREST DATE',
	"trad_cncy_acre_a" NUMBER(18,2) COMMENT 'TRADING CURRENCY ACCRUED AMOUNT',
	"last_int_payt_d" DATE COMMENT 'LAST INTEREST PAYMENTDATE',
	"trad_cncy_totl_int_recv_a" NUMBER(18,2) COMMENT 'TRADING CURRENCY TOTALINTEREST RECEIVED AMOUNT',
	"pdct_n" NUMBER(38,0) COMMENT 'PRODUCT NUMBER',
	"trad_cncy_c" STRING COMMENT 'CURRENCY CODE',
	"base_cncy_c" STRING COMMENT 'CURRENCY CODE',
	"trad_cncy_int_a" NUMBER(18,2) COMMENT 'TRADING CURRENCY INTEREST AMOUNT',
	"aud_cncy_int_a" NUMBER(18,2) COMMENT 'AUSTRALIAN DOLLAR CURRENCYINTEREST AMOUNT',
	"base_cncy_int_a" NUMBER(18,2) COMMENT 'BASE CURRENCY INTEREST AMOUNT',
	"trad_cncy_avrg_baln_a" NUMBER(18,2) COMMENT 'TRADING CURRENCY AVERAGEBALANCE AMOUNT',
	"aud_cncy_avrg_baln_a" NUMBER(18,2) COMMENT 'AUSTRALIAN DOLLAR CURRENCYAVERAGE BALANCE AMOUNT',
	"base_cncy_avrg_baln_a" NUMBER(18,2) COMMENT 'BASE CURRENCY AVERAGE BALANCEAMOUNT',
	"trad_cncy_marg_a" NUMBER(18,2) COMMENT 'TRADING CURRENCY MARGIN AMOUNT',
	"aud_cncy_marg_a" NUMBER(18,2) COMMENT 'AUSTRALIAN DOLLAR CURRENCY MARGINAMOUNT',
	"base_cncy_marg_a" NUMBER(18,2) COMMENT 'BASE CURRENCY MARGIN AMOUNT',
	"avrg_marg_r" NUMBER(18,8) COMMENT 'AVERAGE MARGIN RATE',
	"dept_i" STRING COMMENT 'DEPARTMENT IDENTIFIER',
	"msa_baln_gl_acct_i" STRING COMMENT 'MSA BALANCE GENERAL LEDGERACCOUNT IDENTIFIER',
	"msa_int_gl_acct_i" STRING COMMENT 'MSA INTEREST GENERAL LEDGERACCOUNT IDENTIFIER',
	"efft_d" DATE NOT NULL COMMENT 'EFFECTIVEDATE',
	"expy_d" DATE NOT NULL COMMENT 'EXPIRY DATE',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'PROCESS KEY EFFECTIVEIDENTIFIER',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER',
	"prof_cntr_code_x" STRING COMMENT 'Profit Centre Code Description'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."paty_appt_pdct" (
	"paty_i" STRING NOT NULL COMMENT 'Party Identifier',
	"appt_pdct_i" STRING NOT NULL COMMENT 'Application Product Identifier',
	"paty_role_c" STRING NOT NULL COMMENT 'Party Role Code',
	"efft_d" DATE NOT NULL COMMENT 'Effective Date',
	"srce_syst_c" STRING NOT NULL COMMENT 'Source System Code',
	"srce_syst_appt_pdct_paty_i" STRING NOT NULL COMMENT 'Source System Application Product Party Identifier',
	"expy_d" DATE NOT NULL COMMENT 'Expiry Date',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'Source System Expiry Identifier',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."paty_int_grup" (
	"int_grup_i" STRING NOT NULL COMMENT 'INTEREST GROUP IDENTIFIER',
	"paty_i" STRING NOT NULL COMMENT 'PARTY IDENTIFIER',
	"efft_d" DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	"expy_d" DATE NOT NULL COMMENT 'EXPIRY DATE',
	"srce_syst_c" STRING NOT NULL COMMENT 'SOURCE SYSTEM CODE',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER',
	"eror_seqn_i" NUMBER(10,0) COMMENT 'ERROR SEQUENCE IDENTIFIER',
	"prim_clnt_f" STRING COMMENT 'PRIMARY CLIENT FLAG',
	"rel_i" STRING COMMENT 'RELATIONSHIP IDENTIFIER',
	"srce_syst_paty_int_grup_i" STRING COMMENT 'SOURCE SYSTEM PARTY INTEREST GROUP IDENTIFIER',
	"orig_srce_syst_paty_type_c" STRING COMMENT 'ORIGINAL SOURCE SYSTEM PARTY TYPE CODE',
	"orig_srce_syst_paty_i" STRING COMMENT 'ORIGINAL SOURCE SYSTEM PARTY IDENTIFIER',
	"rel_c" STRING COMMENT 'RELATIONSHIP CODE',
	"vald_from_d" DATE COMMENT 'VALID FROM DATE',
	"vald_to_d" DATE COMMENT 'VALID TO DATE',
	"row_secu_accs_c" NUMBER(38,0) COMMENT 'ROW SECURITY ACCESS CODE',
	"prim_clnt_slct_c" STRING COMMENT 'PRIMARY CLIENT SELECTION CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."tha_acct" (
	"tha_acct_i" STRING NOT NULL COMMENT 'THALER ACCOUNT IDENTIFIER',
	"acct_qlfy_c" STRING COMMENT 'ACCOUNT QUALIFIER CODE',
	"ext_d" DATE COMMENT 'EXTRACT DATE',
	"csl_clnt_i" STRING COMMENT 'COMMSEC CLIENT IDENTIFIER',
	"trad_acct_i" STRING COMMENT 'TRADE ACCOUNT IDENTIFIER',
	"tha_acct_type_c" STRING COMMENT 'THALER ACCOUNT TYPE CODE',
	"tha_acct_stus_c" STRING COMMENT 'THALER ACCOUNT STATUS CODE',
	"pall_busn_unit_i" STRING COMMENT 'PRE ALLOCATION BUSINESS UNIT IDENTIFIER',
	"pall_dept_i" STRING COMMENT 'PRE ALLOCATION DEPARTMENT IDENTIFIER',
	"baln_a" NUMBER(15,2) COMMENT 'BALANCE AMOUNT',
	"iacr_mtd_a" NUMBER(15,2) COMMENT 'INTEREST ACCRUED MONTH TO DATE',
	"iacr_fytd_a" NUMBER(15,2) COMMENT 'INTEREST ACCRUED FINANCIAL YEAR TO DATE',
	"daly_aggr_fee_a" NUMBER(15,2) COMMENT 'DAILY AGGREGATE FEE AMOUNT',
	"baln_d" DATE COMMENT 'BALANCE DATE',
	"efft_d" DATE COMMENT 'EFFECTIVE DATE',
	"expy_d" DATE COMMENT 'EXPIRY DATE',
	"pros_key_efft_i" NUMBER(10,0) NOT NULL COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	"pros_key_expy_i" NUMBER(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_btch_isac" (
	"btch_key_i" NUMBER(10,0) NOT NULL,
	"btch_rqst_s" TIMESTAMP_NTZ(6),
	"btch_run_d" DATE NOT NULL,
	"srce_syst_m" STRING NOT NULL,
	"btch_stus_c" STRING,
	"stus_chng_s" TIMESTAMP_NTZ(6)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_eti_conv" (
	"conv_m" STRING NOT NULL,
	"appt_c" STRING,
	"devl_i" STRING,
	"logn_user_m" STRING,
	"conv_stus_c" STRING,
	"last_modf_d" DATE,
	"last_genr_d" DATE,
	"meta_stor_m" STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_parm" (
	"parm_m" STRING NOT NULL,
	"parm_ltrl_n" NUMBER(38,0),
	"parm_ltrl_d" DATE,
	"parm_ltrl_strg_x" STRING,
	"parm_ltrl_a" NUMBER(18,4)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."starcadproddata"."util_pros_isac" (
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
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."sysadmin"."tpumpstatustbl" (
	"logdb" STRING NOT NULL,
	"logtable" STRING NOT NULL,
	"import" NUMBER(38,0) NOT NULL,
	"username" STRING NOT NULL,
	"initstartdate" DATE,
	"initstarttime" double,
	"currstartdate" DATE,
	"currstarttime" double,
	"lastupdatedate" DATE,
	"lastupdatetime" double,
	"restartcount" NUMBER(38,0) NOT NULL,
	"complete" STRING,
	"stmtslast" NUMBER(38,0),
	"recordsout" NUMBER(38,0),
	"recordsskipped" NUMBER(38,0),
	"recordsrejcted" NUMBER(38,0),
	"recordsread" NUMBER(38,0),
	"recordserrored" NUMBER(38,0),
	"stmtsunlimited" STRING NOT NULL,
	"stmtsdesired" NUMBER(38,0),
	"sessndesired" NUMBER(38,0),
	"tasksdesired" NUMBER(38,0),
	"periodsdesired" NUMBER(38,0),
	"pleasepause" STRING,
	"pleaseabort" STRING,
	"requestchange" STRING NOT NULL,
	"requestaction" STRING NOT NULL,
	"logonsource" STRING,
	"loadpid" STRING,
	"numchildren" NUMBER(38,0),
	"childrenlist" STRING,
	"parentnode" STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table "NPD_D12_DMN_GDWMIG_IBRG"."syscalendar"."caldates" (
	"cdate" DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;