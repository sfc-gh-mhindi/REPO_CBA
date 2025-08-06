use role r_dev_npd_d12_gdwmig;
use database npd_d12_dmn_gdwmig;
use schema migration_tracking;
use warehouse wh_usr_npd_d12_gdwmig_001;

USE DATABASE NPD_D12_DMN_GDWMIG_IBRG;

CREATE SCHEMA IF NOT EXISTS PDCBODS;
CREATE SCHEMA IF NOT EXISTS PDCMS;
CREATE SCHEMA IF NOT EXISTS PDDSTG;
CREATE SCHEMA IF NOT EXISTS PDGRD;
CREATE SCHEMA IF NOT EXISTS PDPATY;
CREATE SCHEMA IF NOT EXISTS PDSECURITY;
CREATE SCHEMA IF NOT EXISTS PDSRCCS;
CREATE SCHEMA IF NOT EXISTS PDTRPC;
CREATE SCHEMA IF NOT EXISTS PUTIL;
CREATE SCHEMA IF NOT EXISTS STARCADPRODDATA;
CREATE SCHEMA IF NOT EXISTS Sys_Calendar;
CREATE SCHEMA IF NOT EXISTS SysAdmin;



use role r_dev_npd_d12_gdwmig;
use database npd_d12_dmn_gdwmig;
use schema migration_tracking;
use warehouse wh_usr_npd_d12_gdwmig_001;

create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDCBODS.ODS_RULE (
	RULE_CODE STRING NOT NULL COMMENT 'Rule Code',
	RULE_STEP_SEQN NUMBER(3,0) NOT NULL COMMENT 'Rule Step Sequence',
	PRTY NUMBER(3,0) NOT NULL COMMENT 'Priority',
	VALD_FROM DATE NOT NULL COMMENT 'Valid From Date',
	VALD_TO DATE NOT NULL COMMENT 'Valid To Date',
	RULE_DESN STRING COMMENT 'Rule Description',
	RULE_STEP_DESN STRING COMMENT 'Rule Step Description',
	LKUP1_TEXT STRING COMMENT 'Lookup1 Text',
	LKUP1_NUMB NUMBER(3,0) COMMENT 'Lookup1 Number',
	LKUP1_DATE DATE COMMENT 'Lookup1 Date',
	LKUP1_ADD_ATTR STRING COMMENT 'Lookup1 Additional Attribute',
	LKUP2_TEXT STRING COMMENT 'Lookup2 Text',
	LKUP2_NUMB NUMBER(3,0) COMMENT 'Lookup2 Number',
	LKUP2_DATE DATE COMMENT 'Lookup2 Date',
	LKUP2_ADD_ATTR STRING COMMENT 'Lookup2 Additional Attribute',
	RULE_CMMT STRING COMMENT 'Rule Comment',
	UPDT_DTTS TIMESTAMP_NTZ(6) COMMENT 'Update Timestamp',
	CRAT_DTTS TIMESTAMP_NTZ(6) COMMENT 'Create Timestamp'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;

create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDCMS.MAP_CMS_PDCT (
	CRIS_PDCT_CAT_ID STRING,
	CRIS_PDCT_C STRING,
	CRIS_DESC STRING,
	ACCT_I_PRFX STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_DEDUP (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	ASSC_ACCT_I STRING NOT NULL COMMENT 'Associated Account Identifier',
	PATY_ACCT_REL_C STRING NOT NULL COMMENT 'Party Account Relationship Code',
	/* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */ PRFR_PATY_F STRING COMMENT 'Preferred Party Flag',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_THA (
	THA_ACCT_I STRING NOT NULL COMMENT 'Thaler Account Identifier',
	TRAD_ACCT_I STRING NOT NULL COMMENT 'Trading Account Identifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_REL_WSS (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	ASSC_ACCT_I STRING NOT NULL COMMENT 'Associated Account Identifier',
	PATY_ACCT_REL_C STRING NOT NULL COMMENT 'Party Account Relationship Code',
	/* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */ PRFR_PATY_F STRING COMMENT 'Preferred Party Flag',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_PATY_THA_NEW_RNGE (
	THA_ACCT_I STRING NOT NULL COMMENT 'Thaler Account Identifier',
	TRAD_ACCT_I STRING NOT NULL COMMENT 'Trading Account Identifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	NEW_EXPY_D DATE COMMENT 'New Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.ACCT_REL_WSS_DITPS (
	ACCT_I STRING NOT NULL COMMENT 'ACCOUNT IDENTIFIER'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ADD (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	ASSC_ACCT_I STRING NOT NULL COMMENT 'Associated Account Identifier',
	PATY_ACCT_REL_C STRING NOT NULL COMMENT 'Party Account Relationship Code',
	/* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */ PRFR_PATY_F STRING COMMENT 'Preferred Party Flag',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CHG (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	ASSC_ACCT_I STRING NOT NULL COMMENT 'Associated Account Identifier',
	PATY_ACCT_REL_C STRING NOT NULL COMMENT 'Party Account Relationship Code',
	/* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */ PRFR_PATY_F STRING COMMENT 'Preferred Party Flag',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_CURR (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	ASSC_ACCT_I STRING NOT NULL COMMENT 'Associated Account Identifier',
	PATY_ACCT_REL_C STRING NOT NULL COMMENT 'Party Account Relationship Code',
	/* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */ PRFR_PATY_F STRING COMMENT 'Preferred Party Flag',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_DEL (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	ASSC_ACCT_I STRING NOT NULL COMMENT 'Associated Account Identifier',
	PATY_ACCT_REL_C STRING NOT NULL COMMENT 'Party Account Relationship Code',
	/* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */ PRFR_PATY_F STRING COMMENT 'Preferred Party Flag',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_FLAG (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	ASSC_ACCT_I STRING NOT NULL COMMENT 'Associated Account Identifier',
	PATY_ACCT_REL_C STRING NOT NULL COMMENT 'Party Account Relationship Code',
	/* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */ PRFR_PATY_F STRING COMMENT 'Preferred Party Flag',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_NON_RM (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	ASSC_ACCT_I STRING NOT NULL COMMENT 'Associated Account Identifier',
	PATY_ACCT_REL_C STRING NOT NULL COMMENT 'Party Account Relationship Code',
	/* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */ PRFR_PATY_F STRING COMMENT 'Preferred Party Flag',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE',
	RANK_I NUMBER(38,0) /* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_RM (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	ASSC_ACCT_I STRING NOT NULL COMMENT 'Associated Account Identifier',
	PATY_ACCT_REL_C STRING NOT NULL COMMENT 'Party Account Relationship Code',
	/* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */ PRFR_PATY_F STRING COMMENT 'Preferred Party Flag',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_ACCT_PATY_ROW_SECU_FIX (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	DERV_ACCT_PATY_ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'DERV ACCT_PATY ROW SECURITY ACCESS CODE',
	ACCT_PATY_ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ACCT_PATY ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_PSST (
	ACCT_I STRING COMMENT 'Account Identifier',
	PATY_I STRING COMMENT 'Party Identifier',
	ACCT_PRTF_C STRING COMMENT 'Account Portfolio Code',
	RANK_I NUMBER(38,0),
	/* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */ PATY_ACCT_REL_C STRING /* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_PATY_STAG (
	ACCT_I STRING COMMENT 'Account Identifier',
	PATY_I STRING COMMENT 'Party Identifier',
	ACCT_PRTF_C STRING COMMENT 'Account Portfolio Code',
	PATY_PRTF_C STRING COMMENT 'Party Portfolio Code',
	PATY_ACCT_REL_C STRING COMMENT 'Paty Account Relationship',
	/* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */ RANK_I NUMBER(38,0) COMMENT 'RANK' /* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_ACCT_STAG (
	ACCT_I STRING COMMENT 'Account Identifier',
	PRTF_CODE_X STRING COMMENT 'Derived Portfolio Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.DERV_PRTF_PATY_STAG (
	PATY_I STRING COMMENT 'Party Identifier',
	PRTF_CODE_X STRING COMMENT 'Derived Portfolio Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_BUSN_SEGM_PRTY (
	MAP_TYPE_C STRING NOT NULL COMMENT 'MAPPING TYPE CODE',
	BUSN_SEGM_C STRING COMMENT 'Business Segment Code',
	BUSN_SEGM_X STRING COMMENT 'Business Segment Description',
	BUSN_SEGM_PRTY SMALLINT COMMENT 'Business Segment Priority',
	EFFT_D DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	EXPY_D DATE NOT NULL COMMENT 'EXPIRY DATE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD (
	MAP_TYPE_C STRING NOT NULL COMMENT 'MAPPING TYPE CODE',
	PATY_ACCT_REL_C STRING COMMENT 'Account Party Relationship Code',
	PATY_ACCT_REL_X STRING COMMENT 'Account Party Relationship Code Description',
	EFFT_D DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	EXPY_D DATE NOT NULL COMMENT 'EXPIRY DATE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_PATY_REL (
	MAP_TYPE_C STRING NOT NULL COMMENT 'MAPPING TYPE CODE',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	REL_C STRING COMMENT 'Relationship Code',
	ACCT_I_C STRING COMMENT 'Account Code',
	EFFT_D DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	EXPY_D DATE NOT NULL COMMENT 'EXPIRY DATE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_DERV_UNID_PATY (
	MAP_TYPE_C STRING NOT NULL COMMENT 'MAPPING TYPE CODE',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	UNID_PATY_SRCE_SYST_C STRING COMMENT 'Unidentified Party Source System Code',
	UNID_PATY_ACCT_REL_C STRING COMMENT 'Unidentified Party Account Relationship Code',
	EFFT_D DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	EXPY_D DATE NOT NULL COMMENT 'EXPIRY DATE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDDSTG.GRD_GNRC_MAP_PATY_HOLD_PRTY (
	MAP_TYPE_C STRING NOT NULL COMMENT 'MAPPING TYPE CODE',
	PATY_ACCT_REL_C STRING COMMENT 'Account Party Relationship Code',
	PATY_ACCT_REL_X STRING COMMENT 'Account Party Relationship Code Description',
	PATY_ACCT_REL_PRTY SMALLINT COMMENT 'Account Party Relationship Code Priority',
	EFFT_D DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	EXPY_D DATE NOT NULL COMMENT 'EXPIRY DATE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDGRD.DEPT_DIMN_NODE_ANCS_CURR (
	DEPT_I STRING NOT NULL,
	ANCS_DEPT_I STRING,
	ANCS_LEVL_N NUMBER(3,0),
	AS_AT_D DATE NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDGRD.GRD_DEPT_FLAT_CURR (
	DEPT_L1_NODE_C STRING,
	DEPT_L1_NODE_M STRING,
	DEPT_L1_LEVL_N NUMBER(3,0),
	DEPT_L1_DISP_SEQN_N NUMBER(38,0),
	DEPT_L1_REPT_F STRING,
	DEPT_L1_RPRT_TYPE_C STRING,
	DEPT_L2_NODE_C STRING,
	DEPT_L2_NODE_M STRING,
	DEPT_L2_LEVL_N NUMBER(3,0),
	DEPT_L2_DISP_SEQN_N NUMBER(38,0),
	DEPT_L2_REPT_F STRING,
	DEPT_L2_RPRT_TYPE_C STRING,
	DEPT_L3_NODE_C STRING,
	DEPT_L3_NODE_M STRING,
	DEPT_L3_LEVL_N NUMBER(3,0),
	DEPT_L3_DISP_SEQN_N NUMBER(38,0),
	DEPT_L3_REPT_F STRING,
	DEPT_L3_RPRT_TYPE_C STRING,
	DEPT_L4_NODE_C STRING,
	DEPT_L4_NODE_M STRING,
	DEPT_L4_LEVL_N NUMBER(3,0),
	DEPT_L4_DISP_SEQN_N NUMBER(38,0),
	DEPT_L4_REPT_F STRING,
	DEPT_L4_RPRT_TYPE_C STRING,
	DEPT_L5_NODE_C STRING,
	DEPT_L5_NODE_M STRING,
	DEPT_L5_LEVL_N NUMBER(3,0),
	DEPT_L5_DISP_SEQN_N NUMBER(38,0),
	DEPT_L5_REPT_F STRING,
	DEPT_L5_RPRT_TYPE_C STRING,
	DEPT_L6_NODE_C STRING,
	DEPT_L6_NODE_M STRING,
	DEPT_L6_LEVL_N NUMBER(3,0),
	DEPT_L6_DISP_SEQN_N NUMBER(38,0),
	DEPT_L6_REPT_F STRING,
	DEPT_L6_RPRT_TYPE_C STRING,
	DEPT_L7_NODE_C STRING,
	DEPT_L7_NODE_M STRING,
	DEPT_L7_LEVL_N NUMBER(3,0),
	DEPT_L7_DISP_SEQN_N NUMBER(38,0),
	DEPT_L7_REPT_F STRING,
	DEPT_L7_RPRT_TYPE_C STRING,
	DEPT_L8_NODE_C STRING,
	DEPT_L8_NODE_M STRING,
	DEPT_L8_LEVL_N NUMBER(3,0),
	DEPT_L8_DISP_SEQN_N NUMBER(38,0),
	DEPT_L8_REPT_F STRING,
	DEPT_L8_RPRT_TYPE_C STRING,
	DEPT_L9_NODE_C STRING,
	DEPT_L9_NODE_M STRING,
	DEPT_L9_LEVL_N NUMBER(3,0),
	DEPT_L9_DISP_SEQN_N NUMBER(38,0),
	DEPT_L9_REPT_F STRING,
	DEPT_L9_RPRT_TYPE_C STRING,
	DEPT_L10_NODE_C STRING,
	DEPT_L10_NODE_M STRING,
	DEPT_L10_LEVL_N NUMBER(3,0),
	DEPT_L10_DISP_SEQN_N NUMBER(38,0),
	DEPT_L10_REPT_F STRING,
	DEPT_L10_RPRT_TYPE_C STRING,
	DEPT_LEAF_NODE_C STRING,
	DEPT_LEAF_NODE_M STRING,
	DEPT_LEAF_LEVL_N NUMBER(3,0),
	DEPT_LEAF_DISP_SEQN_N NUMBER(38,0),
	DEPT_LEAF_RPRT_TYPE_C STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDGRD.GRD_GNRC_MAP (
	MAP_TYPE_C STRING NOT NULL COMMENT 'MAPPING TYPE CODE',
	EFFT_D DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	TARG_NUMC_C NUMBER(38,0) COMMENT 'TARGET NUMERIC CODE',
	TARG_CHAR_C STRING COMMENT 'TARGET CHARACTER CODE',
	SRCE_NUMC_1_C NUMBER(38,0) COMMENT 'SOURCE NUMERIC 1 CODE',
	SRCE_CHAR_1_C STRING COMMENT 'SOURCE CHARACTER 1 CODE',
	SRCE_NUMC_2_C NUMBER(38,0) COMMENT 'SOURCE NUMERIC 2 CODE',
	SRCE_CHAR_2_C STRING COMMENT 'SOURCE CHARACTER 2 CODE',
	SRCE_NUMC_3_C NUMBER(38,0) COMMENT 'SOURCE NUMERIC 3 CODE',
	SRCE_CHAR_3_C STRING COMMENT 'SOURCE CHARACTER 3 CODE',
	SRCE_NUMC_4_C NUMBER(38,0) COMMENT 'SOURCE NUMERIC 4 CODE',
	SRCE_CHAR_4_C STRING COMMENT 'SOURCE CHARACTER 4 CODE',
	SRCE_NUMC_5_C NUMBER(38,0) COMMENT 'SOURCE NUMERIC 5 CODE',
	SRCE_CHAR_5_C STRING COMMENT 'SOURCE CHARACTER 5 CODE',
	SRCE_NUMC_6_C NUMBER(38,0) COMMENT 'SOURCE NUMERIC 6 CODE',
	SRCE_CHAR_6_C STRING COMMENT 'SOURCE CHARACTER 6 CODE',
	SRCE_NUMC_7_C NUMBER(38,0) COMMENT 'SOURCE NUMERIC 7 CODE',
	SRCE_CHAR_7_C STRING COMMENT 'SOURCE CHARACTER 7 CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDGRD.NON_WORK_DAY (
	GEOA_TYPE_C STRING NOT NULL COMMENT 'GEOGRAPHICAL AREA TYPE CODE',
	GEOA_C STRING NOT NULL COMMENT 'GEOGRAPHICAL AREA CODE',
	NON_WORK_D DATE NOT NULL COMMENT 'NON-WORKING DATE',
	NON_WORK_DAY_TYPE_C STRING NOT NULL COMMENT 'NON-WORKING DAY TYPE CODE',
	NON_WORK_DAY_M STRING NOT NULL COMMENT 'NON-WORKING DAY NAME',
	HLDY_STUS_X STRING COMMENT 'Holiday Status Description'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDPATY.ACCT_PATY (
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PATY_ACCT_REL_C STRING NOT NULL COMMENT 'Party Account Relationship Code',
	REL_LEVL_C STRING COMMENT 'Relation Level Code',
	REL_REAS_C STRING COMMENT 'Relation Reason Code',
	REL_STUS_C STRING COMMENT 'Relation Status Code',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I NUMBER(38,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I NUMBER(38,0) COMMENT 'Process Key Expiry Identifier',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDPATY.PATY_REL (
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	RELD_PATY_I STRING NOT NULL COMMENT 'Related Party Identifier',
	REL_I STRING NOT NULL COMMENT 'Relationship Identifier',
	REL_REAS_C STRING NOT NULL COMMENT 'Relation Reason Code',
	REL_TYPE_C STRING NOT NULL COMMENT 'Relation TYPE Code',
	SRCE_SYST_C STRING NOT NULL COMMENT 'Source System Code',
	PATY_ROLE_C STRING NOT NULL COMMENT 'Party Role Code',
	REL_STUS_C STRING NOT NULL COMMENT 'Relation Status Code',
	REL_LEVL_C STRING NOT NULL COMMENT 'Relation level Code',
	REL_EFFT_D DATE COMMENT 'Relationship Effective Date',
	REL_EXPY_D DATE COMMENT 'Relationship Expiry Date',
	SRCE_SYST_REL_I STRING COMMENT 'Source System Relationship Identifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I NUMBER(38,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I NUMBER(38,0) COMMENT 'Process Key Expiry Identifier',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code',
	RISK_AGGR_F STRING COMMENT 'Risk Aggregation Flag'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDPATY.UTIL_PROS_ISAC (
	PROS_KEY_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Identifier',
	CONV_M STRING COMMENT 'Convert Name',
	CONV_TYPE_M STRING COMMENT 'Convert Type Name',
	PROS_RQST_S TIMESTAMP_NTZ(0) COMMENT 'Process Request Timestamp',
	PROS_LAST_RQST_S TIMESTAMP_NTZ(0) COMMENT 'Process Last Request Timestamp',
	PROS_RQST_Q NUMBER(38,0) COMMENT 'Process Request Count',
	BTCH_RUN_D DATE COMMENT 'Batch Run Date',
	BTCH_KEY_I DECIMAL(10,0) COMMENT 'Batch Key Identifier',
	SRCE_SYST_M STRING COMMENT 'Source System Name',
	SRCE_M STRING COMMENT 'Source Name',
	TRGT_M STRING COMMENT 'Trgt Name',
	SUCC_F STRING COMMENT 'Success Flag',
	COMT_F STRING COMMENT 'Complete Flag',
	COMT_S TIMESTAMP_NTZ(0) COMMENT 'Complete Timestamp',
	MLTI_LOAD_EFFT_D DATE COMMENT 'Multiple Load Effect Date',
	SYST_S TIMESTAMP_NTZ(0) COMMENT 'System Timestamp',
	MLTI_LOAD_COMT_S TIMESTAMP_NTZ(0) COMMENT 'Multiple Load Complete Timestamp',
	SYST_ET_Q NUMBER(38,0) COMMENT 'System Et Count',
	SYST_UV_Q NUMBER(38,0) COMMENT 'System Uv Count',
	SYST_INS_Q NUMBER(38,0) COMMENT 'System Ins Count',
	SYST_UPD_Q NUMBER(38,0) COMMENT 'System Upd Count',
	SYST_DEL_Q NUMBER(38,0) COMMENT 'System Del Count',
	SYST_ET_TABL_M STRING COMMENT 'System Et Table Name',
	SYST_UV_TABL_M STRING COMMENT 'System Uv Table Name',
	SYST_HEAD_ET_TABL_M STRING COMMENT 'System Head Et Table Name',
	SYST_HEAD_UV_TABL_M STRING COMMENT 'System Head Uv Table Name',
	SYST_TRLR_ET_TABL_M STRING COMMENT 'System Trailer Et Table Name',
	SYST_TRLR_UV_TABL_M STRING COMMENT 'System Trailer Uv Table Name',
	PREV_PROS_KEY_I DECIMAL(10,0) COMMENT 'Previous Process Key Identifier',
	HEAD_RECD_TYPE_C STRING COMMENT 'Head Record Type Code',
	HEAD_FILE_M STRING COMMENT 'Head File Name',
	HEAD_BTCH_RUN_D DATE COMMENT 'Head Batch Run Date',
	HEAD_FILE_CRAT_S TIMESTAMP_NTZ(0) COMMENT 'Head File Create Timestamp',
	HEAD_GENR_PRGM_M STRING COMMENT 'Head Generate Program Name',
	HEAD_BTCH_KEY_I DECIMAL(10,0) COMMENT 'Head Batch Key Identifier',
	HEAD_PROS_KEY_I DECIMAL(10,0) COMMENT 'Head Process Key Identifier',
	HEAD_PROS_PREV_KEY_I DECIMAL(10,0) COMMENT 'Head Process Previous Key Identifier',
	TRLR_RECD_TYPE_C STRING COMMENT 'Trailer Record Type Code',
	TRLR_RECD_Q NUMBER(38,0) COMMENT 'Trailer Record Count',
	TRLR_HASH_TOTL_A DECIMAL(18,4) COMMENT 'Trailer Hash Total Amount',
	TRLR_COLM_HASH_TOTL_M STRING COMMENT 'Trailer Column Hash Total Name',
	TRLR_EROR_RECD_Q NUMBER(38,0) COMMENT 'Trailer Error Record Count',
	TRLR_FILE_COMT_S TIMESTAMP_NTZ(0) COMMENT 'Trailer File Complete Timestamp',
	TRLR_RECD_ISRT_Q NUMBER(38,0) COMMENT 'Trailer Record Insert Count',
	TRLR_RECD_UPDT_Q NUMBER(38,0) COMMENT 'Trailer Record Update Count',
	TRLR_RECD_DELT_Q NUMBER(38,0) COMMENT 'Trailer Record Delete Count'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDSECURITY.ROW_LEVL_SECU_USER_PRFL (
	USERNAME STRING NOT NULL COMMENT 'User Name',
	ROW_SECU_PRFL_C BINARY NOT NULL COMMENT 'Row Security Profile Code',
	MY_SERVICE_NO STRING COMMENT 'MyServiceNo',
	REQ_NO STRING COMMENT 'REQNo',
	RITM_NO STRING COMMENT 'RITMNo',
	SAR_NO STRING COMMENT 'SARNo',
	CMMT STRING COMMENT 'Comments',
	UPDT_USERNAME STRING NOT NULL COMMENT 'User Name',
	UPDT_DATE DATE NOT NULL COMMENT 'Update Date',
	UPDT_DTTS TIMESTAMP_NTZ(0) NOT NULL COMMENT 'Update DateTime'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDSRCCS.PLAN_BALN_SEGM_MSTR (
	CORP_IDNN STRING NOT NULL COMMENT 'Corporation Identification',
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PLAN_IDNN STRING NOT NULL COMMENT 'Plan Identification',
	PLAN_SQNO DECIMAL(3,0) COMMENT 'Plan Sequence Number',
	SRCE_SYST_ACCT_NUMB STRING COMMENT 'Source System Account Number',
	STOR_IDNN STRING COMMENT 'Store Identification',
	ORIG_STAT STRING COMMENT 'Original State',
	PLAN_TYPE STRING COMMENT 'Plan Type',
	PLAN_CATG STRING COMMENT 'Plan Category',
	POST_PUR_STUS STRING COMMENT 'Post Purchase Status',
	TERM_STUS STRING COMMENT 'Terms Status',
	INT_DEFR_STUS STRING COMMENT 'Interest Defer Status',
	PAYT_DEFR_STUS STRING COMMENT 'Payment Defer Status',
	TIME_BILL DECIMAL(3,0) COMMENT 'Times Billed',
	SPEC_TERM_CYCL DECIMAL(3,0) COMMENT 'Special Terms Cycle',
	INT_DEFR_CYCL DECIMAL(3,0) COMMENT 'Interest Defer Cycle',
	PAYT_DEFR_CYCL DECIMAL(3,0) COMMENT 'Payment Defer Cycle',
	DATE_SPEC_TERM_END DATE COMMENT 'Date Special Terms End',
	DATE_INT_DEFR_END DATE COMMENT 'Date Interest Defer End',
	DATE_PAYT_DEFR_END DATE COMMENT 'Date Payment Defer End',
	DATE_FRST_TRAN DATE COMMENT 'Date First Transaction',
	DATE_PAID_OFF DATE COMMENT 'Date Paid Off',
	DATE_LAST_PAYT DATE COMMENT 'Date Last Payment',
	DATE_LAST_MNTN DATE COMMENT 'Date Last Maintenance',
	LFTD_HIGH_BALN DECIMAL(13,2) COMMENT 'Life To Date High Balance',
	PAYT_TABL_HIGH_BALN DECIMAL(13,2) COMMENT 'Payment Table High Balance',
	LFTD_ICHG DECIMAL(13,2) COMMENT 'Life to Date Interest Charged',
	LFTD_INT_WAVR DECIMAL(13,2) COMMENT 'Life to Date Interest Waived',
	FIX_PAYT_AMT DECIMAL(13,2) COMMENT 'Fixed Payment Amount',
	FIX_PACY DECIMAL(3,0) COMMENT 'Fixed Payment Cycle',
	LAST_IRTE DECIMAL(7,2) COMMENT 'Last Interest Rate',
	LAST_RATE_CODE STRING COMMENT 'Last Rate Code',
	LAST_RATE_CODE_SEQN DECIMAL(4,0) COMMENT 'Last Rate Code Sequence',
	LAST_MIN_PAYT_CODE STRING COMMENT 'Last Minimum Payment Code',
	LAST_MIN_PAYT_CODE_SEQN DECIMAL(4,0) COMMENT 'Last Minimum Payment Code Sequence',
	LAST_MIN_PAYT_AMT DECIMAL(13,2) COMMENT 'Last Minimum Payment Amount',
	CURR_MIN_PAYT_DUE DECIMAL(13,2) COMMENT 'Current Minimum Payment Due',
	MIN_PAYT_PAST_DUE DECIMAL(13,2) COMMENT 'Minimum Payment Past Due',
	LAST_APPY_PAYT_AMT DECIMAL(13,2) COMMENT 'Last Applied Payment Amount',
	PAYT_BFOR_GRCE DECIMAL(13,2) COMMENT 'Payments Before Grace',
	ORIG_BALN DECIMAL(13,2) COMMENT 'Original Balance',
	INT_SAVE_RATE DECIMAL(2,2) COMMENT 'Interest Saver Rate',
	PLAN_DUE_DATE DATE COMMENT 'Plan Due Date',
	INT_FEE_END_DATE DATE COMMENT 'Interest Fee End Date',
	INT_FREE_BALN DECIMAL(13,2) COMMENT 'Interest Free Balance',
	OPEN_BALN_CURR_CYCL DECIMAL(13,2) COMMENT 'Opening Balance Current Cycle',
	CTD_SALE_TRAN DECIMAL(13,2) COMMENT 'Cycle To Date Sales Transactions',
	CTD_DR_TRAN DECIMAL(13,2) COMMENT 'Cycle To Date Debit Transactions',
	CTD_CR_TRAN DECIMAL(13,2) COMMENT 'Cycle To Date Credit Transactions',
	CTD_PAYT_TRAN DECIMAL(13,2) COMMENT 'Cycle To Date Payment Transactions',
	INT_UNPD_OPEN_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Interest Unpaid Opening Not Accrued Balance',
	INT_UNPD_CTD_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Interest Unpaid Cycle To Date Not Accrued Balance',
	INT_UNPD_CURR_CYCL_BALN DECIMAL(13,2) COMMENT 'Interest Unpaid Current Cycle Balance',
	INT_UNPD_1_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Interest Unpaid 1 Cycle AGO Balance',
	INT_UNPD_2_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Interest Unpaid 2 Cycle AGO Balance',
	FEE_UNPD_OPEN_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Fees Unpaid Opening Not Accrued Balance',
	FEE_UNPD_CTD_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Fees Unpaid Cycle To Date Not Accrued Balance',
	FEE_UNPD_CURR_CYCL_BALN DECIMAL(13,2) COMMENT 'Fees Unpaid Current Cycle Balance',
	FEE_UNPD_1_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Fees Unpaid 1 Cycle AGO Balance',
	FEE_UNPD_2_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Fees Unpaid 2 Cycle AGO Balance',
	AMF_UNPD_OPEN_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Annual Membership Fee Unpaid Opening Not Accrued Balance',
	AMF_UNPD_CTD_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Annual Membership Fee Unpaid Cycle To Date Not Accrued Balan',
	AMF_UNPD_CURR_CYCL_BALN DECIMAL(13,2) COMMENT 'Annual Membership Fee Unpaid Current Cycle Balance',
	AMF_UNPD_1_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Annual Membership Fee Unpaid 1 Cycle AGO Balance',
	AMF_UNPD_2_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Annual Membership Fee Unpaid 2 Cycle AGO Balance',
	INSR_UNPD_OPEN_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Insurance Unpaid Opening Not Accrued Balance',
	INSR_UNPD_CTD_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Insurance Unpaid Cycle To Date Not Accrued Balance',
	INSR_UNPD_CURR_CYCL_BALN DECIMAL(13,2) COMMENT 'Insurance Unpaid Current Cycle Balance',
	INSR_UNPD_1_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Insurance Unpaid 1 Cycle AGO Balance',
	INSR_UNPD_2_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Insurance Unpaid 2 Cycle AGO Balance',
	PRIN_UNPD_OPEN_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Principal Unpaid Opening Not Accrued Balance',
	PRIN_UNPD_CTD_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Principal Unpaid Cycle To Date Not Accrued Balance',
	PRIN_UNPD_CURR_CYCL_BALN DECIMAL(13,2) COMMENT 'Principal Unpaid Current Cycle Balance',
	PRIN_UNPD_1_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Principal Unpaid 1 Cycle AGO Balance',
	PRIN_UNPD_2_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Principal Unpaid 2 Cycle AGO Balance',
	AGGR_CURR_CYCL_BALN DECIMAL(15,2) COMMENT 'Aggregate Current Cycle Balance',
	AGGR_1_CYCL_AGO_BALN DECIMAL(15,2) COMMENT 'Aggregate 1 Cycle AGO Balance',
	AGGR_2_CYCL_AGO_BALN DECIMAL(15,2) COMMENT 'Aggregate 2 Cycle AGO Balance',
	AGGR_DLAY_BALN DECIMAL(15,2) COMMENT 'Aggregate Delay Balance',
	DLAY_DAY_QNTY DECIMAL(3,0) COMMENT 'Delayed Day Quantity',
	CTD_CSAD_FEE_AMT DECIMAL(13,2) COMMENT 'Cycle To Date Cash Advance Fee Amount',
	CTD_SYST_GENR_CSAD_FEE_AMT DECIMAL(13,2) COMMENT 'Cycle To Date System Generated Cash Advance Fee Amount',
	INT_DEFR_DAY_QNTY DECIMAL(3,0) COMMENT 'Interest Deferred Days Quantity',
	INT_DEFR_AMT DECIMAL(13,2) COMMENT 'Interest Deferred Amount',
	INT_DEFR_AGGR_CURR_BALN DECIMAL(15,2) COMMENT 'Interest Deferred Aggregate Current Balance',
	USER_CODE STRING COMMENT 'User Code',
	MIGR_TO_PLAN_IDNN STRING COMMENT 'Migrate To Plan Identification',
	MIGR_TO_PLAN_SQNO DECIMAL(3,0) COMMENT 'Migrate To Plan Sequence Number',
	DATE_MIGR DATE COMMENT 'Date Migrate',
	DSPT_AMT DECIMAL(13,2) COMMENT 'Disputed Amount',
	EXCL_DSPT_AMT DECIMAL(13,2) COMMENT 'Excluded Disputed Amount',
	ACRL_DSPT_AMT DECIMAL(13,2) COMMENT 'Accrued Disputed Amount',
	FRNT_LOAD_ISMT_BILL DECIMAL(3,0) COMMENT 'Front Load Install Billed',
	FRNT_LOAD_LAST_ISMT_DATE DATE COMMENT 'Front Load Last Install Date',
	FRNT_LOAD_ORIG_INT_AMT DECIMAL(13,2) COMMENT 'Front Load Original Interest Amount',
	FRNT_LOAD_EARN_INT_AMT DECIMAL(13,2) COMMENT 'Front Load Earned Interest Amount',
	FRNT_LOAD_REBT_INT_AMT DECIMAL(13,2) COMMENT 'Front Load Rebate Interest Amount',
	FRNT_LOAD_IRTE DECIMAL(6,6) COMMENT 'Front Load Interest Rate',
	FRNT_LOAD_ORIG_INSR_AMT DECIMAL(13,2) COMMENT 'Front Load Original Insurance Amount',
	FRNT_LOAD_EARN_INSR_AMT DECIMAL(13,2) COMMENT 'Front Load Earned Insurance Amount',
	FRNT_LOAD_UPFR_INSR_AMT DECIMAL(13,2) COMMENT 'Front Load Up Front Insurance Amount',
	FRNT_LOAD_REBT_INSR_AMT DECIMAL(13,2) COMMENT 'Front Load Rebate Insurance Amount',
	SCHE_PAYF_DATE DATE COMMENT 'Scheduled Payoff Date',
	SCHE_PAYF_AMT DECIMAL(13,2) COMMENT 'Scheduled Payoff Amount',
	SCHE_PAYF_FEE DECIMAL(13,2) COMMENT 'Scheduled Payoff Fee',
	SCHE_PAYF_REAS STRING COMMENT 'Scheduled Payoff Reason',
	ACTL_PAYF_DATE DATE COMMENT 'Actual Payoff Date',
	ACTL_PAYF_AMT DECIMAL(13,2) COMMENT 'Actual Payoff Amount',
	FRNT_LOAD_INSR_PAID DECIMAL(13,2) COMMENT 'Front Load Insurance Paid',
	FRNT_LOAD_REBT_INSR_PAID DECIMAL(13,2) COMMENT 'Front Load Rebate Insurance Paid',
	GOVT_CHRG_NOT_ACRL_OPEN_AMT DECIMAL(13,2) COMMENT 'Government Charges Unpaid Non Accruing Open Amount',
	GOVT_CHRG_NOT_ACRL_CTD_AMT DECIMAL(13,2) COMMENT 'Government Charges Non-Accruing Cycle-To-Date Amount',
	GOVT_CHRG_CURR_CYCL_AMT DECIMAL(13,2) COMMENT 'Government Charges Current Cycle Amount',
	GOVT_CHRG_1_CYCL_AGO_AMT DECIMAL(13,2) COMMENT 'Government Charges 1 Cycle Ago Amount',
	GOVT_CHRG_2_CYCL_AGO_AMT DECIMAL(13,2) COMMENT 'Government Charges 2 Cycle Ago Amount',
	CTD_FNCL_CHRG_REV DECIMAL(7,2) COMMENT 'Cycle To Date Finance Charge Reversals',
	ISMT_QNTY DECIMAL(3,0) COMMENT 'Number Of Installments',
	ISMT_PREV_QNTY DECIMAL(3,0) COMMENT 'Previous Number Of Installments',
	DATE_ISMT_TERM_CHNG DATE COMMENT 'Date Instalment Term Changed',
	PREV_MIN_PAYT_AMT DECIMAL(13,2) COMMENT 'Previous Minimum Payment Amount',
	ORIG_LOAN_BALN DECIMAL(13,2) COMMENT 'Original Loan Balance',
	LFTD_ALL_CR DECIMAL(13,2) COMMENT 'Life To Date All Credits',
	LFTD_INT_SAVE DECIMAL(13,2) COMMENT 'Life To Date Interest Saved',
	PREV_CYCL_INT_SAVE DECIMAL(13,2) COMMENT 'Previous Cycle Interest Saved',
	DATE_LOAN_PAID_OUT DATE COMMENT 'Date Loan Paid Out',
	PRJC_PAY_OFF_DATE DATE COMMENT 'Projected Pay-Off Date',
	DSPT_QNTY DECIMAL(3,0) COMMENT 'Disputed Quantity',
	DSPT_OLD_DATE DATE COMMENT 'Disputed Old Date',
	TERM DECIMAL(13,2) COMMENT 'Term',
	ROW_I NUMBER(38,0) COMMENT 'Row Identifier',
	EFFT_S TIMESTAMP_NTZ(6) NOT NULL COMMENT 'Effective Timestamp',
	-- FORMAT 'YYYY-MM-DDBHH:MI:SSDS(6)' - FORMAT IN TABLE PDSRCCS.PLAN_BALN_SEGM_MSTR NOT SUPPORTED EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EFFT_T TIME(6) COMMENT 'Effective Time',
	-- FORMAT 'HH:MI:SSDS(6)' - FORMAT IN TABLE PDSRCCS.PLAN_BALN_SEGM_MSTR NOT SUPPORTED PROS_KEY_EFFT_I NUMBER(38,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDSRCCS.PLAN_BALN_SEGM_MSTR_ARCH (
	CORP_IDNN STRING NOT NULL COMMENT 'Corporation Identification',
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PLAN_IDNN STRING NOT NULL COMMENT 'Plan Identification',
	PLAN_SQNO DECIMAL(3,0) COMMENT 'Plan Sequence Number',
	SRCE_SYST_ACCT_NUMB STRING COMMENT 'Source System Account Number',
	STOR_IDNN STRING COMMENT 'Store Identification',
	ORIG_STAT STRING COMMENT 'Original State',
	PLAN_TYPE STRING COMMENT 'Plan Type',
	PLAN_CATG STRING COMMENT 'Plan Category',
	POST_PUR_STUS STRING COMMENT 'Post Purchase Status',
	TERM_STUS STRING COMMENT 'Terms Status',
	INT_DEFR_STUS STRING COMMENT 'Interest Defer Status',
	PAYT_DEFR_STUS STRING COMMENT 'Payment Defer Status',
	TIME_BILL DECIMAL(3,0) COMMENT 'Times Billed',
	SPEC_TERM_CYCL DECIMAL(3,0) COMMENT 'Special Terms Cycle',
	INT_DEFR_CYCL DECIMAL(3,0) COMMENT 'Interest Defer Cycle',
	PAYT_DEFR_CYCL DECIMAL(3,0) COMMENT 'Payment Defer Cycle',
	DATE_SPEC_TERM_END DATE COMMENT 'Date Special Terms End',
	DATE_INT_DEFR_END DATE COMMENT 'Date Interest Defer End',
	DATE_PAYT_DEFR_END DATE COMMENT 'Date Payment Defer End',
	DATE_FRST_TRAN DATE COMMENT 'Date First Transaction',
	DATE_PAID_OFF DATE COMMENT 'Date Paid Off',
	DATE_LAST_PAYT DATE COMMENT 'Date Last Payment',
	DATE_LAST_MNTN DATE COMMENT 'Date Last Maintenance',
	LFTD_HIGH_BALN DECIMAL(13,2) COMMENT 'Life To Date High Balance',
	PAYT_TABL_HIGH_BALN DECIMAL(13,2) COMMENT 'Payment Table High Balance',
	LFTD_ICHG DECIMAL(13,2) COMMENT 'Life to Date Interest Charged',
	LFTD_INT_WAVR DECIMAL(13,2) COMMENT 'Life to Date Interest Waived',
	FIX_PAYT_AMT DECIMAL(13,2) COMMENT 'Fixed Payment Amount',
	FIX_PACY DECIMAL(3,0) COMMENT 'Fixed Payment Cycle',
	LAST_IRTE DECIMAL(7,2) COMMENT 'Last Interest Rate',
	LAST_RATE_CODE STRING COMMENT 'Last Rate Code',
	LAST_RATE_CODE_SEQN DECIMAL(4,0) COMMENT 'Last Rate Code Sequence',
	LAST_MIN_PAYT_CODE STRING COMMENT 'Last Minimum Payment Code',
	LAST_MIN_PAYT_CODE_SEQN DECIMAL(4,0) COMMENT 'Last Minimum Payment Code Sequence',
	LAST_MIN_PAYT_AMT DECIMAL(13,2) COMMENT 'Last Minimum Payment Amount',
	CURR_MIN_PAYT_DUE DECIMAL(13,2) COMMENT 'Current Minimum Payment Due',
	MIN_PAYT_PAST_DUE DECIMAL(13,2) COMMENT 'Minimum Payment Past Due',
	LAST_APPY_PAYT_AMT DECIMAL(13,2) COMMENT 'Last Applied Payment Amount',
	PAYT_BFOR_GRCE DECIMAL(13,2) COMMENT 'Payments Before Grace',
	ORIG_BALN DECIMAL(13,2) COMMENT 'Original Balance',
	INT_SAVE_RATE DECIMAL(2,2) COMMENT 'Interest Saver Rate',
	PLAN_DUE_DATE DATE COMMENT 'Plan Due Date',
	INT_FEE_END_DATE DATE COMMENT 'Interest Fee End Date',
	INT_FREE_BALN DECIMAL(13,2) COMMENT 'Interest Free Balance',
	OPEN_BALN_CURR_CYCL DECIMAL(13,2) COMMENT 'Opening Balance Current Cycle',
	CTD_SALE_TRAN DECIMAL(13,2) COMMENT 'Cycle To Date Sales Transactions',
	CTD_DR_TRAN DECIMAL(13,2) COMMENT 'Cycle To Date Debit Transactions',
	CTD_CR_TRAN DECIMAL(13,2) COMMENT 'Cycle To Date Credit Transactions',
	CTD_PAYT_TRAN DECIMAL(13,2) COMMENT 'Cycle To Date Payment Transactions',
	INT_UNPD_OPEN_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Interest Unpaid Opening Not Accrued Balance',
	INT_UNPD_CTD_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Interest Unpaid Cycle To Date Not Accrued Balance',
	INT_UNPD_CURR_CYCL_BALN DECIMAL(13,2) COMMENT 'Interest Unpaid Current Cycle Balance',
	INT_UNPD_1_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Interest Unpaid 1 Cycle AGO Balance',
	INT_UNPD_2_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Interest Unpaid 2 Cycle AGO Balance',
	FEE_UNPD_OPEN_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Fees Unpaid Opening Not Accrued Balance',
	FEE_UNPD_CTD_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Fees Unpaid Cycle To Date Not Accrued Balance',
	FEE_UNPD_CURR_CYCL_BALN DECIMAL(13,2) COMMENT 'Fees Unpaid Current Cycle Balance',
	FEE_UNPD_1_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Fees Unpaid 1 Cycle AGO Balance',
	FEE_UNPD_2_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Fees Unpaid 2 Cycle AGO Balance',
	AMF_UNPD_OPEN_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Annual Membership Fee Unpaid Opening Not Accrued Balance',
	AMF_UNPD_CTD_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Annual Membership Fee Unpaid Cycle To Date Not Accrued Balan',
	AMF_UNPD_CURR_CYCL_BALN DECIMAL(13,2) COMMENT 'Annual Membership Fee Unpaid Current Cycle Balance',
	AMF_UNPD_1_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Annual Membership Fee Unpaid 1 Cycle AGO Balance',
	AMF_UNPD_2_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Annual Membership Fee Unpaid 2 Cycle AGO Balance',
	INSR_UNPD_OPEN_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Insurance Unpaid Opening Not Accrued Balance',
	INSR_UNPD_CTD_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Insurance Unpaid Cycle To Date Not Accrued Balance',
	INSR_UNPD_CURR_CYCL_BALN DECIMAL(13,2) COMMENT 'Insurance Unpaid Current Cycle Balance',
	INSR_UNPD_1_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Insurance Unpaid 1 Cycle AGO Balance',
	INSR_UNPD_2_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Insurance Unpaid 2 Cycle AGO Balance',
	PRIN_UNPD_OPEN_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Principal Unpaid Opening Not Accrued Balance',
	PRIN_UNPD_CTD_NOT_ACRL_BALN DECIMAL(13,2) COMMENT 'Principal Unpaid Cycle To Date Not Accrued Balance',
	PRIN_UNPD_CURR_CYCL_BALN DECIMAL(13,2) COMMENT 'Principal Unpaid Current Cycle Balance',
	PRIN_UNPD_1_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Principal Unpaid 1 Cycle AGO Balance',
	PRIN_UNPD_2_CYCL_AGO_BALN DECIMAL(13,2) COMMENT 'Principal Unpaid 2 Cycle AGO Balance',
	AGGR_CURR_CYCL_BALN DECIMAL(15,2) COMMENT 'Aggregate Current Cycle Balance',
	AGGR_1_CYCL_AGO_BALN DECIMAL(15,2) COMMENT 'Aggregate 1 Cycle AGO Balance',
	AGGR_2_CYCL_AGO_BALN DECIMAL(15,2) COMMENT 'Aggregate 2 Cycle AGO Balance',
	AGGR_DLAY_BALN DECIMAL(15,2) COMMENT 'Aggregate Delay Balance',
	DLAY_DAY_QNTY DECIMAL(3,0) COMMENT 'Delayed Day Quantity',
	CTD_CSAD_FEE_AMT DECIMAL(13,2) COMMENT 'Cycle To Date Cash Advance Fee Amount',
	CTD_SYST_GENR_CSAD_FEE_AMT DECIMAL(13,2) COMMENT 'Cycle To Date System Generated Cash Advance Fee Amount',
	INT_DEFR_DAY_QNTY DECIMAL(3,0) COMMENT 'Interest Deferred Days Quantity',
	INT_DEFR_AMT DECIMAL(13,2) COMMENT 'Interest Deferred Amount',
	INT_DEFR_AGGR_CURR_BALN DECIMAL(15,2) COMMENT 'Interest Deferred Aggregate Current Balance',
	USER_CODE STRING COMMENT 'User Code',
	MIGR_TO_PLAN_IDNN STRING COMMENT 'Migrate To Plan Identification',
	MIGR_TO_PLAN_SQNO DECIMAL(3,0) COMMENT 'Migrate To Plan Sequence Number',
	DATE_MIGR DATE COMMENT 'Date Migrate',
	DSPT_AMT DECIMAL(13,2) COMMENT 'Disputed Amount',
	EXCL_DSPT_AMT DECIMAL(13,2) COMMENT 'Excluded Disputed Amount',
	ACRL_DSPT_AMT DECIMAL(13,2) COMMENT 'Accrued Disputed Amount',
	FRNT_LOAD_ISMT_BILL DECIMAL(3,0) COMMENT 'Front Load Install Billed',
	FRNT_LOAD_LAST_ISMT_DATE DATE COMMENT 'Front Load Last Install Date',
	FRNT_LOAD_ORIG_INT_AMT DECIMAL(13,2) COMMENT 'Front Load Original Interest Amount',
	FRNT_LOAD_EARN_INT_AMT DECIMAL(13,2) COMMENT 'Front Load Earned Interest Amount',
	FRNT_LOAD_REBT_INT_AMT DECIMAL(13,2) COMMENT 'Front Load Rebate Interest Amount',
	FRNT_LOAD_IRTE DECIMAL(6,6) COMMENT 'Front Load Interest Rate',
	FRNT_LOAD_ORIG_INSR_AMT DECIMAL(13,2) COMMENT 'Front Load Original Insurance Amount',
	FRNT_LOAD_EARN_INSR_AMT DECIMAL(13,2) COMMENT 'Front Load Earned Insurance Amount',
	FRNT_LOAD_UPFR_INSR_AMT DECIMAL(13,2) COMMENT 'Front Load Up Front Insurance Amount',
	FRNT_LOAD_REBT_INSR_AMT DECIMAL(13,2) COMMENT 'Front Load Rebate Insurance Amount',
	SCHE_PAYF_DATE DATE COMMENT 'Scheduled Payoff Date',
	SCHE_PAYF_AMT DECIMAL(13,2) COMMENT 'Scheduled Payoff Amount',
	SCHE_PAYF_FEE DECIMAL(13,2) COMMENT 'Scheduled Payoff Fee',
	SCHE_PAYF_REAS STRING COMMENT 'Scheduled Payoff Reason',
	ACTL_PAYF_DATE DATE COMMENT 'Actual Payoff Date',
	ACTL_PAYF_AMT DECIMAL(13,2) COMMENT 'Actual Payoff Amount',
	FRNT_LOAD_INSR_PAID DECIMAL(13,2) COMMENT 'Front Load Insurance Paid',
	FRNT_LOAD_REBT_INSR_PAID DECIMAL(13,2) COMMENT 'Front Load Rebate Insurance Paid',
	GOVT_CHRG_NOT_ACRL_OPEN_AMT DECIMAL(13,2) COMMENT 'Government Charges Unpaid Non Accruing Open Amount',
	GOVT_CHRG_NOT_ACRL_CTD_AMT DECIMAL(13,2) COMMENT 'Government Charges Non-Accruing Cycle-To-Date Amount',
	GOVT_CHRG_CURR_CYCL_AMT DECIMAL(13,2) COMMENT 'Government Charges Current Cycle Amount',
	GOVT_CHRG_1_CYCL_AGO_AMT DECIMAL(13,2) COMMENT 'Government Charges 1 Cycle Ago Amount',
	GOVT_CHRG_2_CYCL_AGO_AMT DECIMAL(13,2) COMMENT 'Government Charges 2 Cycle Ago Amount',
	CTD_FNCL_CHRG_REV DECIMAL(7,2) COMMENT 'Cycle To Date Finance Charge Reversals',
	ISMT_QNTY DECIMAL(3,0) COMMENT 'Number Of Installments',
	ISMT_PREV_QNTY DECIMAL(3,0) COMMENT 'Previous Number Of Installments',
	DATE_ISMT_TERM_CHNG DATE COMMENT 'Date Instalment Term Changed',
	PREV_MIN_PAYT_AMT DECIMAL(13,2) COMMENT 'Previous Minimum Payment Amount',
	ORIG_LOAN_BALN DECIMAL(13,2) COMMENT 'Original Loan Balance',
	LFTD_ALL_CR DECIMAL(13,2) COMMENT 'Life To Date All Credits',
	LFTD_INT_SAVE DECIMAL(13,2) COMMENT 'Life To Date Interest Saved',
	PREV_CYCL_INT_SAVE DECIMAL(13,2) COMMENT 'Previous Cycle Interest Saved',
	DATE_LOAN_PAID_OUT DATE COMMENT 'Date Loan Paid Out',
	PRJC_PAY_OFF_DATE DATE COMMENT 'Projected Pay-Off Date',
	DSPT_QNTY DECIMAL(3,0) COMMENT 'Disputed Quantity',
	DSPT_OLD_DATE DATE COMMENT 'Disputed Old Date',
	TERM DECIMAL(13,2) COMMENT 'Term',
	ROW_I NUMBER(38,0) COMMENT 'Row Identifier',
	EFFT_S TIMESTAMP_NTZ(6)NOT NULL COMMENT 'Effective Timestamp',
	-- FORMAT 'YYYY-MM-DDBHH:MI:SSDS(6)' - FORMAT IN TABLE PDSRCCS.PLAN_BALN_SEGM_MSTR_ARCH NOT SUPPORTED EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EFFT_T TIME(6) COMMENT 'Effective Time',
	-- FORMAT 'HH:MI:SSDS(6)' - FORMAT IN TABLE PDSRCCS.PLAN_BALN_SEGM_MSTR_ARCH NOT SUPPORTED PROS_KEY_EFFT_I NUMBER(38,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDSRCCS.UTIL_BTCH_ISAC (
	BTCH_KEY_I DECIMAL(10,0) NOT NULL,
	BTCH_RQST_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE PDSRCCS.UTIL_BTCH_ISAC NOT SUPPORTED BTCH_RUN_D DATE NOT NULL,
	BTCH_RUN_T TIME(6) NOT NULL,
	-- FORMAT 'HH:MI:SS' !!!RESOLVE - FORMAT IN TABLE PDSRCCS.UTIL_BTCH_ISAC NOT SUPPORTED SRCE_SYST_M STRING NOT NULL,
	SRCE_SUB_SYST_M STRING NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDSRCCS.UTIL_PARM (
	PARM_M STRING NOT NULL,
	PARM_LTRL_N NUMBER(38,0),
	PARM_LTRL_D DATE,
	PARM_LTRL_STRG_X STRING,
	PARM_LTRL_A DECIMAL(18,4)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDSRCCS.UTIL_PROS_ISAC (
	PROS_KEY_I DECIMAL(10,0) NOT NULL,
	CONV_M STRING,
	CONV_TYPE_M STRING,
	PROS_RQST_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE PDSRCCS.UTIL_PROS_ISAC NOT SUPPORTED PROS_LAST_RQST_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE PDSRCCS.UTIL_PROS_ISAC NOT SUPPORTED PROS_RQST_Q NUMBER(38,0),
	BTCH_KEY_I DECIMAL(10,0),
	PROS_EFFT_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE PDSRCCS.UTIL_PROS_ISAC NOT SUPPORTED STUS_C STRING,
	STUS_CHNG_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE PDSRCCS.UTIL_PROS_ISAC NOT SUPPORTED SRCE_SYST_M STRING,
	SRCE_M STRING,
	TRGT_M STRING,
	COMT_F STRING,
	COMT_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE PDSRCCS.UTIL_PROS_ISAC NOT SUPPORTED SYST_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE PDSRCCS.UTIL_PROS_ISAC NOT SUPPORTED SYST_ET_Q NUMBER(38,0),
	SYST_UV_Q NUMBER(38,0),
	SYST_INS_Q NUMBER(38,0),
	SYST_UPD_Q NUMBER(38,0),
	SYST_DEL_Q NUMBER(38,0),
	SYST_ET_TABL_M STRING,
	SYST_UV_TABL_M STRING,
	SRCE_RECD_CNT NUMBER(38,0),
	SRCE_LOAD_CNT NUMBER(38,0),
	SRCE_BTCH_LOAD_CNT NUMBER(38,0)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDSRCCS.UTIL_TRSF_EROR_RQM3 (
	SRCE_KEY_I STRING NOT NULL,
	CONV_M STRING NOT NULL,
	CONV_MAP_RULE_M STRING,
	TRSF_TABL_M STRING,
	SRCE_EFFT_D DATE NOT NULL,
	VALU_CHNG_BFOR_X STRING,
	VALU_CHNG_AFTR_X STRING,
	TRSF_X STRING,
	TRSF_COLM_M STRING,
	EROR_SEQN_I DECIMAL(10,0),
	SRCE_FILE_M STRING,
	PROS_KEY_EFFT_I DECIMAL(10,0),
	TRSF_KEY_I STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PDTRPC.WKND_PBLC_HLDY (
	CALR_D DATE,
	WKND_PBLC_HLDY_F STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PUTIL.PROS_EROR_LOG (
	SRCE_SYST_C STRING,
	SRCE_FILE_M STRING,
	CALG_PROS_DB_M STRING,
	CALG_PROS_M STRING,
	EROR_TIME_S TIMESTAMP_NTZ(6),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss.s(6)' - FORMAT IN TABLE PUTIL.PROS_EROR_LOG NOT SUPPORTED SQL_STAT_C DECIMAL(6,0),
	SQL_CODE_C DECIMAL(6,0),
	ACTV_CNT_Q DECIMAL(6,0),
	ABRT_PROS_F STRING,
	EROR_MSGE_X STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.PUTIL.TERASYNC (
	COOKIE NUMBER(38,0) NOT NULL,
	OPTYPE NUMBER(3,0) NOT NULL,
	CTLSTATE NUMBER(3,0) NOT NULL,
	EVENTCOUNT NUMBER(38,0) NOT NULL,
	LSN NUMBER(38,0),
	BLOCKCOUNT NUMBER(38,0),
	QUERY STRING,
	USERNAME STRING,
	DBNAME STRING,
	RECORDCOUNT FLOAT,
	BYTECOUNT FLOAT,
	REJECTCOUNT FLOAT,
	STARTTIME NUMBER(38,0),
	ENDTIME NUMBER(38,0)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_APPT_PDCT (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	APPT_PDCT_I STRING NOT NULL COMMENT 'Application Product Identifier',
	REL_TYPE_C STRING NOT NULL COMMENT 'Relation Type Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_REL (
	SUBJ_ACCT_I STRING NOT NULL,
	OBJC_ACCT_I STRING NOT NULL,
	REL_C STRING NOT NULL,
	EFFT_D DATE NOT NULL,
	EXPY_D DATE NOT NULL,
	STRT_D DATE,
	REL_EXPY_D DATE,
	PROS_KEY_EFFT_I DECIMAL(10,0),
	PROS_KEY_EXPY_I DECIMAL(10,0),
	EROR_SEQN_I DECIMAL(10,0),
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE',
	REL_STUS_C STRING COMMENT 'RELATIONSHIP STATUS TYPECODE',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	SUBJ_ACCT_LEVL_N NUMBER(3,0) COMMENT 'Subject Account Level Number',
	OBJC_ACCT_LEVL_N NUMBER(3,0) COMMENT 'Object Account Level Number',
	CRIN_SHRE_P DECIMAL(5,2) COMMENT 'Credit Interest Share Percentage',
	DR_INT_SHRE_P DECIMAL(5,2) COMMENT 'Debit Interest Share Percentage',
	RECORD_DELETED_FLAG NUMBER(3,0) NOT NULL COMMENT 'Record Deleted Flag',
	CTL_ID SMALLINT NOT NULL COMMENT 'Ctl Id',
	PROCESS_NAME STRING NOT NULL COMMENT 'Process Name',
	PROCESS_ID NUMBER(38,0) NOT NULL COMMENT 'Process Id',
	UPDATE_PROCESS_NAME STRING COMMENT 'Update Process Name',
	UPDATE_PROCESS_ID NUMBER(38,0) COMMENT 'Update Process Id'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_UNID_PATY (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	SRCE_SYST_PATY_I STRING NOT NULL COMMENT 'Source System Party Identifier',
	SRCE_SYST_C STRING NOT NULL COMMENT 'SOURCE SYSTEM CODE',
	PATY_ACCT_REL_C STRING NOT NULL COMMENT 'PARTY ACCOUNT RELATIONSHIP CODE',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE',
	ORIG_SRCE_SYST_C STRING COMMENT 'Original Source System Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_XREF_BPS_CBS (
	CBS_ACCT_I STRING NOT NULL COMMENT 'Cbs AccountIdentifier',
	BPS_ACCT_I STRING NOT NULL COMMENT 'Bps AccountIdentifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) COMMENT 'Process Key EffectiveIdentifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key ExpiryIdentifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error SequenceIdentifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.ACCT_XREF_MAS_DAR (
	MAS_MERC_ACCT_I STRING NOT NULL COMMENT 'MerchantIdentifier',
	DAR_ACCT_I STRING NOT NULL COMMENT 'Cbs AccountIdentifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) COMMENT 'Process Key EffectiveIdentifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key ExpiryIdentifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error SequenceIdentifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT (
	APPT_I STRING NOT NULL COMMENT 'Application Identifier',
	APPT_C STRING COMMENT 'Application Code',
	APPT_FORM_C STRING COMMENT 'Application Form Code',
	APPT_QLFY_C STRING NOT NULL COMMENT 'Application Qualifier Code',
	STUS_TRAK_I STRING COMMENT 'Status Tracking Identifier',
	APPT_ORIG_C STRING COMMENT 'Application Origin Code',
	APPT_N STRING COMMENT 'Application Number',
	SRCE_SYST_C STRING NOT NULL COMMENT 'Source System Code',
	SRCE_SYST_APPT_I STRING NOT NULL COMMENT 'Source System Application Identifier',
	APPT_CRAT_D DATE COMMENT 'Application Create Date',
	RATE_SEEK_F STRING COMMENT 'Rate Seeker Flag',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
	ORIG_APPT_SRCE_C STRING COMMENT 'Original Application Source Code',
	REL_MGR_STAT_C STRING COMMENT 'Relationship Manager State Code',
	APPT_RECV_S TIMESTAMP_NTZ(6) COMMENT 'Application Received Timestamp',
	APPT_RECV_D DATE COMMENT 'Application Received Date',
	APPT_RECV_T TIME(6) COMMENT 'Application Received Time',
	APPT_ENTR_POIT_M STRING COMMENT 'Entry Point Name',
	EXT_TO_PDCT_SYST_D DATE COMMENT 'Extract To Product System Date',
	EFFT_D DATE COMMENT 'Effective Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_DEPT (
	APPT_I STRING NOT NULL COMMENT 'Application Identifier',
	DEPT_ROLE_C STRING NOT NULL COMMENT 'Department Role Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	DEPT_I STRING NOT NULL COMMENT 'Department Identifier',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
	BRCH_N SMALLINT COMMENT 'Branch Number',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	CHNG_REAS_C STRING COMMENT 'Change Reason Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_FEAT (
	APPT_I STRING NOT NULL COMMENT 'Application Identifier',
	FEAT_I STRING NOT NULL COMMENT 'Feature Identifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	SRCE_SYST_C STRING NOT NULL COMMENT 'Source System Code',
	SRCE_SYST_APPT_FEAT_I STRING COMMENT 'Source System Application Feature Identifier',
	SRCE_SYST_STND_VALU_Q DECIMAL(15,6) COMMENT 'Source System Standard Value Quantity',
	SRCE_SYST_STND_VALU_R DECIMAL(18,6) COMMENT 'Source System Standard Value Rate',
	SRCE_SYST_STND_VALU_A DECIMAL(18,2) COMMENT 'Source System Standard Value Amount',
	ACTL_VALU_Q DECIMAL(15,6) COMMENT 'Actual Value Quantity',
	ACTL_VALU_R DECIMAL(18,6) COMMENT 'Actual Value Rate',
	ACTL_VALU_A DECIMAL(18,2) COMMENT 'Actual Value Amount',
	CNCY_C STRING COMMENT 'Currency Code',
	OVRD_FEAT_I STRING COMMENT 'Override Feature Identifier',
	OVRD_REAS_C STRING COMMENT 'Override Reason Code',
	FEAT_SEQN_N SMALLINT COMMENT 'Feature Sequence Number',
	FEAT_STRT_D DATE COMMENT 'Feature Start Date',
	FEE_CHRG_D DATE COMMENT 'Fee Charged Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT (
	APPT_PDCT_I STRING NOT NULL COMMENT 'Application Product Identifier',
	APPT_QLFY_C STRING COMMENT 'Application Qualifier Code',
	ACQR_TYPE_C STRING COMMENT 'Acquisition Type Code',
	ACQR_ADHC_X STRING COMMENT 'Acquisition Ad Hoc Comment',
	ACQR_SRCE_C STRING COMMENT 'Acquisition Source Code',
	PDCT_N NUMBER(38,0) NOT NULL COMMENT 'Product Number',
	APPT_I STRING NOT NULL COMMENT 'Application Identifier',
	SRCE_SYST_C STRING NOT NULL COMMENT 'Source System Code',
	SRCE_SYST_APPT_PDCT_I STRING NOT NULL COMMENT 'Source System Application Product Identifier',
	LOAN_FNDD_METH_C STRING COMMENT 'Loan Funding Method Code',
	NEW_ACCT_F STRING COMMENT 'New Account Flag',
	BROK_PATY_I STRING COMMENT 'Broker Party Identifier',
	COPY_FROM_OTHR_APPT_F STRING COMMENT 'Copy From Other Application Flag',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
	JOB_COMM_CATG_C STRING COMMENT 'JOB COMMISSION CATEGORY CODE',
	DEBT_ABN_X STRING,
	DEBT_BUSN_M STRING,
	SMPL_APPT_F STRING,
	APPT_PDCT_CATG_C STRING,
	APPT_PDCT_DURT_C STRING,
	ASES_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT_ACCT (
	APPT_PDCT_I STRING NOT NULL COMMENT 'Application Product Identifier',
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	REL_TYPE_C STRING NOT NULL COMMENT 'Relation Type Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT_AMT (
	APPT_PDCT_I STRING NOT NULL COMMENT 'Application Product Identifier',
	AMT_TYPE_C STRING NOT NULL COMMENT 'Amount Type Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	CNCY_C STRING COMMENT 'Currency Code',
	APPT_PDCT_A DECIMAL(18,2) COMMENT 'Application Product Amount',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
	XCES_AMT_REAS_X STRING,
	SRCE_SYST_C STRING,
	APPT_PDCT_AUD_EQAL_A DECIMAL(18,2) COMMENT 'Application Product Australian Dollar Equivalent Amount',
	CNCY_CONV_R DECIMAL(15,8) COMMENT 'Currency Conversion Rate',
	DISC_CNCY_CONV_R DECIMAL(15,8) COMMENT 'Discounted Currency Conversion Rate',
	DISC_CNCY_DEAL_AUTN_N SMALLINT COMMENT 'Discounted Currency Dealer Authorisation number',
	SRCE_SYST_APPT_PDCT_AMT_I STRING COMMENT 'Source System Application Identifier',
	PAYT_METH_TYPE_C STRING COMMENT 'Payment Method Type Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT_FEAT (
	APPT_PDCT_I STRING NOT NULL COMMENT 'Application Product Identifier',
	FEAT_I STRING NOT NULL COMMENT 'Feature Identifier',
	SRCE_SYST_APPT_FEAT_I STRING NOT NULL COMMENT 'Source System Application Feature Identifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	SRCE_SYST_C STRING NOT NULL COMMENT 'Source System Code',
	SRCE_SYST_APPT_OVRD_I STRING COMMENT 'Source System Application Override Identifier',
	OVRD_FEAT_I STRING COMMENT 'Overridden Feature Identifier',
	SRCE_SYST_STND_VALU_Q DECIMAL(15,6) COMMENT 'Source System Standard Value Quantity',
	SRCE_SYST_STND_VALU_R DECIMAL(18,6) COMMENT 'SourceSystem Standard Value Rate',
	SRCE_SYST_STND_VALU_A DECIMAL(18,2) COMMENT 'Source System Standard Value Amount',
	CNCY_C STRING COMMENT 'Currency Code',
	ACTL_VALU_Q DECIMAL(15,6) COMMENT ' Actual Value Quantity',
	ACTL_VALU_R DECIMAL(18,6) COMMENT 'Actual Value Rate',
	ACTL_VALU_A DECIMAL(18,2) COMMENT 'Actual Value Amount',
	FEAT_SEQN_N SMALLINT COMMENT 'Feature Sequence Number',
	FEAT_STRT_D DATE COMMENT 'Feature Start Date',
	FEE_CHRG_D DATE COMMENT 'Fee Charge Date',
	OVRD_REAS_C STRING COMMENT 'Override Reason Code',
	FEE_ADD_TO_TOTL_F STRING COMMENT 'Fee Added To Total Flag',
	FEE_CAPL_F STRING COMMENT 'Fee Capitalisation Flag',
	EXPY_D DATE NOT NULL COMMENT 'Expiry date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
	FEAT_VALU_C STRING COMMENT 'Feature Value Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT_PATY (
	APPT_PDCT_I STRING NOT NULL COMMENT 'Application Product Identifier',
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	PATY_ROLE_C STRING NOT NULL COMMENT 'Party Role Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	SRCE_SYST_C STRING NOT NULL COMMENT 'Source System Code',
	SRCE_SYST_APPT_PDCT_PATY_I STRING NOT NULL COMMENT 'Source System Application Product Party Identifier',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT_PURP (
	APPT_PDCT_I STRING NOT NULL COMMENT 'Application Product Identifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	SRCE_SYST_APPT_PDCT_PURP_I STRING NOT NULL COMMENT 'Source System Application Product Purpose Identifier',
	PURP_TYPE_C STRING NOT NULL COMMENT 'Purpose Type Code',
	PURP_CLAS_C STRING COMMENT 'Purpose Class Code',
	SRCE_SYST_C STRING NOT NULL COMMENT 'Source System Code',
	PURP_A DECIMAL(18,2) COMMENT 'Purpose Amount',
	CNCY_C STRING COMMENT 'Currency Code',
	MAIN_PURP_F STRING COMMENT 'Main Purpose Flag',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT_REL (
	APPT_PDCT_I STRING NOT NULL COMMENT 'Application Product Identifier',
	RELD_APPT_PDCT_I STRING NOT NULL COMMENT 'Related Application Product Identifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	REL_TYPE_C STRING NOT NULL COMMENT 'Relation Type Code',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT_RPAY (
	APPT_PDCT_I STRING NOT NULL COMMENT 'Application Product Identifier',
	RPAY_TYPE_C STRING NOT NULL COMMENT 'Repayment Type Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	PAYT_FREQ_C STRING COMMENT 'Payment Frequency Code',
	STRT_RPAY_D DATE COMMENT 'Start Repayment Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
	SRCE_SYST_C STRING,
	RPAY_SRCE_C STRING,
	RPAY_SRCE_OTHR_X STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_PDCT_UNID_PATY (
	APPT_PDCT_I STRING NOT NULL COMMENT 'Application Product Identifier',
	PATY_ROLE_C STRING NOT NULL COMMENT 'Party Role Code',
	SRCE_SYST_PATY_I STRING NOT NULL COMMENT 'Source System Party Identifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	SRCE_SYST_C STRING NOT NULL COMMENT 'Source System Code',
	UNID_PATY_CATG_C STRING COMMENT 'Unidentified Party Category Code',
	PATY_M STRING COMMENT 'Party Name',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_REL (
	APPT_I STRING NOT NULL COMMENT 'Application Identifier',
	RELD_APPT_I STRING NOT NULL COMMENT 'Related Application Identifier',
	REL_TYPE_C STRING NOT NULL COMMENT 'Relationship Type Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.APPT_TRNF_DETL (
	APPT_I STRING NOT NULL COMMENT 'Application Identifier',
	APPT_TRNF_I STRING NOT NULL COMMENT 'Application Transfer Identifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	TRNF_OPTN_C STRING COMMENT 'Transfer Option Code',
	TRNF_A DECIMAL(18,2) COMMENT 'Transfer Amount',
	CNCY_C STRING COMMENT 'Currency Code',
	CMPE_I STRING COMMENT 'Competitor Identifier',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.BUSN_EVNT (
	EVNT_I STRING NOT NULL COMMENT 'EVENT IDENTIFIER',
	SRCE_SYST_EVNT_I STRING COMMENT 'SOURCE SYSTEM EVENT IDENTIFIER',
	EVNT_ACTL_D DATE COMMENT 'EVENT ACTUAL DATE',
	SRCE_SYST_C STRING NOT NULL COMMENT 'SOURCE SYSTEM CODE',
	PROS_KEY_EFFT_I DECIMAL(10,0) COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'ERROR SEQUENCE IDENTIFIER',
	SRCE_SYST_EVNT_TYPE_I STRING COMMENT 'SOURCE SYSTEM EVENT TYPE IDENTIFIER',
	EVNT_ACTL_T TIME(6),
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL,
	EVNT_ACTV_TYPE_C STRING COMMENT 'Event Activity Type Code',
	EFFT_D DATE COMMENT 'Effective Date',
	EXPY_D DATE COMMENT 'Expiry Date',
	RECORD_DELETED_FLAG NUMBER(3,0) NOT NULL COMMENT 'Record Deleted Flag',
	CTL_ID SMALLINT NOT NULL COMMENT 'Ctl Id',
	PROCESS_NAME STRING NOT NULL COMMENT 'Process Name',
	PROCESS_ID NUMBER(38,0) NOT NULL COMMENT 'Process Id',
	UPDATE_PROCESS_NAME STRING COMMENT 'Update Process Name',
	UPDATE_PROCESS_ID NUMBER(38,0) COMMENT 'Update Process Id'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.CLS_FCLY (
	ACCT_I STRING NOT NULL COMMENT 'ACCOUNT IDENTIFIER',
	LIBL_N STRING COMMENT 'LIABILITY NUMBER',
	SRCE_SYST_ACCT_I STRING COMMENT 'SOURCE SYSTEM ACCOUNT IDENTIFIER',
	SRCE_SYST_PATY_I STRING COMMENT 'SOURCE SYSTEM PARTY IDENTIFIER',
	CIF_ACCT_I STRING COMMENT 'CLIENT INFORMATION FACILITY ACCOUNT IDENTIFIER',
	FCLY_TYPE_C STRING COMMENT 'FACILITY TYPE CODE',
	PDCT_N NUMBER(38,0) COMMENT 'PRODUCT NUMBER',
	CNCY_C STRING COMMENT 'CURRENCY CODE',
	FCLY_A DECIMAL(18,2) COMMENT 'FACILITY AMOUNT',
	STRT_D DATE COMMENT 'START DATE',
	CNCL_D DATE COMMENT 'CANCELLATION DATE',
	FEE_IN_ADVN_F STRING COMMENT 'FEE IN ADVANCE FLAG',
	FEE_SCAL_F STRING COMMENT 'FEE SCALE FLAG',
	CFRM_FEE_F STRING COMMENT 'CONFIRMATION FEE INDICATOR',
	MIN_FEE_CHRG_A DECIMAL(18,2) COMMENT 'MINIMUM FEE CHARGE AMOUNT',
	MAX_FEE_CHRG_A DECIMAL(18,2) COMMENT 'MAXIMUM FEE CHARGE AMOUNT',
	YRLY_CHRG_A DECIMAL(18,2) COMMENT 'YEARLY CHARGE AMOUNT',
	INT_1_R DECIMAL(15,6) COMMENT 'INTEREST RATE 1',
	INT_2_R DECIMAL(15,6) COMMENT 'INTEREST RATE 2',
	IRTE_2_LIMT_A DECIMAL(18,2) COMMENT 'INTEREST RATE 2 LIMIT AMOUNT',
	PYAT_FREQ_C STRING COMMENT 'PAYMENT FREQUENCY CODE',
	LAST_PAYT_D DATE COMMENT 'LAST PAYMENT DATE',
	NEXT_PAYT_D DATE COMMENT 'NEXT PAYMENT DATE',
	PAYT_FI_I STRING COMMENT 'PAYMENT FINANCIAL INSTITUTION IDENTIFIER',
	PAYT_FI_C STRING COMMENT 'PAYMENT FINANCIAL INSTITUTION CODE',
	PAYT_FI_BRCH_N STRING COMMENT 'PAYMENT FINANCIAL INSTITUTION BRANCH NUMBER',
	PAYT_FI_ACCT_N STRING COMMENT 'PAYMENT FINANCIAL INSTITUTION ACCOUNT NUMBER',
	FEE_DUE_A DECIMAL(18,2) COMMENT 'FEE DUE AMOUNT',
	FEE_ACRL_A DECIMAL(18,2) COMMENT 'FEE ACCRUAL AMOUNT',
	CASH_COVR_F STRING COMMENT 'CASH COVERED FLAG',
	CASH_COVR_ACCT_N STRING COMMENT 'CASH COVER ACCOUNT NUMBER',
	EFFT_D DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	EXPY_D DATE NOT NULL COMMENT 'EXPIRY DATE',
	PROS_KEY_EFFT_I DECIMAL(10,0) COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.CLS_UNID_PATY (
	SRCE_SYST_PATY_I STRING NOT NULL COMMENT 'SOURCE SYSTEM PARTY IDENTIFIER',
	SRCE_SYST_C STRING NOT NULL COMMENT 'SOURCE SYSTEM CODE',
	PATY_M STRING COMMENT 'PARTY NAME',
	CRIS_DEBT_I STRING COMMENT 'CUSTOMER RELATIONSHIP INFORMATION SYSTEM DEBTOR IDENTIFIER',
	EFFT_D DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	EXPY_D DATE NOT NULL COMMENT 'EXPIRY DATE',
	PROS_KEY_EFFT_I DECIMAL(10,0) COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DAR_ACCT (
	MERC_ACCT_I STRING NOT NULL COMMENT 'Merchant Identifier',
	SRCE_SYST_MERC_ACCT_I STRING COMMENT 'Source System Merchant Account Identifier',
	SRCE_SYST_MERC_GRUP_I STRING COMMENT 'Source System Merchant Group Identifier',
	MERC_TRAD_M STRING COMMENT 'Merchant Trading Name',
	ACCT_I STRING COMMENT 'Account Identifier',
	ACCT_X STRING COMMENT 'Account Text',
	PDCT_N NUMBER(38,0) COMMENT 'Product Number',
	DAR_STUS_C STRING COMMENT 'DAR Status Code',
	OPEN_D DATE COMMENT 'Open Date',
	CLSE_D DATE COMMENT 'Closed Date',
	OVRD_CRIS_X STRING COMMENT 'Override Cris Text',
	DAR_MERC_CATG_C STRING COMMENT 'Dare Merchant Category code',
	MERC_CLAS_X STRING COMMENT 'Merchant Class Text',
	BRCH_N STRING COMMENT 'Branch Number',
	SERV_REPS_X STRING COMMENT 'Service Representative Text',
	MRKT_REPS_X STRING COMMENT 'Marketing RepresentativeText',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DEPT_APPT (
	DEPT_I STRING NOT NULL COMMENT 'Department Identifier',
	APPT_I STRING NOT NULL COMMENT 'Application Identifier',
	DEPT_ROLE_C STRING NOT NULL COMMENT 'Department Role Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
	BRCH_N SMALLINT COMMENT 'Branch Number',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	CHNG_REAS_C STRING COMMENT 'Change Reason Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_ACCT_PATY (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	ASSC_ACCT_I STRING NOT NULL COMMENT 'Associated Account Identifier',
	PATY_ACCT_REL_C STRING NOT NULL COMMENT 'Party Account Relationship Code',
	PRFR_PATY_F STRING COMMENT 'Preferred Party Flag',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I NUMBER(38,0) NOT NULL COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	PROS_KEY_EXPY_I NUMBER(38,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'ROW SECURITY ACCESS CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_ACCT_REL (
	ACCT_I STRING NOT NULL COMMENT 'Account Identifier',
	INT_GRUP_I STRING NOT NULL COMMENT 'Interest Group Identifier',
	DERV_PRTF_CATG_C STRING COMMENT 'Derived Portfolio Category Code',
	DERV_PRTF_CLAS_C STRING COMMENT 'Derived Portfolio Class Code',
	DERV_PRTF_TYPE_C STRING COMMENT 'Derived Portfolio Type Code',
	VALD_FROM_D DATE COMMENT 'Valid From Date',
	VALD_TO_D DATE COMMENT 'Valid To Date',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PTCL_N SMALLINT COMMENT 'Point Of Control Number',
	REL_MNGE_I STRING COMMENT 'Relationship Manager Identifier',
	PRTF_CODE_X STRING COMMENT 'Portfolio Code Description',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'Row Secuirty Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_OWN_REL (
	INT_GRUP_I STRING NOT NULL COMMENT 'Interest Group Identifier',
	DERV_PRTF_TYPE_C STRING COMMENT 'Derived Portfolio Type Code',
	DERV_PRTF_CATG_C STRING COMMENT 'Derived Portfolio Category Code',
	DERV_PRTF_CLAS_C STRING COMMENT 'Derived Portfolio Class Code',
	VALD_FROM_D DATE COMMENT 'Valid From Date',
	VALD_TO_D DATE COMMENT 'Valid To Date',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PTCL_N SMALLINT COMMENT 'Point Of Control Number',
	REL_MNGE_I STRING COMMENT 'Relationship Manager Identifier',
	PRTF_CODE_X STRING COMMENT 'Portfolio Code Description',
	DERV_PRTF_ROLE_C STRING COMMENT 'Derived Portfolio Role Code',
	ROLE_PLAY_TYPE_X STRING COMMENT 'Role Player Type Description',
	ROLE_PLAY_I STRING NOT NULL COMMENT 'Role Player Identifier',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.DERV_PRTF_PATY_REL (
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	INT_GRUP_I STRING NOT NULL COMMENT 'Interest Group Identifier',
	DERV_PRTF_CATG_C STRING COMMENT 'Derived Portfolio Category Code',
	DERV_PRTF_CLAS_C STRING COMMENT 'Derived Portfolio Class Code',
	DERV_PRTF_TYPE_C STRING COMMENT 'Derived Portfolio Type Code',
	VALD_FROM_D DATE COMMENT 'Valid From Date',
	VALD_TO_D DATE COMMENT 'Valid To Date',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PTCL_N SMALLINT COMMENT 'Point Of Control Number',
	REL_MNGE_I STRING COMMENT 'Relationship Manager Identifier',
	PRTF_CODE_X STRING COMMENT 'Portfolio Code Description',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.EVNT (
	EVNT_I STRING NOT NULL COMMENT 'Event Identifier',
	EVNT_ACTV_TYPE_C STRING COMMENT 'Event Activity Type Code',
	INVT_EVNT_F STRING NOT NULL COMMENT 'Investment Event Indicator',
	FNCL_ACCT_EVNT_F STRING NOT NULL COMMENT 'Financial Account Event Indicator',
	CTCT_EVNT_F STRING NOT NULL COMMENT 'Contact Event Indicator',
	BUSN_EVNT_F STRING NOT NULL COMMENT 'Business Event Indicator',
	PROS_KEY_EFFT_I DECIMAL(10,0) COMMENT 'Process Key Effective Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
	FNCL_NVAL_EVNT_F STRING NOT NULL COMMENT 'Financial Non Value Event Indicator',
	INCD_F STRING NOT NULL COMMENT 'Incident Indicator',
	INSR_EVNT_F STRING NOT NULL COMMENT 'Insurance Event Indicator',
	INSR_NVAL_EVNT_F STRING NOT NULL COMMENT 'Insurance Non Value Event Indicator',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code',
	FNCL_GL_EVNT_F STRING NOT NULL COMMENT 'Financial General Ledger Event Indicator',
	AUTT_AUTN_EVNT_F STRING NOT NULL COMMENT 'Authentication Authorisation Event Indicator',
	COLL_EVNT_F STRING NOT NULL COMMENT 'Collection Event Indicator',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	EVNT_REAS_C STRING COMMENT 'Event Reason Code',
	EFFT_D DATE COMMENT 'Effective Date',
	EXPY_D DATE COMMENT 'Expiry Date',
	RECORD_DELETED_FLAG NUMBER(3,0) NOT NULL COMMENT 'Record Deleted Flag',
	CTL_ID SMALLINT NOT NULL COMMENT 'Ctl Id',
	PROCESS_NAME STRING NOT NULL COMMENT 'Process Name',
	PROCESS_ID NUMBER(38,0) NOT NULL COMMENT 'Process Id',
	UPDATE_PROCESS_NAME STRING COMMENT 'Update Process Name',
	UPDATE_PROCESS_ID NUMBER(38,0) COMMENT 'Update Process Id'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.EVNT_EMPL (
	EVNT_I STRING NOT NULL COMMENT 'EVENT IDENTIFIER',
	EMPL_I STRING NOT NULL COMMENT 'EMPLOYEE IDENTIFIER',
	EVNT_PATY_ROLE_TYPE_C STRING NOT NULL COMMENT 'EVENT PARTY ROLE TYPE CODE',
	EFFT_D DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	EXPY_D DATE NOT NULL COMMENT 'EXPIRY DATE',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'ERROR SEQUENCE IDENTIFIER',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.EVNT_INT_GRUP (
	EVNT_I STRING NOT NULL COMMENT 'Event Identifier',
	INT_GRUP_I STRING NOT NULL COMMENT 'Interest Group Identifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.GDW_EFFT_DATE (
	GDW_EFFT_D DATE NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.INT_GRUP (
	INT_GRUP_I STRING NOT NULL COMMENT 'Interest Group Identifier',
	INT_GRUP_TYPE_C STRING NOT NULL COMMENT 'Event Group Type Code',
	SRCE_SYST_INT_GRUP_I STRING NOT NULL COMMENT 'Source System Interest Group Identifier',
	SRCE_SYST_C STRING NOT NULL COMMENT 'Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
	INT_GRUP_M STRING COMMENT 'INTEREST GROUP NAME',
	ORIG_SRCE_SYST_INT_GRUP_I STRING COMMENT 'ORIGINAL SOURCE SYSTEM INTEREST GROUP IDENTIFIER',
	CRAT_D DATE COMMENT 'CREATED DATE',
	QLFY_C STRING COMMENT 'Qualifier Code',
	PTCL_N STRING COMMENT 'Point Of Control Number',
	REL_MNGE_I STRING COMMENT 'Relationship Manager Identifier',
	VALD_TO_D DATE COMMENT 'Valid To Date',
	ROW_SECU_ACCS_C NUMBER(38,0) COMMENT 'Row Security Access Code',
	INT_GRUP_CATG_C STRING COMMENT 'Interest Group Category Code',
	ISO_CNTY_C STRING COMMENT 'ISO Country Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.INT_GRUP_DEPT (
	INT_GRUP_I STRING NOT NULL COMMENT 'Interest Group Identifier',
	DEPT_I STRING NOT NULL COMMENT 'Department Identifier',
	DEPT_ROLE_C STRING NOT NULL COMMENT 'Department Role Code',
	SRCE_SYST_C STRING COMMENT 'Source System Code',
	VALD_FROM_D DATE COMMENT 'Valid From Date',
	VALD_TO_D DATE COMMENT 'Valid To Date',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	ROW_SECU_ACCS_C NUMBER(38,0) NOT NULL COMMENT 'Row Security Access Code'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.INT_GRUP_EMPL (
	INT_GRUP_I STRING NOT NULL COMMENT 'Interest Group Identifier',
	EMPL_I STRING NOT NULL COMMENT 'Employee Identifier',
	EMPL_ROLE_C STRING NOT NULL COMMENT 'Employee Role Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identfier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier',
	REL_C STRING COMMENT 'Relationship Code',
	VALD_FROM_D DATE COMMENT 'Valid From Date',
	VALD_TO_D DATE COMMENT 'Valid To Date',
	ROW_SECU_ACCS_C NUMBER(38,0) COMMENT 'Row Security Access Code',
	SRCE_SYST_C STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.INT_GRUP_UNID_PATY (
	INT_GRUP_I STRING NOT NULL COMMENT 'Interest Group Identifier',
	SRCE_SYST_PATY_I STRING NOT NULL COMMENT 'Source System Party Identifier',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	ORIG_SRCE_SYST_PATY_I STRING COMMENT 'Original Source System Party Identifier',
	SRCE_SYST_C STRING NOT NULL COMMENT 'Source System Code',
	UNID_PATY_M STRING COMMENT 'Unidentified Party Name',
	PATY_TYPE_C STRING NOT NULL COMMENT 'Party Type Code',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Process Key Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ADRS_TYPE (
	ADRS_TYPE_ID STRING,
	PYAD_TYPE_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_ACQR_SRCE (
	PL_MRKT_SRCE_CAT_ID STRING,
	ACQR_SRCE_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_C (
	CCL_APP_CAT_ID STRING,
	APPT_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_CMPE (
	BAL_XFER_INSN_CAT_ID STRING,
	CMPE_I STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_CODE_HM (
	HLM_APPT_TYPE_CATG_I STRING NOT NULL COMMENT 'Hlm Application Type Category Identifier',
	APPT_C STRING COMMENT 'Application Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_COND (
	COND_APPT_CAT_ID STRING,
	APPT_COND_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_DOCU_DELY (
	EXEC_DOCU_RECV_TYPE STRING NOT NULL COMMENT 'Executive Documents Receiver Type',
	DOCU_DELY_RECV_C STRING COMMENT 'Document Delivery Receiver Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_FEAT (
	MAP_TYPE_C STRING NOT NULL,
	TARG_NUMC_C NUMBER(38,0),
	TARG_CHAR_C STRING,
	SRCE_NUMC_1_C NUMBER(38,0),
	SRCE_CHAR_1_C STRING,
	SRCE_NUMC_2_C NUMBER(38,0),
	SRCE_CHAR_2_C STRING,
	SRCE_NUMC_3_C NUMBER(38,0),
	SRCE_CHAR_3_C STRING,
	SRCE_NUMC_4_C NUMBER(38,0),
	SRCE_CHAR_4_C STRING,
	SRCE_NUMC_5_C NUMBER(38,0),
	SRCE_CHAR_5_C STRING,
	SRCE_NUMC_6_C NUMBER(38,0),
	SRCE_CHAR_6_C STRING,
	SRCE_NUMC_7_C NUMBER(38,0),
	SRCE_CHAR_7_C STRING,
	EFFT_D DATE NOT NULL,
	EXPY_D DATE NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_FORM (
	CCL_FORM_CAT_ID STRING,
	APPT_FORM_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_ORIG (
	CHNL_CAT_ID STRING,
	APPT_ORIG_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_PDCT_FEAT (
	PEXA_FLAG STRING NOT NULL COMMENT 'Pexa Flag',
	FEAT_VALU_C STRING COMMENT 'Feature Value Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_PDCT_PATY_ROLE (
	ROLE_CAT_ID STRING,
	PATY_ROLE_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_PURP_CL (
	CCL_LOAN_PURP_CAT_ID STRING,
	PURP_TYPE_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_PURP_CLAS_CL (
	LOAN_PURP_CLAS_CODE STRING,
	PURP_CLAS_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_PURP_HL (
	HL_LOAN_PURP_CAT_ID STRING,
	PURP_TYPE_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_PURP_PL (
	PL_PROD_PURP_CAT_ID STRING,
	PURP_TYPE_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_QLFY (
	SBTY_CODE STRING,
	APPT_QLFY_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_QSTN_HL (
	QA_QUESTION_ID STRING NOT NULL,
	QSTN_C STRING,
	EFFT_D DATE NOT NULL,
	EXPY_D DATE NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_APPT_QSTN_RESP_HL (
	QA_ANSWER_ID STRING NOT NULL,
	RESP_C STRING,
	EFFT_D DATE NOT NULL,
	EXPY_D DATE NOT NULL
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_CMPE_IDNN (
	INSN_ID STRING,
	CMPE_I STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_CNTY (
	DOCU_COLL_CNTY_ID STRING,
	ISO_CNTY_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_CRIS_PDCT (
	CRIS_PDCT_ID STRING NOT NULL COMMENT 'Customer Relationship Information SYSTEM Product Identifier',
	ACCT_QLFY_C STRING COMMENT 'ACCOUNT Qualifier Code',
	SRCE_SYST_C STRING COMMENT 'SOURCE SYSTEM Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective DATE',
	EXPY_D DATE NOT NULL COMMENT 'Expiry DATE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_DOCU_METH (
	DOCU_COLL_CAT_ID STRING,
	DOCU_DELY_METH_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ENV_CHLD_PATY_REL (
	FA_CHLD_STAT_CAT_ID STRING,
	REL_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ENV_EVNT_ACTV_TYPE (
	FA_ENV_EVNT_CAT_ID STRING,
	EVNT_ACTV_TYPE_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ENV_PATY_REL (
	CLNT_RELN_TYPE_ID STRING,
	CLNT_POSN STRING,
	REL_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ENV_PATY_TYPE (
	FA_ENTY_CAT_ID STRING,
	PATY_TYPE_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_FEAT_OVRD_REAS_HL (
	HL_PROD_INT_MARG_CAT_ID STRING,
	OVRD_REAS_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_FEAT_OVRD_REAS_HL_D (
	HL_FEE_DISCOUNT_CAT_ID STRING,
	OVRD_REAS_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_FEAT_OVRD_REAS_PL (
	MARG_REAS_CAT_ID STRING,
	OVRD_REAS_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_FEE_CAPL (
	PL_CAPL_FEE_CAT_ID STRING,
	FEE_CAPL_F STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_FNDD_METH (
	FNDD_METH_CAT_ID STRING,
	FNDD_INSS_METH_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_JOB_COMM_CATG (
	CLP_JOB_FAMILY_CAT_ID STRING,
	JOB_COMM_CATG_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_LOAN_FNDD_METH (
	PL_TARG_CAT_ID STRING,
	LOAN_FNDD_METH_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_LOAN_TERM_PL (
	PL_PROD_TERM_CAT_ID NUMBER(38,0),
	ACTL_VALU_Q DECIMAL(15,6),
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_LPC_DEPT_HL (
	LPC_OFFICE STRING,
	DEPT_I STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ORIG_APPT_SRCE (
	ORIG_SRCE_SYST_I STRING NOT NULL COMMENT 'Original Source System Identifier',
	ORIG_APPT_SRCE_C STRING COMMENT 'Original Application Source Code',
	EFFT_D DATE COMMENT 'Effective Date',
	EXPY_D DATE COMMENT 'Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_ORIG_APPT_SRCE_HM (
	HL_BUSN_CHNL_CAT_I NUMBER(38,0) NOT NULL COMMENT 'Homeloan Business Channel Cat Id',
	ORIG_APPT_SRCE_SYST_C STRING COMMENT 'Original Application Source System Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_OVRD_FEE_FRQ_CL (
	OVRD_FEE_PCT_FREQ STRING,
	FREQ_IN_MTHS DECIMAL(15,6),
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_PACK_PDCT_HL (
	HL_PACK_CAT_ID STRING,
	PDCT_N NUMBER(38,0),
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_PACK_PDCT_PL (
	PL_PACK_CAT_ID STRING,
	PDCT_N NUMBER(38,0),
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_PAYT_FREQ (
	HL_RPAY_PERD_CAT_ID STRING,
	PAYT_FREQ_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_PDCT_REL_CL (
	PARN_PDCT_LVL_CAT_ID STRING,
	CHLD_PDCT_LVL_CAT_ID STRING,
	REL_TYPE_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_PL_ACQR_TYPE (
	PL_CMPN_CAT_ID STRING,
	ACQR_TYPE_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_SM_CASE_STUS (
	SM_STAT_CAT_ID STRING,
	STUS_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_SM_CASE_STUS_REAS (
	SM_REAS_CAT_ID STRING,
	STUS_REAS_TYPE_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_STATE (
	STATE_ID STRING,
	STAT_X STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_TRNF_OPTN (
	BAL_XFER_OPTN_CAT_ID STRING,
	TRNF_OPTN_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_TU_APPT_C (
	SBTY_CODE STRING,
	APPT_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MAP_CSE_UNID_PATY_CATG_PL (
	TP_BROK_GRUP_CAT_ID STRING,
	UNID_PATY_CATG_C STRING,
	EFFT_D DATE,
	EXPY_D DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MOS_FCLY (
	FCLY_I STRING NOT NULL COMMENT 'FACILITY IDENTIFIER',
	SRCE_SYST_PATY_I STRING COMMENT 'SOURCE SYSTEM PARTY IDENTIFIER',
	BRCH_C STRING COMMENT 'BRANCH CODE',
	APPT_C STRING COMMENT 'APPLICATION CODE',
	CRAT_D DATE COMMENT 'CREATION DATE',
	ASET_LIBL_C STRING COMMENT 'ASSET LIABILITY CODE',
	STUS_C STRING COMMENT 'STATUS CODE',
	CNCY_C STRING COMMENT 'CURRENCY CODE',
	ORIG_A DECIMAL(18,2) COMMENT 'ORIGINAL AMOUNT',
	CURR_BALN_A DECIMAL(18,2) COMMENT 'CURRENT BALANCE AMOUNT',
	ISSU_D DATE COMMENT 'ISSUE DATE',
	MTUR_D DATE COMMENT 'MATURITY DATE',
	ORIG_ISSU_D DATE COMMENT 'ORIGINAL ISSUE DATE',
	EFFT_D DATE COMMENT 'EFFECTIVE DATE',
	EXPY_D DATE COMMENT 'EXPIRY DATE',
	PROS_KEY_EFFT_I DECIMAL(10,0) COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.MOS_LOAN (
	LOAN_I STRING NOT NULL COMMENT 'LOAN IDENTIFIER',
	FCLY_I STRING COMMENT 'FACILITY IDENTIFIER',
	MOS_APPT_C STRING COMMENT 'MOS APPLICATION CODE',
	MOS_ASET_LIBL_C STRING COMMENT 'MOS ASET LIABILITY CODE',
	MOS_STUS_C STRING COMMENT 'MOS STATUS CODE',
	ACRL_X STRING COMMENT 'MIDAS OFFSHORE ACCRUAL DESCRIPTION',
	TRAD_CNCY_ORIG_A DECIMAL(18,2) COMMENT 'TRADING CURRENCY ORIGINAL AMOUNT',
	TRAD_CNCY_SETL_A DECIMAL(18,2) COMMENT 'TRADING CURRENCY SETTLEMENTAMOUNT',
	TRAD_CNCY_CURR_BALN_A DECIMAL(18,2) COMMENT 'TRADING CURRENCY CURRENTBALANCE AMOUNT',
	INT_R DECIMAL(18,8) COMMENT 'INTEREST RATE',
	FIX_INT_R DECIMAL(18,8) COMMENT 'FIXED INTEREST RATE',
	VARY_INT_R DECIMAL(18,8) COMMENT 'VARIABLE INTEREST RATE',
	ISSU_D DATE COMMENT 'ISSUE DATE',
	MTUR_D DATE COMMENT 'MATURITY DATE',
	ROLV_D DATE COMMENT 'ROLLOVER DATE',
	INT_PAYT_FREQ_C STRING COMMENT 'PAYMENT FREQUENCY CODE',
	NEXT_INT_D DATE COMMENT 'NEXT INTEREST DATE',
	TRAD_CNCY_ACRE_A DECIMAL(18,2) COMMENT 'TRADING CURRENCY ACCRUED AMOUNT',
	LAST_INT_PAYT_D DATE COMMENT 'LAST INTEREST PAYMENTDATE',
	TRAD_CNCY_TOTL_INT_RECV_A DECIMAL(18,2) COMMENT 'TRADING CURRENCY TOTALINTEREST RECEIVED AMOUNT',
	PDCT_N NUMBER(38,0) COMMENT 'PRODUCT NUMBER',
	TRAD_CNCY_C STRING COMMENT 'CURRENCY CODE',
	BASE_CNCY_C STRING COMMENT 'CURRENCY CODE',
	TRAD_CNCY_INT_A DECIMAL(18,2) COMMENT 'TRADING CURRENCY INTEREST AMOUNT',
	AUD_CNCY_INT_A DECIMAL(18,2) COMMENT 'AUSTRALIAN DOLLAR CURRENCYINTEREST AMOUNT',
	BASE_CNCY_INT_A DECIMAL(18,2) COMMENT 'BASE CURRENCY INTEREST AMOUNT',
	TRAD_CNCY_AVRG_BALN_A DECIMAL(18,2) COMMENT 'TRADING CURRENCY AVERAGEBALANCE AMOUNT',
	AUD_CNCY_AVRG_BALN_A DECIMAL(18,2) COMMENT 'AUSTRALIAN DOLLAR CURRENCYAVERAGE BALANCE AMOUNT',
	BASE_CNCY_AVRG_BALN_A DECIMAL(18,2) COMMENT 'BASE CURRENCY AVERAGE BALANCEAMOUNT',
	TRAD_CNCY_MARG_A DECIMAL(18,2) COMMENT 'TRADING CURRENCY MARGIN AMOUNT',
	AUD_CNCY_MARG_A DECIMAL(18,2) COMMENT 'AUSTRALIAN DOLLAR CURRENCY MARGINAMOUNT',
	BASE_CNCY_MARG_A DECIMAL(18,2) COMMENT 'BASE CURRENCY MARGIN AMOUNT',
	AVRG_MARG_R DECIMAL(18,8) COMMENT 'AVERAGE MARGIN RATE',
	DEPT_I STRING COMMENT 'DEPARTMENT IDENTIFIER',
	MSA_BALN_GL_ACCT_I STRING COMMENT 'MSA BALANCE GENERAL LEDGERACCOUNT IDENTIFIER',
	MSA_INT_GL_ACCT_I STRING COMMENT 'MSA INTEREST GENERAL LEDGERACCOUNT IDENTIFIER',
	EFFT_D DATE NOT NULL COMMENT 'EFFECTIVEDATE',
	EXPY_D DATE NOT NULL COMMENT 'EXPIRY DATE',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'PROCESS KEY EFFECTIVEIDENTIFIER',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER',
	PROF_CNTR_CODE_X STRING COMMENT 'Profit Centre Code Description'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.PATY_APPT_PDCT (
	PATY_I STRING NOT NULL COMMENT 'Party Identifier',
	APPT_PDCT_I STRING NOT NULL COMMENT 'Application Product Identifier',
	PATY_ROLE_C STRING NOT NULL COMMENT 'Party Role Code',
	EFFT_D DATE NOT NULL COMMENT 'Effective Date',
	SRCE_SYST_C STRING NOT NULL COMMENT 'Source System Code',
	SRCE_SYST_APPT_PDCT_PATY_I STRING NOT NULL COMMENT 'Source System Application Product Party Identifier',
	EXPY_D DATE NOT NULL COMMENT 'Expiry Date',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'Process Key Effective Identifier',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'Source System Expiry Identifier',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'Error Sequence Identifier'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.PATY_INT_GRUP (
	INT_GRUP_I STRING NOT NULL COMMENT 'INTEREST GROUP IDENTIFIER',
	PATY_I STRING NOT NULL COMMENT 'PARTY IDENTIFIER',
	EFFT_D DATE NOT NULL COMMENT 'EFFECTIVE DATE',
	EXPY_D DATE NOT NULL COMMENT 'EXPIRY DATE',
	SRCE_SYST_C STRING NOT NULL COMMENT 'SOURCE SYSTEM CODE',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER',
	EROR_SEQN_I DECIMAL(10,0) COMMENT 'ERROR SEQUENCE IDENTIFIER',
	PRIM_CLNT_F STRING COMMENT 'PRIMARY CLIENT FLAG',
	REL_I STRING COMMENT 'RELATIONSHIP IDENTIFIER',
	SRCE_SYST_PATY_INT_GRUP_I STRING COMMENT 'SOURCE SYSTEM PARTY INTEREST GROUP IDENTIFIER',
	ORIG_SRCE_SYST_PATY_TYPE_C STRING COMMENT 'ORIGINAL SOURCE SYSTEM PARTY TYPE CODE',
	ORIG_SRCE_SYST_PATY_I STRING COMMENT 'ORIGINAL SOURCE SYSTEM PARTY IDENTIFIER',
	REL_C STRING COMMENT 'RELATIONSHIP CODE',
	VALD_FROM_D DATE COMMENT 'VALID FROM DATE',
	VALD_TO_D DATE COMMENT 'VALID TO DATE',
	ROW_SECU_ACCS_C NUMBER(38,0) COMMENT 'ROW SECURITY ACCESS CODE',
	PRIM_CLNT_SLCT_C STRING COMMENT 'PRIMARY CLIENT SELECTION CODE'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.THA_ACCT (
	THA_ACCT_I STRING NOT NULL COMMENT 'THALER ACCOUNT IDENTIFIER',
	ACCT_QLFY_C STRING COMMENT 'ACCOUNT QUALIFIER CODE',
	EXT_D DATE COMMENT 'EXTRACT DATE',
	CSL_CLNT_I STRING COMMENT 'COMMSEC CLIENT IDENTIFIER',
	TRAD_ACCT_I STRING COMMENT 'TRADE ACCOUNT IDENTIFIER',
	THA_ACCT_TYPE_C STRING COMMENT 'THALER ACCOUNT TYPE CODE',
	THA_ACCT_STUS_C STRING COMMENT 'THALER ACCOUNT STATUS CODE',
	PALL_BUSN_UNIT_I STRING COMMENT 'PRE ALLOCATION BUSINESS UNIT IDENTIFIER',
	PALL_DEPT_I STRING COMMENT 'PRE ALLOCATION DEPARTMENT IDENTIFIER',
	BALN_A DECIMAL(15,2) COMMENT 'BALANCE AMOUNT',
	IACR_MTD_A DECIMAL(15,2) COMMENT 'INTEREST ACCRUED MONTH TO DATE',
	IACR_FYTD_A DECIMAL(15,2) COMMENT 'INTEREST ACCRUED FINANCIAL YEAR TO DATE',
	DALY_AGGR_FEE_A DECIMAL(15,2) COMMENT 'DAILY AGGREGATE FEE AMOUNT',
	BALN_D DATE COMMENT 'BALANCE DATE',
	EFFT_D DATE COMMENT 'EFFECTIVE DATE',
	EXPY_D DATE COMMENT 'EXPIRY DATE',
	PROS_KEY_EFFT_I DECIMAL(10,0) NOT NULL COMMENT 'PROCESS KEY EFFECTIVE IDENTIFIER',
	PROS_KEY_EXPY_I DECIMAL(10,0) COMMENT 'PROCESS KEY EXPIRY IDENTIFIER'
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.UTIL_BTCH_ISAC (
	BTCH_KEY_I DECIMAL(10,0) NOT NULL,
	BTCH_RQST_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STARCADPRODDATA.UTIL_BTCH_ISAC NOT SUPPORTED BTCH_RUN_D DATE NOT NULL,
	SRCE_SYST_M STRING NOT NULL,
	BTCH_STUS_C STRING,
	STUS_CHNG_S TIMESTAMP_NTZ(0) -- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STARCADPRODDATA.UTIL_BTCH_ISAC NOT SUPPORTED
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.UTIL_ETI_CONV (
	CONV_M STRING NOT NULL,
	APPT_C STRING,
	DEVL_I STRING,
	LOGN_USER_M STRING,
	CONV_STUS_C STRING,
	LAST_MODF_D DATE,
	LAST_GENR_D DATE,
	META_STOR_M STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.UTIL_PARM (
	PARM_M STRING NOT NULL,
	PARM_LTRL_N NUMBER(38,0),
	PARM_LTRL_D DATE,
	PARM_LTRL_STRG_X STRING,
	PARM_LTRL_A DECIMAL(18,4)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.STARCADPRODDATA.UTIL_PROS_ISAC (
	PROS_KEY_I DECIMAL(10,0) NOT NULL,
	CONV_M STRING,
	CONV_TYPE_M STRING,
	PROS_RQST_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STARCADPRODDATA.UTIL_PROS_ISAC NOT SUPPORTED PROS_LAST_RQST_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STARCADPRODDATA.UTIL_PROS_ISAC NOT SUPPORTED PROS_RQST_Q NUMBER(38,0),
	BTCH_RUN_D DATE,
	BTCH_KEY_I DECIMAL(10,0),
	SRCE_SYST_M STRING,
	SRCE_M STRING,
	TRGT_M STRING,
	SUCC_F STRING,
	COMT_F STRING,
	COMT_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STARCADPRODDATA.UTIL_PROS_ISAC NOT SUPPORTED MLTI_LOAD_EFFT_D DATE,
	SYST_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STARCADPRODDATA.UTIL_PROS_ISAC NOT SUPPORTED MLTI_LOAD_COMT_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STARCADPRODDATA.UTIL_PROS_ISAC NOT SUPPORTED SYST_ET_Q NUMBER(38,0),
	SYST_UV_Q NUMBER(38,0),
	SYST_INS_Q NUMBER(38,0),
	SYST_UPD_Q NUMBER(38,0),
	SYST_DEL_Q NUMBER(38,0),
	SYST_ET_TABL_M STRING,
	SYST_UV_TABL_M STRING,
	SYST_HEAD_ET_TABL_M STRING,
	SYST_HEAD_UV_TABL_M STRING,
	SYST_TRLR_ET_TABL_M STRING,
	SYST_TRLR_UV_TABL_M STRING,
	PREV_PROS_KEY_I DECIMAL(10,0),
	HEAD_RECD_TYPE_C STRING,
	HEAD_FILE_M STRING,
	HEAD_BTCH_RUN_D DATE,
	HEAD_FILE_CRAT_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STARCADPRODDATA.UTIL_PROS_ISAC NOT SUPPORTED HEAD_GENR_PRGM_M STRING,
	HEAD_BTCH_KEY_I DECIMAL(10,0),
	HEAD_PROS_KEY_I DECIMAL(10,0),
	HEAD_PROS_PREV_KEY_I DECIMAL(10,0),
	TRLR_RECD_TYPE_C STRING,
	TRLR_RECD_Q NUMBER(38,0),
	TRLR_HASH_TOTL_A DECIMAL(18,4),
	TRLR_COLM_HASH_TOTL_M STRING,
	TRLR_EROR_RECD_Q NUMBER(38,0),
	TRLR_FILE_COMT_S TIMESTAMP_NTZ(0),
	-- FORMAT 'yyyy-mm-ddbhh:mi:ss' - FORMAT IN TABLE STARCADPRODDATA.UTIL_PROS_ISAC NOT SUPPORTED TRLR_RECD_ISRT_Q NUMBER(38,0),
	TRLR_RECD_UPDT_Q NUMBER(38,0),
	TRLR_RECD_DELT_Q NUMBER(38,0)
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.SysAdmin.TPUMPSTATUSTBL (
	LOGDB STRING NOT NULL,
	LOGTABLE STRING NOT NULL,
	IMPORT NUMBER(38,0) NOT NULL,
	USERNAME STRING NOT NULL,
	INITSTARTDATE DATE,
	INITSTARTTIME FLOAT,
	--- DEFAULT CURRENT_TIME NOT VALID FOR DATA TYPE FLOAT CurrStartDate DATE,
	CURRSTARTTIME FLOAT,
	-- DEFAULT CURRENT_TIME NOT VALID FOR DATA TYPE FLOAT LastUpdateDate DATE,
	LASTUPDATETIME FLOAT,
	-- DEFAULT CURRENT_TIME NOT VALID FOR DATA TYPE FLOAT RestartCount NUMBER(38,0) NOT NULL,
	COMPLETE STRING,
	STMTSLAST NUMBER(38,0),
	RECORDSOUT NUMBER(38,0),
	RECORDSSKIPPED NUMBER(38,0),
	RECORDSREJCTED NUMBER(38,0),
	RECORDSREAD NUMBER(38,0),
	RECORDSERRORED NUMBER(38,0),
	STMTSUNLIMITED STRING NOT NULL,
	STMTSDESIRED NUMBER(38,0),
	SESSNDESIRED NUMBER(38,0),
	TASKSDESIRED NUMBER(38,0),
	PERIODSDESIRED NUMBER(38,0),
	PLEASEPAUSE STRING,
	PLEASEABORT STRING,
	REQUESTCHANGE STRING NOT NULL,
	REQUESTACTION STRING NOT NULL,
	LOGONSOURCE STRING,
	LOADPID STRING,
	NUMCHILDREN SMALLINT,
	CHILDRENLIST STRING,
	PARENTNODE STRING
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;
create iceberg table NPD_D12_DMN_GDWMIG_IBRG.Sys_Calendar.CALDATES (
	CDATE DATE
)COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"teradata\",  \"convertedOn\": \"07/11/2025\",  \"domain\": \"snowflake\" }}'
;