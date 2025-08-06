{{ config(materialized='incremental', alias='tdcsdstg.tmp_fa_prop_clnt', incremental_strategy='append', tags=['LdTMP_FA_PROP_CLNTFrmXfm']) }}

SELECT
	FA_PROP_CLNT_ID
	COIN_ENTY_ID
	CLNT_CORL_ID
	COIN_ENTY_NAME
	FA_ENTY_CAT_ID
	FA_UTAK_ID
	FA_PROP_CLNT_CAT_ID
	ORIG_ETL_D
	CHNG_CODE
	INT_GRUP_I
	SRCE_SYST_PATY_I
	ORIG_SRCE_SYST_PATY_I
	UNID_PATY_M
	PATY_TYPE_C
	SRCE_SYST_C 
FROM {{ ref('SrcFAPropClntDS') }}