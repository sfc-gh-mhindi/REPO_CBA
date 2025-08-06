{{ config(materialized='incremental', alias='tmp_aset_adrs', incremental_strategy='append', tags=['LdTMP_ASET_ADRSTrmXfm']) }}

SELECT
	ASET_I
	ADRS_I
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	EROR_SEQN_I
	RUN_STRM
	CHL_PRCP_SCUY_FLAG 
FROM {{ ref('TgtTmp_Asset_AdrsDS') }}