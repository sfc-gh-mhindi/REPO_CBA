{{ config(materialized='incremental', alias='cse4_tmp_adrs', incremental_strategy='append', tags=['LdTMP_ADRSTrmXfm']) }}

SELECT
	ADRS_I
	ADRS_TYPE_C
	SRCE_SYST_C
	ADRS_QLFY_C
	SRCE_SYST_ADRS_I
	SRCE_SYST_ADRS_SEQN_N
	RUN_STRM 
FROM {{ ref('TgtTmp_AdrsDS') }}