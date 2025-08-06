{{ config(materialized='incremental', alias='util_trsf_eror_rqm3', incremental_strategy='append', tags=['LdUtilTrsfEror_Rqm3Ins']) }}

SELECT
	SRCE_KEY_I
	CONV_M
	CONV_MAP_RULE_M
	TRSF_TABL_M
	SRCE_EFFT_D
	VALU_CHNG_BFOR_X
	VALU_CHNG_AFTR_X
	TRSF_X
	TRSF_COLM_M
	EROR_SEQN_I
	SRCE_FILE_M
	PROS_KEY_EFFT_I
	TRSF_KEY_I 
FROM {{ ref('UTIL_TRSF_EROR_RQM3_I') }}