{{ config(materialized='incremental', alias='util_trsf_eror_r2', incremental_strategy='append', tags=['LdUTIL_TRSF_EROR_R2_OD_Ins']) }}

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
FROM {{ ref('Generate_Sequence') }}