{{ config(materialized='incremental', alias='tmp_appt_rel', incremental_strategy='append', tags=['LdTMP_APPT_REL_LPFrmXfm']) }}

SELECT
	APPT_I
	RELD_APPT_I
	REL_TYPE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM 
FROM {{ ref('Transformer_90') }}