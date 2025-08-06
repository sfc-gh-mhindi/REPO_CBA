{{ config(materialized='incremental', alias='tmp_appt_pdct_rel', incremental_strategy='append', tags=['LdTMP_APPT_PDCT_RELFrmXfm']) }}

SELECT
	APPT_PDCT_I
	RELD_APPT_PDCT_I
	EFFT_D
	REL_TYPE_C
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM 
FROM {{ ref('SrcApptPdctRelDS') }}