{{ config(materialized='incremental', alias='appt_pdct_cond', incremental_strategy='append', tags=['Ld_APPT_PDCT_COND_Ins']) }}

SELECT
	APPT_PDCT_I
	COND_C
	EFFT_D
	EXPY_D
	APPT_PDCT_COND_MEET_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtAPPT_PDCT_CONDInsertDS') }}