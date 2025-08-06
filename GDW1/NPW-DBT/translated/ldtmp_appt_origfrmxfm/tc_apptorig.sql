{{ config(materialized='incremental', alias='ddnikestg.tmp_appt_orig', incremental_strategy='append', tags=['LdTMP_APPT_ORIGFrmXfm']) }}

SELECT
	APPT_I
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	ROW_SECU_ACCS_C
	SRCE_SYST_C
	APPT_ORIG_C
	APPT_ORIG_CATG_C 
FROM {{ ref('Fnl') }}