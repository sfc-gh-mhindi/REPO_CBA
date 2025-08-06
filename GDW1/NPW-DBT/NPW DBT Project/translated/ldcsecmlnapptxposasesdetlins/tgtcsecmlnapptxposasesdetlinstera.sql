{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='append', tags=['LdCseCmlnApptXposAsesDetlIns']) }}

SELECT
	APPT_I
	XPOS_A
	XPOS_AMT_D
	OVRD_COVTS_ASES_F
	CSE_CMLN_OVRD_REAS_CATG_C
	SHRT_DFLT_OVRD_F
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtCseCmlnApptXposAsesDetlInsertDS') }}