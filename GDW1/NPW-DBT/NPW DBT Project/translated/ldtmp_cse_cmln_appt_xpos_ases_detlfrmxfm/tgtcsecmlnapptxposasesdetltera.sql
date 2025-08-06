{{ config(materialized='incremental', alias='tmp_cse_cmln_appt_xpos_ases_detl', incremental_strategy='append', tags=['LdTMP_CSE_CMLN_APPT_XPOS_ASES_DETLFrmXfm']) }}

SELECT
	APPT_I
	XPOS_A
	XPOS_AMT_D
	OVRD_COVTS_ASES_F
	CSE_CMLN_OVRD_REAS_CATG_C
	SHRT_DFLT_OVRD_F
	EFFT_D
	EXPY_D 
FROM {{ ref('TmpCclCseCmlnApptXposAsesDetll') }}