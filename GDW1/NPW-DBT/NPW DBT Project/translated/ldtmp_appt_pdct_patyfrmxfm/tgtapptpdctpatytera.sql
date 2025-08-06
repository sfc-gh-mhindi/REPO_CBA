{{ config(materialized='incremental', alias='tmp_appt_pdct_paty', incremental_strategy='append', tags=['LdTMP_APPT_PDCT_PATYFrmXfm']) }}

SELECT
	APPT_PDCT_I
	PATY_I
	PATY_ROLE_C
	EFFT_D
	SRCE_SYST_C
	SRCE_SYST_APPT_PDCT_PATY_I
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM 
FROM {{ ref('SrcApptPdctPatyDS') }}