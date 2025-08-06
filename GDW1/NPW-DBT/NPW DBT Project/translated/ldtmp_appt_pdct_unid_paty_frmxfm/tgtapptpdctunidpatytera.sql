{{ config(materialized='incremental', alias='tmp_appt_pdct_unid_paty', incremental_strategy='append', tags=['LdTMP_APPT_PDCT_UNID_PATY_FrmXfm']) }}

SELECT
	APPT_PDCT_I
	PATY_ROLE_C
	SRCE_SYST_PATY_I
	EFFT_D
	SRCE_SYST_C
	UNID_PATY_CATG_C
	PATY_M
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM 
FROM {{ ref('Copy_89') }}