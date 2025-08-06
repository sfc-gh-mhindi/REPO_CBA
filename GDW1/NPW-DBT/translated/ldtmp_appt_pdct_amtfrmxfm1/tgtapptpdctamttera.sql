{{ config(materialized='incremental', alias='tmp_appt_pdct_amt', incremental_strategy='append', tags=['LdTMP_APPT_PDCT_AMTFrmXfm1']) }}

SELECT
	APPT_PDCT_I
	AMT_TYPE_C
	EFFT_D
	EXPY_D
	CNCY_C
	APPT_PDCT_A
	SRCE_SYST_C
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM
	DLTA_VERS 
FROM {{ ref('SrcApptPdctAmtDS') }}