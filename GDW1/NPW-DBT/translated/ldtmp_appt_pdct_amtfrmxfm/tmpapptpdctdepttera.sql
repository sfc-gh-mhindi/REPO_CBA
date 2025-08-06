{{ config(materialized='incremental', alias='tmp_appt_pdct_amt', incremental_strategy='append', tags=['LdTmp_Appt_Pdct_AmtFrmXfm']) }}

SELECT
	APPT_PDCT_I
	AMT_TYPE_C
	CNCY_C
	APPT_PDCT_A
	XCES_AMT_REAS_X
	EFFT_D
	SRCE_SYST_C
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM
	DLTA_VERS 
FROM {{ ref('TgtAppt_Pdct') }}