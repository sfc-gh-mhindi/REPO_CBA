{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__amt__i__cse__com__cpo__bus__ncpr__clnt__20101015', incremental_strategy='insert_overwrite', tags=['DltAppt_Pdct_AmtFrmTMP']) }}

SELECT
	APPT_PDCT_I,
	AMT_TYPE_C,
	CNCY_C,
	APPT_PDCT_A,
	XCES_AMT_REAS_X,
	EFFT_D,
	SRCE_SYST_C,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('Update__Appt_Pdct_Amt_Ins') }}