{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_tmp__cse__com__cpo__bus__ncpr__clnt__tmp__acct__pdct__appt', incremental_strategy='insert_overwrite', tags=['XfmAppt_Pdct_AcctFrmTmp']) }}

SELECT
	APPT_PDCT_I,
	ACCT_I,
	REL_TYPE_C,
	EROR_SEQN_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	RUN_STRM 
FROM {{ ref('Const_Acct_I__ACCT_PDCT_APPT') }}