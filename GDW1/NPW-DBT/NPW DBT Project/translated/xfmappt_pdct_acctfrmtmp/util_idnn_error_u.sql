{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_util__acct__idnn__eror__u__cse__com__cpo__bus__ncpr__clnt__20110127', incremental_strategy='insert_overwrite', tags=['XfmAppt_Pdct_AcctFrmTmp']) }}

SELECT
	EROR_ACCT_NUMB,
	PROS_KEY_EFFT_I,
	EXPY_DATE,
	PROS_KEY_EXPY_I 
FROM {{ ref('Funnel_Update') }}