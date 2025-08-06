{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_appt__qstn__i__cse__com__cpo__bus__ncpr__clnt__20110118', incremental_strategy='insert_overwrite', tags=['DltAppt_QstnFrmTMP']) }}

SELECT
	APPT_I,
	QSTN_C,
	RESP_C,
	RESP_CMMT_X,
	PATY_I,
	EFFT_D,
	EXPY_D,
	ROW_SECU_ACCS_C,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('Remove_Duplicates_261') }}