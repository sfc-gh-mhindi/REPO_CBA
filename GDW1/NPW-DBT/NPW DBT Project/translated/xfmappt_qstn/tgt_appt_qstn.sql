{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__com__bus__app__ncpr__answer__cse__com__cpo__bus__ncpr__clnt__20110110', incremental_strategy='insert_overwrite', tags=['XfmAppt_Qstn']) }}

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
FROM {{ ref('Xfm__Xfm_to_Tgt') }}