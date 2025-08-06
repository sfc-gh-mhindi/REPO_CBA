{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__xs__chl__cpl__bus__app__ans__appt__qstn__detl', incremental_strategy='insert_overwrite', tags=['Ext_APP_ANS']) }}

SELECT
	APPT_I,
	QSTN_C,
	EFFT_D,
	RESP_C,
	RESP_CMMT_X,
	EXPY_D,
	PATY_I,
	ROW_SECU_ACCS_C,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('Transformer_243__DSLink241') }}