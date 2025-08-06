{{ config(materialized='incremental', alias='_cba__app_hlt_sit_dataset_tmp__cse__clp__bus__appt__qstn__appt__qstn__detl', incremental_strategy='insert_overwrite', tags=['XfmCLP_ANS_Appt_QstnExt']) }}

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
FROM {{ ref('XfmBusinessRules__OutTmpApptTrnfDetlDS') }}