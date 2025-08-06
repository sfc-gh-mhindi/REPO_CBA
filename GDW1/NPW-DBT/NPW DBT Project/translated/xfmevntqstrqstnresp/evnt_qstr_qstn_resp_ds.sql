{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_evnt__qstr__qstn__resp__i__cse__onln__bus__clnt__rm__rate__20100808', incremental_strategy='insert_overwrite', tags=['XfmEvntQstrQstnResp']) }}

SELECT
	EVNT_I,
	QSTR_C,
	QSTN_C,
	RESP_C,
	RESP_VALU_N,
	RESP_VALU_S,
	RESP_VALU_D,
	RESP_VALU_T,
	RESP_VALU_X,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('XfmInserts__Insert') }}