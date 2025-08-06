{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_tmp__cse__ccc__bus__app__prod__bal__xfer__appt__trnf__detl', incremental_strategy='insert_overwrite', tags=['XfmCC_APP_PROD_BAL_XFERFrmExt']) }}

SELECT
	APPT_I,
	APPT_TRNF_I,
	EFFT_D,
	TRNF_OPTN_C,
	TRNF_A,
	CNCY_C,
	CMPE_I,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__OutTmpApptTrnfDetlDS') }}