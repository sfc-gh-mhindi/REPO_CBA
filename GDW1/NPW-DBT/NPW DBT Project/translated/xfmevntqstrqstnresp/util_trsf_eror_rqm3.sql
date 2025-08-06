{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_util__trsf__eror__rqm3__i__cse__onln__bus__clnt__rm__rate__20100808', incremental_strategy='insert_overwrite', tags=['XfmEvntQstrQstnResp']) }}

SELECT
	SRCE_KEY_I,
	CONV_M,
	CONV_MAP_RULE_M,
	TRSF_TABL_M,
	SRCE_EFFT_D,
	VALU_CHNG_BFOR_X,
	VALU_CHNG_AFTR_X,
	TRSF_X,
	TRSF_COLM_M,
	EROR_SEQN_I,
	SRCE_FILE_M,
	PROS_KEY_EFFT_I,
	TRSF_KEY_I 
FROM {{ ref('XfmInserts__ToErr') }}