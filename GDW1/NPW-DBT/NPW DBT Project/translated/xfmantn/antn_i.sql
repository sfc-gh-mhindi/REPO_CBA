{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_antn__i__cse__onln__bus__clnt__rm__rate__20100803', incremental_strategy='insert_overwrite', tags=['XfmAntn']) }}

SELECT
	ANTN_I,
	ANTN_TYPE_C,
	ANTN_X,
	SRCE_SYST_C,
	SRCE_SYST_ANTN_I,
	ANTN_S,
	ANTN_D,
	ANTN_T,
	EMPL_I,
	USER_I,
	PRVT_F,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmInserts') }}