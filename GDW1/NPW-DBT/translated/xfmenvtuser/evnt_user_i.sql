{{ config(materialized='incremental', alias='_cba__app01_csel4_dev_dataset_evnt__user__i__cse__onln__bus__clnt__rm__rate__20100303', incremental_strategy='insert_overwrite', tags=['XfmEnvtUser']) }}

SELECT
	EVNT_I,
	USER_I,
	EVNT_PATY_ROLE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmInserts') }}