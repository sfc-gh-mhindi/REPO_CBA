{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_evnt__antn__i__cse__onln__bus__clnt__rm__rate__20100808', incremental_strategy='insert_overwrite', tags=['XfmEvntAntn']) }}

SELECT
	EVNT_I,
	ANTN_I,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('XfmInserts') }}