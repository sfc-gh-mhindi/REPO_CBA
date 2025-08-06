{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_evnt__rel__i__cse__onln__bus__clnt__rm__rate__20100803', incremental_strategy='insert_overwrite', tags=['XfmEvntRel']) }}

SELECT
	EVNT_I,
	RELD_EVNT_I,
	EFFT_D,
	EVNT_REL_TYPE_C,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('XfmInserts') }}