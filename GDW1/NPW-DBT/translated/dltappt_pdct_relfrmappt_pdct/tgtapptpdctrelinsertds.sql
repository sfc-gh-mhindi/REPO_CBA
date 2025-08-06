{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__rel__i__cse__gdw__appt__pdct__20060101', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_RELFrmAPPT_PDCT']) }}

SELECT
	APPT_PDCT_I,
	RELD_APPT_PDCT_I,
	REL_TYPE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaActionUI') }}