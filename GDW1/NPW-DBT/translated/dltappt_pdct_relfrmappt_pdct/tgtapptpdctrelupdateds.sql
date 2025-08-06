{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__rel__u__cse__gdw__appt__pdct__20060101', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_RELFrmAPPT_PDCT']) }}

SELECT
	APPT_PDCT_I,
	RELD_APPT_PDCT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaActionU') }}