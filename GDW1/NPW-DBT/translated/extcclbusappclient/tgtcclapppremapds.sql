{{ config(materialized='incremental', alias='_cba__app_hlt_sit_dataset_tmp__cse__ccl__bus__app__client__appt__paty', incremental_strategy='insert_overwrite', tags=['ExtCclBusAppClient']) }}

SELECT
	APPT_I,
	PATY_I,
	REL_C,
	REL_REAS_C,
	REL_STUS_C,
	REL_LEVL_C,
	SRCE_SYST_C,
	RUN_STRM 
FROM {{ ref('Trm') }}