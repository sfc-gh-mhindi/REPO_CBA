{{ config(materialized='incremental', alias='_cba__app_csel4_sit_dataset_tmp__cse__ccl__bus__preapproval__log__appt__paty', incremental_strategy='insert_overwrite', tags=['ExtCclBusPreprovLog']) }}

SELECT
	APPT_I,
	PATY_I,
	REL_C,
	REL_REAS_C,
	REL_STUS_C,
	REL_LEVL_C,
	SRCE_SYST_C,
	RUN_STRM 
FROM {{ ref('Trm__OutCclApptPatyDS') }}