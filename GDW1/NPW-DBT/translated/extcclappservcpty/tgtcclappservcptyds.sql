{{ config(materialized='incremental', alias='_cba__app_hlt_sit_dataset_tmp__cse__ccl__bus__app__servicetst__appt__serv__cpty', incremental_strategy='insert_overwrite', tags=['ExtCclappServCpty']) }}

SELECT
	APPT_I,
	APPT_SERV_CPTY_I,
	SRCE_SYST_APPT_SERV_CPTY_I,
	NET_SRPL_A,
	TOTL_HSHD_EXPD_A,
	RUN_STRM 
FROM {{ ref('Trm') }}