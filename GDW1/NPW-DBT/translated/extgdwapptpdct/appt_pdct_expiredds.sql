{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__gdw__appt__pdct__appt__pdct__activedelete', incremental_strategy='insert_overwrite', tags=['ExtGdwApptPdct']) }}

SELECT
	APPT_PDCT_I,
	RELD_APPT_PDCT_I,
	EXPY_FLAG 
FROM {{ ref('SrcAPPT_PDCTA') }}