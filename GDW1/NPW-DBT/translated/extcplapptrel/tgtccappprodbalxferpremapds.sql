{{ config(materialized='incremental', alias='_cba__app_hlt_sit_dataset_cse__clp__bus__appt__rel__premap__20070910', incremental_strategy='insert_overwrite', tags=['ExtCplApptRel']) }}

SELECT
	LOAN_SUBTYPE_CODE,
	APPT_I,
	REL_TYPE_C,
	RELD_APPT_I,
	ORIG_ETL_D 
FROM {{ ref('FunMergeJournal') }}