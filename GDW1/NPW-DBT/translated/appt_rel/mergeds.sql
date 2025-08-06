{{ config(materialized='incremental', alias='_cba__app_hlt_sit_dataset_cse__clp__bus__appt__rel__merge__20070910', incremental_strategy='insert_overwrite', tags=['APPT_REL']) }}

SELECT
	APP_ID,
	SUBTYPE_CODE,
	LOAN_APP_ID,
	LOAN_SUBTYPE_CODE 
FROM {{ ref('Identify_IDs') }}