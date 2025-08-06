{{ config(materialized='incremental', alias='____var__dbt__poutbound_________var__dbt__prun__strm__c__________var__dbt__pctable__name__________var__dbt__prun__strm__pros__d______u', incremental_strategy='insert_overwrite', tags=['DltAPPT_PRMO_TRAKFrmTMP_APPT_PRMO_TRAK']) }}

SELECT
	APPT_PDCT_I,
	TRAK_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('xf_DecideRecordType__Ln_Update_Records') }}