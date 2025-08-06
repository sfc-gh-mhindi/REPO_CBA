{{ config(materialized='incremental', alias='____var__dbt__poutbound_________var__dbt__prun__strm__c__________var__dbt__pctable__name__________var__dbt__prun__strm__pros__d______i', incremental_strategy='insert_overwrite', tags=['DltAPPT_PRMO_TRAKFrmTMP_APPT_PRMO_TRAK']) }}

SELECT
	APPT_PDCT_I,
	TRAK_I,
	TRAK_IDNN_TYPE_C,
	PRVD_D,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('xf_DecideRecordType__Ln_Insert_Records') }}