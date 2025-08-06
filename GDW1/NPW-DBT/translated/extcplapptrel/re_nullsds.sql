{{ config(materialized='incremental', alias='_cba__app_hlt_sit_dataset_cse__clp__bus__appt__rel__nulls__rejects20070910', incremental_strategy='insert_overwrite', tags=['ExtCplApptRel']) }}

SELECT
	APPT_I,
	RELD_APPT_I,
	REL_TYPE_C,
	LOAN_SUBTYPE_CODE,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmCheckCCAppProdBalXferIdNulls__OutCCAppProdBalXferIdNullsDS') }}