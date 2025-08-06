{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__coi__bus__envi__evnt__mapping__rejects', incremental_strategy='insert_overwrite', tags=['XfmFaEnvisionEventFrmExt']) }}

SELECT
	FA_ENVISION_EVENT_ID,
	FA_UNDERTAKING_ID,
	FA_ENVISION_EVENT_CAT_ID,
	CREATED_DATE,
	CREATED_BY_STAFF_NUMBER,
	COIN_REQUEST_ID,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmBusinessRules__OutFAEnvEvntRejectsDS') }}