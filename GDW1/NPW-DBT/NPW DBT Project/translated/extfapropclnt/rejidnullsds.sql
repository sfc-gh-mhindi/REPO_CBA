{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__coi__bus__prop__clnt__nulls__rejects', incremental_strategy='insert_overwrite', tags=['ExtFaPropClnt']) }}

SELECT
	FA_PROPOSED_CLIENT_ID,
	COIN_ENTITY_ID,
	CLIENT_CORRELATION_ID,
	COIN_ENTITY_NAME,
	FA_ENTITY_CAT_ID,
	FA_UNDERTAKING_ID,
	FA_PROPOSED_CLIENT_CAT_ID,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmCheckIdNulls__OutIdNullsDS') }}