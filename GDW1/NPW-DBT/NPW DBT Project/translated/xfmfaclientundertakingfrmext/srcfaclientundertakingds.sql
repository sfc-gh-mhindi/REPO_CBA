{{ config(materialized='view', tags=['XfmFaClientUndertakingFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__coi__bus__envi__evnt__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__coi__bus__envi__evnt__premap")  }})
SrcFAClientundertakingDS AS (
	SELECT FA_CLIENT_UNDERTAKING_ID,
		FA_UNDERTAKING_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		FA_ENTITY_CAT_ID,
		FA_CHILD_STATUS_CAT_ID,
		CLIENT_RELATIONSHIP_TYPE_ID,
		CLIENT_POSITION,
		IS_PRIMARY_FLAG,
		CIF_CODE,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__coi__bus__envi__evnt__premap
)

SELECT * FROM SrcFAClientundertakingDS