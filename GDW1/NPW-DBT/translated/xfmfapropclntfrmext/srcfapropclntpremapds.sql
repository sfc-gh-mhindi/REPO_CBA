{{ config(materialized='view', tags=['XfmFaPropClntFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__coi__bus__prop__clnt__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__coi__bus__prop__clnt__premap")  }})
SrcFAPropClntPremapDS AS (
	SELECT FA_PROPOSED_CLIENT_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		COIN_ENTITY_NAME,
		FA_ENTITY_CAT_ID,
		FA_UNDERTAKING_ID,
		FA_PROPOSED_CLIENT_CAT_ID,
		ORIG_ETL_D,
		change_code
	FROM _cba__app_csel4_csel4dev_dataset_cse__coi__bus__prop__clnt__premap
)

SELECT * FROM SrcFAPropClntPremapDS