{{ config(materialized='view', tags=['LdREJT_CSE_COI_BUS_PROP_CLNT']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__coi__bus__prop__clnt__mapping__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__coi__bus__prop__clnt__mapping__rejects")  }})
SrcFAPropClntRejectsDS AS (
	SELECT FA_PROPOSED_CLIENT_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		COIN_ENTITY_NAME,
		FA_ENTITY_CAT_ID,
		FA_UNDERTAKING_ID,
		FA_PROPOSED_CLIENT_CAT_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__coi__bus__prop__clnt__mapping__rejects
)

SELECT * FROM SrcFAPropClntRejectsDS