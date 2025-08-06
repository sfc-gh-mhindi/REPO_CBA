{{ config(materialized='view', tags=['IdentifyDeltaFrmPrevFaPropClntOra']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__coi__bus__prop__clnt__cse__coi__bus__prop__clnt__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__coi__bus__prop__clnt__cse__coi__bus__prop__clnt__20060101")  }})
SrcFAProposedClient AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		FA_PROPOSED_CLIENT_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		COIN_ENTITY_NAME,
		FA_ENTITY_CAT_ID,
		FA_UNDERTAKING_ID,
		FA_PROPOSED_CLIENT_CAT_ID,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__coi__bus__prop__clnt__cse__coi__bus__prop__clnt__20060101
)

SELECT * FROM SrcFAProposedClient