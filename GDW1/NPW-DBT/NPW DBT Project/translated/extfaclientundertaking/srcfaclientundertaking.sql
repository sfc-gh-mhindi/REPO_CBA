{{ config(materialized='view', tags=['ExtFAClientUndertaking']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__coi__bus__clnt__undtak__cse__coi__bus__clnt__undtak__20060824 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__coi__bus__clnt__undtak__cse__coi__bus__clnt__undtak__20060824")  }})
SrcFAClientUndertaking AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		FA_CLIENT_UNDERTAKING_ID,
		FA_UNDERTAKING_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		FA_ENTITY_CAT_ID,
		FA_CHILD_STATUS_CAT_ID,
		CLIENT_RELATIONSHIP_TYPE_ID,
		CLIENT_POSITION,
		IS_PRIMARY_FLAG,
		CIF_CODE,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__coi__bus__clnt__undtak__cse__coi__bus__clnt__undtak__20060824
)

SELECT * FROM SrcFAClientUndertaking