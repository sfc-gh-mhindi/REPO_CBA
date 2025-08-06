{{ config(materialized='view', tags=['ExtFaUndertaking']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__coi__bus__undtak__cse__coi__bus__undtak__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__coi__bus__undtak__cse__coi__bus__undtak__20060616")  }})
SrcFAUndertaking AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		FA_UNDERTAKING_ID,
		PLANNING_GROUP_NAME,
		COIN_ADVICE_GROUP_ID,
		ADVICE_GROUP_CORRELATION_ID,
		CREATED_DATE,
		CREATED_BY_STAFF_NUMBER,
		SM_CASE_ID,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__coi__bus__undtak__cse__coi__bus__undtak__20060616
)

SELECT * FROM SrcFAUndertaking