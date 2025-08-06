{{ config(materialized='view', tags=['ExtFaEnvisionEvent']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__coi__bus__envi__evnt__cse__coi__bus__envi__evnt__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__coi__bus__envi__evnt__cse__coi__bus__envi__evnt__20060616")  }})
SrcFAEnvisionEvent AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		FA_ENVISION_EVENT_ID,
		FA_UNDERTAKING_ID,
		FA_ENVISION_EVENT_CAT_ID,
		CREATED_DATE,
		CREATED_BY_STAFF_NUMBER,
		COIN_REQUEST_ID,
		DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__coi__bus__envi__evnt__cse__coi__bus__envi__evnt__20060616
)

SELECT * FROM SrcFAEnvisionEvent