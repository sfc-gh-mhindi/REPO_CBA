{{ config(materialized='view', tags=['XfmAppt_Stus']) }}

WITH 
_cba__app_csel4_sit_inprocess_cse__com__bus__app__cse__com__bus__ccl__chl__com__app__20110202 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_inprocess_cse__com__bus__app__cse__com__bus__ccl__chl__com__app__20110202")  }})
CseComBusApp AS (
	SELECT RecordType,
		Mod_Date,
		APP_ID,
		SUBTYPE_CODE,
		APP_NO,
		CREATED_DATE,
		CREATED_BY_STAFF_NUMBER,
		OWNED_BY_STAFF_NUMBER,
		CHANNEL_CAT_ID,
		GL_DEPT_NO,
		SM_CASE_ID,
		APP_ENTRY_POINT
	FROM _cba__app_csel4_sit_inprocess_cse__com__bus__app__cse__com__bus__ccl__chl__com__app__20110202
)

SELECT * FROM CseComBusApp