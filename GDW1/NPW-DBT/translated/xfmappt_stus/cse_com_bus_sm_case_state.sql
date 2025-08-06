{{ config(materialized='view', tags=['XfmAppt_Stus']) }}

WITH 
_cba__app_csel4_sit_inprocess_cse__com__bus__sm__case__state__cse__com__bus__sm__case__state__20110202 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_inprocess_cse__com__bus__sm__case__state__cse__com__bus__sm__case__state__20110202")  }})
CSE_COM_BUS_SM_CASE_STATE AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		SM_CASE_STATE_ID,
		SM_CASE_ID,
		SM_STATE_CAT_ID,
		START_DATE,
		END_DATE,
		CREATED_BY_STAFF_NUMBER,
		STATE_CAUSED_BY_ACTION_ID
	FROM _cba__app_csel4_sit_inprocess_cse__com__bus__sm__case__state__cse__com__bus__sm__case__state__20110202
)

SELECT * FROM CSE_COM_BUS_SM_CASE_STATE