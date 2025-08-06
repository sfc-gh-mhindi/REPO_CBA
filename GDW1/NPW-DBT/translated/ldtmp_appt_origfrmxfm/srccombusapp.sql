{{ config(materialized='view', tags=['LdTMP_APPT_ORIGFrmXfm']) }}

WITH 
_cba__app_csel4_dev_inprocess_cse__com__bus__app__cse__com__bus__ccl__chl__com__app__20130705 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_cse__com__bus__app__cse__com__bus__ccl__chl__com__app__20130705")  }})
SrcComBusApp AS (
	SELECT APP_RECORD_TYPE,
		APP_MOD_TIMESTAMP,
		APP_APP_ID,
		APP_SUBTYPE_CODE,
		APP_APP_NO,
		APP_CREATED_DATE,
		APP_CREATED_BY_STAFF_NUMBER,
		APP_OWNED_BY_STAFF_NUMBER,
		APP_CHANNEL_CAT_ID,
		APP_LODGEMENT_BRANCH_ID,
		APP_SM_CASE_ID,
		APP_ENTRY_POINT,
		APP_CREATED_CHANNEL_CAT_ID,
		APP_SUBMITTED_CHANNEL_CAT_ID
	FROM _cba__app_csel4_dev_inprocess_cse__com__bus__app__cse__com__bus__ccl__chl__com__app__20130705
)

SELECT * FROM SrcComBusApp