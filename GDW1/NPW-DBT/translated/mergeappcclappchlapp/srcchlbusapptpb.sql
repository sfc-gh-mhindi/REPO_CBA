{{ config(materialized='view', tags=['MergeAppCclAppChlApp']) }}

WITH 
_cba__app_csel4_dev_inprocess_chl__bus__hl__app__tpb__cse__com__bus__ccl__chl__com__app__20100614 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_chl__bus__hl__app__tpb__cse__com__bus__ccl__chl__com__app__20100614")  }})
SrcChlBusAppTPB AS (
	SELECT TPB_RECORD_TYPE,
		TPB_MOD_TIMESTAMP,
		TPB_HL_APP_ID,
		TPB_SUBTYPE_CODE,
		TPB_REL_MANAGER_STATE_ID,
		TPB_MOD_USER_ID,
		TPB_DATE_RECEIVED,
		TPB_HL_BUSINESS_CHANNEL_CAT_ID,
		TPB_AGENT_ALIAS_ID,
		TPB_AGENT_NAME
	FROM _cba__app_csel4_dev_inprocess_chl__bus__hl__app__tpb__cse__com__bus__ccl__chl__com__app__20100614
)

SELECT * FROM SrcChlBusAppTPB