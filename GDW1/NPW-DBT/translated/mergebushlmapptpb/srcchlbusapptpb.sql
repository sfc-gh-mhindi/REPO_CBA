{{ config(materialized='view', tags=['MergeBusHlmAppTpb']) }}

WITH 
_cba__app_csel4_dev_inprocess_chl__bus__hl__app__tpb__cse__chl__bus__hlmapp__20100614 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_chl__bus__hl__app__tpb__cse__chl__bus__hlmapp__20100614")  }})
SrcChlBusAppTPB AS (
	SELECT CHL_TPB_RECORD_TYPE,
		CHL_TPB_MOD_TIMESTAMP,
		CHL_TPB_HL_APP_ID,
		CHL_TPB_SUBTYPE_CODE,
		REL_MANAGER_STATE_ID,
		MOD_USER_ID,
		DATE_RECEIVED,
		HL_BUSINESS_CHANNEL_CAT_ID,
		CHL_AGENT_ALIAS_ID,
		CHL_AGENT_NAME
	FROM _cba__app_csel4_dev_inprocess_chl__bus__hl__app__tpb__cse__chl__bus__hlmapp__20100614
)

SELECT * FROM SrcChlBusAppTPB