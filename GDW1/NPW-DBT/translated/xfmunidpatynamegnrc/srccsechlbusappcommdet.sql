{{ config(materialized='view', tags=['XfmUnidPatyNameGnrc']) }}

WITH 
_cba__app_csel4_sit_inprocess_cse__chl__bus__app__commission__det__cse__chl__bus__app__comm__20110627 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_inprocess_cse__chl__bus__app__commission__det__cse__chl__bus__app__comm__20110627")  }})
SrcCseChlBusAppCommDet AS (
	SELECT REC_TYPE,
		HL_APP_COMMISSION_ID,
		MOD_TIMESTAMP,
		MOD_USER_ID,
		HL_APP_ID,
		HL_COMMISSION_CAT_ID,
		NAME,
		ABN_ACN_NUMBER,
		AMOUNT,
		ACN,
		ABN,
		ADDITIONAL_COMMISSIONS_FLAG,
		MOD_CLIENT_ID,
		HL_APP_COMMISSION_BASE_VN,
		IS_REFERRAL_AGENT,
		REFERRAL_AGENT_ID
	FROM _cba__app_csel4_sit_inprocess_cse__chl__bus__app__commission__det__cse__chl__bus__app__comm__20110627
)

SELECT * FROM SrcCseChlBusAppCommDet