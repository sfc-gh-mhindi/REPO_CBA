{{ config(materialized='view', tags=['XfmUnidPatyGnrc']) }}

WITH 
_cba__app_csel4_dev_inprocess_cse__chl__bus__app__commission__det__cse__chl__bus__app__commission__det__20110623 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_cse__chl__bus__app__commission__det__cse__chl__bus__app__commission__det__20110623")  }})
SrcCseChlBusAppCommDet AS (
	SELECT Rec_typ,
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
	FROM _cba__app_csel4_dev_inprocess_cse__chl__bus__app__commission__det__cse__chl__bus__app__commission__det__20110623
)

SELECT * FROM SrcCseChlBusAppCommDet