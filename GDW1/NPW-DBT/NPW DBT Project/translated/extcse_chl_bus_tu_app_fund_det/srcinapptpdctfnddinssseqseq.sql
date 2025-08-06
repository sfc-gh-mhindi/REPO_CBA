{{ config(materialized='view', tags=['ExtCSE_CHL_BUS_TU_APP_FUND_DET']) }}

WITH 
_cba__app_csel4_csel4prd_inprocess_cse__chl__bus__tu__app__fund__det__cse__chl__bus__tu__app__fund__det__20071010 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4prd_inprocess_cse__chl__bus__tu__app__fund__det__cse__chl__bus__tu__app__fund__det__20071010")  }})
SrcInApptPdctFnddInssSeqSeq AS (
	SELECT RECORD_TYPE,
		MOD_TIMESTAMP,
		TU_APP_ID,
		FUNDING_DATE,
		TU_APP_FUNDING_DETAIL_ID,
		TU_APP_FUNDING_ID,
		FUNDING_CBA_ACCOUNT_ID,
		FUNDING_NONCBA_BANK_NUMBER,
		FUNDING_NONCBA_INST_NAME,
		FUNDING_NONCBA_INST_ADDRESS,
		FUNDING_NONCBA_BSB,
		FUNDING_NONCBA_ACCOUNT_NUMBER,
		FUNDING_NONCBA_ACCOUNT_NAME,
		FUNDING_BANKCHEQUE_NUMBER,
		FUNDING_BANKCHEQUE_PAYEE,
		MOD_USER_ID,
		FUNDING_METHOD_CAT_ID,
		FUNDING_BANKCHEQUE_CBAACCOUNT,
		PROGRESSIVE_PAYMENT_AMT,
		SBTY_CODE,
		HL_APP_PROD_ID,
		DUMMY
	FROM _cba__app_csel4_csel4prd_inprocess_cse__chl__bus__tu__app__fund__det__cse__chl__bus__tu__app__fund__det__20071010
)

SELECT * FROM SrcInApptPdctFnddInssSeqSeq