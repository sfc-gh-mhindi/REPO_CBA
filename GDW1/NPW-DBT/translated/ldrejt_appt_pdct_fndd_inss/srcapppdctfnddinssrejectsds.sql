{{ config(materialized='view', tags=['LdREJT_APPT_PDCT_FNDD_INSS']) }}

WITH 
_cba__app_csel4_csel4prd_dataset_cse__chl__bus__tu__app__fund__det__mapping__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4prd_dataset_cse__chl__bus__tu__app__fund__det__mapping__rejects")  }})
SrcAppPdctFnddInssRejectsDS AS (
	SELECT TU_APP_ID,
		FUNDING_DATE,
		TU_APP_FUNDING_DETAIL_ID,
		TU_APP_FUNDING_ID,
		FUNDING_CBA_ACCOUNT_ID,
		FUNDING_NONCBA_INSTITUTION_ID,
		FUNDING_NONCBA_INST_NAME,
		FUNDING_NONCBA_INST_ADDRESS,
		FUNDING_NONCBA_BSB,
		FUNDING_NONCBA_ACCOUNT_NUMBER,
		FUNDING_NONCBA_ACCOUNT_NAME,
		FUNDING_BANKCHEQUE_NUMBER,
		FUNDING_BANKCHEQUE_PAYEE,
		FUNDING_METHOD_CAT_ID,
		FUNDING_BANKCHEQUE_CBAACCOUNT,
		PROGRESSIVE_PAYMENT_AMT,
		SBTY_CODE,
		HL_APP_PROD_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4prd_dataset_cse__chl__bus__tu__app__fund__det__mapping__rejects
)

SELECT * FROM SrcAppPdctFnddInssRejectsDS