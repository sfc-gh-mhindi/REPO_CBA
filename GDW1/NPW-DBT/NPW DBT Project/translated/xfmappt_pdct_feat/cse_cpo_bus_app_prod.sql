{{ config(materialized='view', tags=['XfmAppt_Pdct_Feat']) }}

WITH 
_cba__app_csel4_dev_inprocess_cse__cpo__bus__app__prod__cse__com__cpo__ncpr__clnt__empl__20101025 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_cse__cpo__bus__app__prod__cse__com__cpo__ncpr__clnt__empl__20101025")  }})
CSE_CPO_BUS_APP_PROD AS (
	SELECT RecordType,
		MOD_TIMESTAMP,
		APP_PROD_ID,
		DDA_ACCOUNT_ID,
		ACCOUNT_NUMBER,
		CRIS_PRODUCT_ID,
		CONTRACT_INTERACTION_ID,
		REPAYMENT_ACCOUNT_ID,
		LIMIT_AMOUNT_PREAPPROVED,
		LIMIT_AMOUNT_REQUESTED,
		LIMIT_AMOUNT_APPROVED,
		PO_OVERDRAFT_CAT_ID,
		EXCESSIVE_LIMIT_REASON,
		PO_REPAYMENT_SOURCE_CAT_ID,
		PO_REPAYMENT_SOURCE_OTHER,
		NOMINATED_BSB,
		PL_PROD_PURP_CAT_ID,
		TEMP_OVERDRAFT_TERM_IN_DAYS,
		WIZARD_TYPE,
		READ_COSTS_AND_RISKS_FLAG,
		ACCEPTS_COSTS_AND_RISKS_DATE,
		LODGEMENT_BRANCH_BSB,
		EXISTING_ACCOUNT_LIMIT,
		REPAYMENT_ACCOUNT_NUMBER,
		REPAYMENT_CRIS_PRODUCT_ID
	FROM _cba__app_csel4_dev_inprocess_cse__cpo__bus__app__prod__cse__com__cpo__ncpr__clnt__empl__20101025
)

SELECT * FROM CSE_CPO_BUS_APP_PROD