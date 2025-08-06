{{ config(materialized='view', tags=['ExtChlBusFeeBusFeeDisc']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__merge__20060831 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__merge__20060831")  }})
MergeDS AS (
	SELECT HL_FEE_ID,
		BF_HL_FEE_ID,
		BF_HL_APP_PROD_ID,
		BF_XML_CODE,
		BF_DISPLAY_NAME,
		BF_CATEGORY,
		BF_UNIT_AMOUNT,
		BF_TOTAL_AMOUNT,
		BF_FOUND_FLAG,
		BFD_HL_FEE_DISCOUNT_ID,
		BFD_HL_FEE_ID,
		BFD_DISCOUNT_REASON,
		BFD_DISCOUNT_CODE,
		BFD_DISCOUNT_AMT,
		BFD_DISCOUNT_TERM,
		BFD_HL_FEE_DISCOUNT_CAT_ID,
		BFD_FOUND_FLAG
	FROM _cba__app_csel4_csel4dev_dataset_cse__chl__bus__fee__disc__fee__merge__20060831
)

SELECT * FROM MergeDS