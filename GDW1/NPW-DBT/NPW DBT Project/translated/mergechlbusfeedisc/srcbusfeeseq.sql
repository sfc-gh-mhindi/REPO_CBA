{{ config(materialized='view', tags=['MergeChlBusFeeDisc']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__fee__cse__chl__bus__fee__disc__fee__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__fee__cse__chl__bus__fee__disc__fee__20060616")  }})
SrcBusFeeSeq AS (
	SELECT BF_RECORD_TYPE,
		BF_MOD_TIMESTAMP,
		HL_FEE_ID,
		BF_HL_APP_PROD_ID,
		BF_XML_CODE,
		BF_DISPLAY_NAME,
		BF_CATEGORY,
		BF_UNIT_AMOUNT,
		BF_TOTAL_AMOUNT,
		BF_DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__chl__bus__fee__cse__chl__bus__fee__disc__fee__20060616
)

SELECT * FROM SrcBusFeeSeq