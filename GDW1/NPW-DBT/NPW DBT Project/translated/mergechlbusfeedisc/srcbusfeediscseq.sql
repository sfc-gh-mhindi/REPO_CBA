{{ config(materialized='view', tags=['MergeChlBusFeeDisc']) }}

WITH 
_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__fee__disc__cse__chl__bus__fee__disc__fee__20060616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_inprocess_cse__chl__bus__fee__disc__cse__chl__bus__fee__disc__fee__20060616")  }})
SrcBusFeeDiscSeq AS (
	SELECT BFD_RECORD_TYPE,
		BFD_MOD_TIMESTAMP,
		BFD_HL_FEE_DISCOUNT_ID,
		HL_FEE_ID,
		BFD_DISCOUNT_REASON,
		BFD_DISCOUNT_CODE,
		BFD_DISCOUNT_AMT,
		BFD_DISCOUNT_TERM,
		BFD_HL_FEE_DISCOUNT_CAT_ID,
		BFD_DUMMY
	FROM _cba__app_csel4_csel4dev_inprocess_cse__chl__bus__fee__disc__cse__chl__bus__fee__disc__fee__20060616
)

SELECT * FROM SrcBusFeeDiscSeq