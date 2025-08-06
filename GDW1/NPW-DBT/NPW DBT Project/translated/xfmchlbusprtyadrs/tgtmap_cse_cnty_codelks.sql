{{ config(materialized='view', tags=['XfmChlBusPrtyAdrs']) }}

WITH 
_cba__app_csel4_csel4__prd_lookupset_map__cse__cnty__code AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4__prd_lookupset_map__cse__cnty__code")  }})
TgtMAP_CSE_CNTY_CODELks AS (
	SELECT CNTY_ID,
		ISO_CNTY_C
	FROM _cba__app_csel4_csel4__prd_lookupset_map__cse__cnty__code
)

SELECT * FROM TgtMAP_CSE_CNTY_CODELks