{{ config(materialized='view', tags=['XfmChlBusTuAPPFrmExt']) }}

WITH 
_cba__app_hlt_dev_lookupset_map__cse__cnty__code AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_lookupset_map__cse__cnty__code")  }})
TgtMAP_CSE_CNTY_CODELks AS (
	SELECT CNTY_ID,
		ISO_CNTY_C
	FROM _cba__app_hlt_dev_lookupset_map__cse__cnty__code
)

SELECT * FROM TgtMAP_CSE_CNTY_CODELks