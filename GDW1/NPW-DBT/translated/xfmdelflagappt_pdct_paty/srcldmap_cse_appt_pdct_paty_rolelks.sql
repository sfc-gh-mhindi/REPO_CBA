{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_PATY']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__appt__pdct__paty__role AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__appt__pdct__paty__role")  }})
SrcLdMAP_CSE_APPT_PDCT_PATY_ROLELks AS (
	SELECT ROLE_CAT_ID,
		PATY_ROLE_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__appt__pdct__paty__role
)

SELECT * FROM SrcLdMAP_CSE_APPT_PDCT_PATY_ROLELks