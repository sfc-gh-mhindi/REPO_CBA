{{ config(materialized='view', tags=['XfmComBusAppProdClientRoleFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__appt__pdct__paty__role AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__appt__pdct__paty__role")  }})
SrcMAP_CSE_APPT_PDCT_PATY_ROLELks AS (
	SELECT ROLE_CAT_ID,
		PATY_ROLE_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__appt__pdct__paty__role
)

SELECT * FROM SrcMAP_CSE_APPT_PDCT_PATY_ROLELks