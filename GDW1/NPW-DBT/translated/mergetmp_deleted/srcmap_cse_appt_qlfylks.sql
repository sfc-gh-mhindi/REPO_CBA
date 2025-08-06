{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__appt__qlfy__sbty__code AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__appt__qlfy__sbty__code")  }})
SrcMAP_CSE_APPT_QLFYLks AS (
	SELECT SBTY_CODE,
		APPT_QLFY_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__appt__qlfy__sbty__code
)

SELECT * FROM SrcMAP_CSE_APPT_QLFYLks