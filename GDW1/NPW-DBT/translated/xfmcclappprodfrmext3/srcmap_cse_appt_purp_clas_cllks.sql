{{ config(materialized='view', tags=['XfmCclAppProdFrmExt3']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__appt__purp__clas__cl__loan__purpose__class__code AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__appt__purp__clas__cl__loan__purpose__class__code")  }})
SrcMAP_CSE_APPT_PURP_CLAS_CLLks AS (
	SELECT LOAN_PURPOSE_CLASS_CODE,
		PURP_CLAS_C
	FROM _cba__app_csel4_dev_lookupset_map__cse__appt__purp__clas__cl__loan__purpose__class__code
)

SELECT * FROM SrcMAP_CSE_APPT_PURP_CLAS_CLLks