{{ config(materialized='view', tags=['XfmAppCclAppChlAppFrmExt']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__appt__form__ccl__form__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__appt__form__ccl__form__cat__id")  }})
SrcMAP_CSE_APPT_FORMLks AS (
	SELECT CCL_FORM_CAT_ID,
		APPT_FORM_C
	FROM _cba__app_csel4_dev_lookupset_map__cse__appt__form__ccl__form__cat__id
)

SELECT * FROM SrcMAP_CSE_APPT_FORMLks