{{ config(materialized='view', tags=['XfmCC_APP_PROD_BAL_XFERFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__appt__cmpe__bal__xfer__institution__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__appt__cmpe__bal__xfer__institution__cat__id")  }})
SrcMAP_CSE_APPT_CMPELks AS (
	SELECT BAL_XFER_INSTITUTION_CAT_ID,
		CMPE_I
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__appt__cmpe__bal__xfer__institution__cat__id
)

SELECT * FROM SrcMAP_CSE_APPT_CMPELks