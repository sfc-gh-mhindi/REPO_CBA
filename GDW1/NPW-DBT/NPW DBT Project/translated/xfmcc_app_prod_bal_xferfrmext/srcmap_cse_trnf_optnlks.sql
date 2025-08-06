{{ config(materialized='view', tags=['XfmCC_APP_PROD_BAL_XFERFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__trnf__optn__bal__xfer__option__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__trnf__optn__bal__xfer__option__cat__id")  }})
SrcMAP_CSE_TRNF_OPTNLks AS (
	SELECT BAL_XFER_OPTION_CAT_ID,
		TRNF_OPTN_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__trnf__optn__bal__xfer__option__cat__id
)

SELECT * FROM SrcMAP_CSE_TRNF_OPTNLks