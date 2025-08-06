{{ config(materialized='view', tags=['XfmHL_APPFrmExt']) }}

WITH 
_cba__app_pj__rapidresponseteam_csel4_dev_lookupset_map__cse__lpc__dept__hl__lpc__office AS (
	SELECT
	*
	FROM {{ source("","_cba__app_pj__rapidresponseteam_csel4_dev_lookupset_map__cse__lpc__dept__hl__lpc__office")  }})
SrcMAP_CSE_LPC_DEPT_HLLks AS (
	SELECT LPC_OFFICE,
		DEPT_I
	FROM _cba__app_pj__rapidresponseteam_csel4_dev_lookupset_map__cse__lpc__dept__hl__lpc__office
)

SELECT * FROM SrcMAP_CSE_LPC_DEPT_HLLks