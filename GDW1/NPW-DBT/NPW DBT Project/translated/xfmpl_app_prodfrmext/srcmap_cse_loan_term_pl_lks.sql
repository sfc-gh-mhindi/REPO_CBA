{{ config(materialized='view', tags=['XfmPL_APP_PRODFrmExt']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__loan__term__pl__prod__term__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__loan__term__pl__prod__term__cat__id")  }})
SrcMAP_CSE_LOAN_TERM_PL_Lks AS (
	SELECT PL_PROD_TERM_CAT_ID,
		SRCE_SYST_ACTL_TERM_Q
	FROM _cba__app_csel4_dev_lookupset_map__cse__loan__term__pl__prod__term__cat__id
)

SELECT * FROM SrcMAP_CSE_LOAN_TERM_PL_Lks