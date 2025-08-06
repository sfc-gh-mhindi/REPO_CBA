{{ config(materialized='view', tags=['XfmAppProdCclAppProdPlAppProdFrmExt']) }}

WITH 
_cba__app_pj__gisswrteam_csel4_dev_lookupset_map__cse__job__comm__catg__code AS (
	SELECT
	*
	FROM {{ source("","_cba__app_pj__gisswrteam_csel4_dev_lookupset_map__cse__job__comm__catg__code")  }})
Outmap_CSE_JOB_Comm_CatgLks AS (
	SELECT CLP_JOB_FAMILY_CAT_ID,
		JOB_COMM_CATG_C
	FROM _cba__app_pj__gisswrteam_csel4_dev_lookupset_map__cse__job__comm__catg__code
)

SELECT * FROM Outmap_CSE_JOB_Comm_CatgLks