{{ config(materialized='view', tags=['XfmAppProdCclAppProdPlAppProdFrmExt']) }}

WITH 
_cba__app_pj__gisswrteam_csel4_dev_lookupset_map__cse__appt__acqr__srce__pl__mrkt__srce__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_pj__gisswrteam_csel4_dev_lookupset_map__cse__appt__acqr__srce__pl__mrkt__srce__cat__id")  }})
SrcMAP_CSE_APPT_ACQR_SRCELks AS (
	SELECT PL_MRKT_SRCE_CAT_ID,
		ACQR_SRCE_C
	FROM _cba__app_pj__gisswrteam_csel4_dev_lookupset_map__cse__appt__acqr__srce__pl__mrkt__srce__cat__id
)

SELECT * FROM SrcMAP_CSE_APPT_ACQR_SRCELks