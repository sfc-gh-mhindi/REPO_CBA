{{ config(materialized='view', tags=['XfmAppCclAppChlAppFrmExt']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__orig__srce__sys AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__orig__srce__sys")  }})
TgtMAP_CSE_ORIG_SRCE_SYS_CLks AS (
	SELECT ORIG_SRCE_SYST_I,
		ORIG_SRCE_SYST_C
	FROM _cba__app_csel4_dev_lookupset_map__cse__orig__srce__sys
)

SELECT * FROM TgtMAP_CSE_ORIG_SRCE_SYS_CLks