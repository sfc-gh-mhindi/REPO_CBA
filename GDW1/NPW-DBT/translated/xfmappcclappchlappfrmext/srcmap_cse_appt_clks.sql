{{ config(materialized='view', tags=['XfmAppCclAppChlAppFrmExt']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__appt__c__ccl__app__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__appt__c__ccl__app__cat__id")  }})
SrcMAP_CSE_APPT_CLks AS (
	SELECT CCL_APP_CAT_ID,
		APPT_C
	FROM _cba__app_csel4_dev_lookupset_map__cse__appt__c__ccl__app__cat__id
)

SELECT * FROM SrcMAP_CSE_APPT_CLks