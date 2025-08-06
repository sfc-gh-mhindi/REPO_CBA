{{ config(materialized='view', tags=['XfmAppCclAppChlAppFrmExt']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__tpb__appt__c AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__tpb__appt__c")  }})
TgtMAP_CSE_TPB_APPT_CLks AS (
	SELECT CHL_TPB_SUBTYPE_CODE,
		APPT_C
	FROM _cba__app_csel4_dev_lookupset_map__cse__tpb__appt__c
)

SELECT * FROM TgtMAP_CSE_TPB_APPT_CLks