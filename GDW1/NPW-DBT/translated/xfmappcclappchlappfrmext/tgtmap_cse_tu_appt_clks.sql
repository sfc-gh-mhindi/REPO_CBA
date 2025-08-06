{{ config(materialized='view', tags=['XfmAppCclAppChlAppFrmExt']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__tu__appt__c AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__tu__appt__c")  }})
TgtMAP_CSE_TU_APPT_CLks AS (
	SELECT SBTY_CODE,
		APPT_C
	FROM _cba__app_csel4_dev_lookupset_map__cse__tu__appt__c
)

SELECT * FROM TgtMAP_CSE_TU_APPT_CLks