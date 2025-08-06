{{ config(materialized='view', tags=['ExtCplApptRel']) }}

WITH 
_cba__app_hlt_sit_dataset_cse__clp__bus__appt__rel__merge__20070910 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_sit_dataset_cse__clp__bus__appt__rel__merge__20070910")  }})
Data_Set_215 AS (
	SELECT APP_ID,
		SUBTYPE_CODE,
		LOAN_APP_ID,
		LOAN_SUBTYPE_CODE
	FROM _cba__app_hlt_sit_dataset_cse__clp__bus__appt__rel__merge__20070910
)

SELECT * FROM Data_Set_215