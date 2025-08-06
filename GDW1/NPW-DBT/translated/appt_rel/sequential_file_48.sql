{{ config(materialized='view', tags=['APPT_REL']) }}

WITH 
_cba__app_hlt_sit_inprocess_cse__clp__bus__appt__rel__cse__clp__bus__appt__rel__20070910 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_sit_inprocess_cse__clp__bus__appt__rel__cse__clp__bus__appt__rel__20070910")  }})
Sequential_File_48 AS (
	SELECT RECORD,
		DATE,
		APP_ID,
		SUBTYPE_CODE,
		LOAN_APP_ID,
		LOAN_SUBTYPE_CODE,
		DUMMY
	FROM _cba__app_hlt_sit_inprocess_cse__clp__bus__appt__rel__cse__clp__bus__appt__rel__20070910
)

SELECT * FROM Sequential_File_48