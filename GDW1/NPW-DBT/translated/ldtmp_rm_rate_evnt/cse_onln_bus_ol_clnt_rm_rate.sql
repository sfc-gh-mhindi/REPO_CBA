{{ config(materialized='view', tags=['LdTmp_Rm_Rate_Evnt']) }}

WITH 
_cba__app_csel4_sit_inprocess_cse__onln__bus__ol__clnt__rm__rate__cse__onln__bus__clnt__rm__rate__20100607 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_inprocess_cse__onln__bus__ol__clnt__rm__rate__cse__onln__bus__clnt__rm__rate__20100607")  }})
CSE_ONLN_BUS_OL_CLNT_RM_RATE AS (
	SELECT RecordType,
		MOD_TIMESTAMP,
		OL_CLIENT_RM_RATING_ID,
		CLIENT_ID,
		CIF_CODE,
		OU_ID,
		CS_USER_ID,
		RATING,
		WIM_PROCESS_ID
	FROM _cba__app_csel4_sit_inprocess_cse__onln__bus__ol__clnt__rm__rate__cse__onln__bus__clnt__rm__rate__20100607
)

SELECT * FROM CSE_ONLN_BUS_OL_CLNT_RM_RATE