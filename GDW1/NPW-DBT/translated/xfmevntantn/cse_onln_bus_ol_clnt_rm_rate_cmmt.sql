{{ config(materialized='view', tags=['XfmEvntAntn']) }}

WITH 
_cba__app_csel4_sit_inprocess_cse__onln__bus__ol__clnt__rm__rate__cmmt__cse__onln__bus__clnt__rm__rate__20100808 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_inprocess_cse__onln__bus__ol__clnt__rm__rate__cmmt__cse__onln__bus__clnt__rm__rate__20100808")  }})
CSE_ONLN_BUS_OL_CLNT_RM_RATE_CMMT AS (
	SELECT RecordType,
		MOD_TIMESTAMP,
		OL_CLIENT_RM_RATING_ID,
		COMMENTS
	FROM _cba__app_csel4_sit_inprocess_cse__onln__bus__ol__clnt__rm__rate__cmmt__cse__onln__bus__clnt__rm__rate__20100808
)

SELECT * FROM CSE_ONLN_BUS_OL_CLNT_RM_RATE_CMMT