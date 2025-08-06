{{ config(materialized='view', tags=['XfmAppt_Pdct_Paty']) }}

WITH 
_cba__app_csel4_dev_inprocess_cse__com__bus__app__prod__client__role__cse__com__bus__app__prod__clnt__rl__20101010 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_cse__com__bus__app__prod__client__role__cse__com__bus__app__prod__clnt__rl__20101010")  }})
Src_Com_Bus_App_Prod_Client_Role AS (
	SELECT Record_Type,
		MOD_TIMESTAMP,
		APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID,
		CIF_CODE,
		APP_PROD_ID,
		SUBTYPE_CODE
	FROM _cba__app_csel4_dev_inprocess_cse__com__bus__app__prod__client__role__cse__com__bus__app__prod__clnt__rl__20101010
)

SELECT * FROM Src_Com_Bus_App_Prod_Client_Role