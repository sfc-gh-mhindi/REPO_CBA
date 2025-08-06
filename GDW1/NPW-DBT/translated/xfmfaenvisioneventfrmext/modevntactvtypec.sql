{{ config(materialized='view', tags=['XfmFaEnvisionEventFrmExt']) }}

WITH ModEvntActvTypeC AS (
	SELECT 
	--Manual
	--EVNT_ACTV_TYPE_C:nullable string[4] = handle_null (EVNT_ACTV_TYPE_C, '9999')
	FA_ENVISION_EVENT_ID, FA_UNDERTAKING_ID, FA_ENV_EVNT_CAT_ID, CREATED_DATE, CREATED_BY_STAFF_NUMBER, COIN_REQUEST_ID, ORIG_ETL_D, EVNT_ACTV_TYPE_C 
	FROM {{ ref('LkEnvtActvType') }}
)

SELECT * FROM ModEvntActvTypeC