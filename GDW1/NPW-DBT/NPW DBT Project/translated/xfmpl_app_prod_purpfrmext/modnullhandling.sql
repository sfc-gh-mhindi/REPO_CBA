{{ config(materialized='view', tags=['XfmPL_APP_PROD_PURPFrmExt']) }}

WITH ModNullHandling AS (
	SELECT 
	--Manual
	--PURP_TYPE_C: string[max=4]= handle_null (PURP_TYPE_C, '9999')
	PL_APP_PROD_PURP_ID, PL_APP_PROD_PURP_CAT_ID, AMT, PL_APP_PROD_ID, ORIG_ETL_D, APPT_APPT_PURP_C AS PURP_TYPE_C 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM ModNullHandling