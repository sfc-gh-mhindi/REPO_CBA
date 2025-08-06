{{ config(materialized='view', tags=['XfmComBusAppProdClientRoleFrmExt']) }}

WITH ModNullHandling AS (
	SELECT 
	--Manual
	--PATY_ROLE_C:string[max=4] = handle_null (PATY_ROLE_C, '9999')
	--APPT_QLFY_C:string[max=2] = handle_null(APPT_QLFY_C,'99')
	APP_PROD_CLIENT_ROLE_ID, ROLE_CAT_ID, CIF_CODE, APP_PROD_ID, SUBTYPE_CODE, ORIG_ETL_D, PATY_ROLE_C, APPT_QLFY_C 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM ModNullHandling