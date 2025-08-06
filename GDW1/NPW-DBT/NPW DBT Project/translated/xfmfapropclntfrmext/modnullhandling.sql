{{ config(materialized='view', tags=['XfmFaPropClntFrmExt']) }}

WITH ModNullHandling AS (
	SELECT 
	--Manual
	--PATY_TYPE_C:  nullable string[1]= handle_null (PATY_TYPE_C, '9')
	FA_PROPOSED_CLIENT_ID, COIN_ENTITY_ID, CLIENT_CORRELATION_ID, COIN_ENTITY_NAME, FA_ENTITY_CAT_ID, FA_UNDERTAKING_ID, FA_PROPOSED_CLIENT_CAT_ID, ORIG_ETL_D, change_code, PATY_TYPE_C 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM ModNullHandling