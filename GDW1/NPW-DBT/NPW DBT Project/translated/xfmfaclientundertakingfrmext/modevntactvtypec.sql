{{ config(materialized='view', tags=['XfmFaClientUndertakingFrmExt']) }}

WITH ModEvntActvTypeC AS (
	SELECT 
	--Manual
	--PATY_TYPE_C:nullable string[1] = handle_null (PATY_TYPE_C, '9')
	--REL_C:nullable string[5] = handle_null (REL_C, '99999')
	--REL_C_CHILD:nullable string[5] = handle_null (REL_C_CHILD, '99999')
	FA_CLIENT_UNDERTAKING_ID, FA_UNDERTAKING_ID, COIN_ENTITY_ID, CLIENT_CORRELATION_ID, FA_ENTITY_CAT_ID, FA_CHILD_STATUS_CAT_ID, CLIENT_RELATIONSHIP_TYPE_ID, CLIENT_POSITION, IS_PRIMARY_FLAG, CIF_CODE, ORIG_ETL_D, PATY_TYPE_C, REL_C, REL_C_CHILD 
	FROM {{ ref('LkEnvtActvType') }}
)

SELECT * FROM ModEvntActvTypeC