{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH ModNullHandling AS (
	SELECT 
	--Manual
	--PATY_ROLE_C:string[max=4] = handle_null (PATY_ROLE_C, '9999')
	--APPT_QLFY_C:string[max=2] = handle_null (APPT_QLFY_C, '99')
	PATY_ROLE_C, DELETED_TABLE_NAME, DELETED_KEY_1, DELETED_KEY_1_VALUE, DELETED_KEY_2, DELETED_KEY_2_VALUE, DELETED_KEY_3, DELETED_KEY_3_VALUE, DELETED_KEY_4, DELETED_KEY_4_VALUE, DELETED_KEY_5, DELETED_KEY_5_VALUE, APPT_QLFY_C 
	FROM {{ ref('LkpReferences') }}
)

SELECT * FROM ModNullHandling