{{ config(materialized='view', tags=['MergeTMP_DELETED']) }}

WITH CpyRename AS (
	SELECT
		DELETED_TABLE_NAME,
		DELETED_KEY_1,
		DELETED_KEY_1_VALUE,
		DELETED_KEY_2,
		DELETED_KEY_2_VALUE,
		DELETED_KEY_3,
		{{ ref('SrcPatyApptPdct') }}.DELETED_KEY_3_VALUE AS ROLE_CAT_ID,
		DELETED_KEY_4,
		{{ ref('SrcPatyApptPdct') }}.DELETED_KEY_4_VALUE AS SBTY_CODE,
		DELETED_KEY_5,
		DELETED_KEY_5_VALUE
	FROM {{ ref('SrcPatyApptPdct') }}
)

SELECT * FROM CpyRename