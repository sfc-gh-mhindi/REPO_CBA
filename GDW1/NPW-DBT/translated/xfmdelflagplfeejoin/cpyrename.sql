{{ config(materialized='view', tags=['XfmDelFlagPlFeeJoin']) }}

WITH CpyRename AS (
	SELECT
		{{ ref('FL_RemoveNulls') }}.DELETED_TABLE_NAME AS DELETED_TABLE_NAME_R,
		{{ ref('FL_RemoveNulls') }}.DELETED_KEY_3 AS DELETED_KEY_1_R,
		{{ ref('FL_RemoveNulls') }}.DELETED_KEY_3_VALUE AS DELETED_KEY_1_VALUE,
		{{ ref('FL_RemoveNulls') }}.DELETED_KEY_2 AS DELETED_KEY_2_R,
		{{ ref('FL_RemoveNulls') }}.DELETED_KEY_2_VALUE AS DELETED_KEY_2_VALUE_R,
		{{ ref('FL_RemoveNulls') }}.DELETED_KEY_1 AS DELETED_KEY_3_R,
		{{ ref('FL_RemoveNulls') }}.DELETED_KEY_1_VALUE AS DELETED_KEY_3_VALUE_R,
		{{ ref('FL_RemoveNulls') }}.DELETED_KEY_4 AS DELETED_KEY_4_R,
		{{ ref('FL_RemoveNulls') }}.DELETED_KEY_4_VALUE AS DELETED_KEY_4_VALUE_R,
		{{ ref('FL_RemoveNulls') }}.DELETED_KEY_5 AS DELETED_KEY_5_R,
		{{ ref('FL_RemoveNulls') }}.DELETED_KEY_5_VALUE AS DELETED_KEY_5_VALUE_R
	FROM {{ ref('FL_RemoveNulls') }}
)

SELECT * FROM CpyRename