{{ config(materialized='view', tags=['XfmDelFlagHlFeeJoin']) }}

WITH JnHlFee AS (
	SELECT
		{{ ref('SrcHlFeeDS') }}.DELETED_TABLE_NAME,
		{{ ref('SrcHlFeeDS') }}.DELETED_KEY_1,
		{{ ref('SrcHlFeeDS') }}.DELETED_KEY_1_VALUE,
		{{ ref('SrcHlFeeDS') }}.DELETED_KEY_2,
		{{ ref('SrcHlFeeDS') }}.DELETED_KEY_2_VALUE,
		{{ ref('SrcHlFeeDS') }}.DELETED_KEY_3,
		{{ ref('SrcHlFeeDS') }}.DELETED_KEY_3_VALUE,
		{{ ref('SrcHlFeeDS') }}.DELETED_KEY_4,
		{{ ref('SrcHlFeeDS') }}.DELETED_KEY_4_VALUE,
		{{ ref('SrcHlFeeDS') }}.DELETED_KEY_5,
		{{ ref('SrcHlFeeDS') }}.DELETED_KEY_5_VALUE,
		{{ ref('CpyRename') }}.DELETED_TABLE_NAME_R,
		{{ ref('CpyRename') }}.DELETED_KEY_1_R,
		{{ ref('CpyRename') }}.DELETED_KEY_1_VALUE AS DELETED_KEY_1_VALUE_R,
		{{ ref('CpyRename') }}.DELETED_KEY_2_R,
		{{ ref('CpyRename') }}.DELETED_KEY_2_VALUE_R,
		{{ ref('CpyRename') }}.DELETED_KEY_3_R,
		{{ ref('CpyRename') }}.DELETED_KEY_3_VALUE_R,
		{{ ref('CpyRename') }}.DELETED_KEY_4_R,
		{{ ref('CpyRename') }}.DELETED_KEY_4_VALUE_R,
		{{ ref('CpyRename') }}.DELETED_KEY_5_R,
		{{ ref('CpyRename') }}.DELETED_KEY_5_VALUE_R
	FROM {{ ref('SrcHlFeeDS') }}
	OUTER JOIN {{ ref('CpyRename') }} ON {{ ref('SrcHlFeeDS') }}.DELETED_KEY_1_VALUE = {{ ref('CpyRename') }}.DELETED_KEY_1_VALUE
)

SELECT * FROM JnHlFee