{{ config(materialized='view', tags=['XfmDelFlagPlFeeJoin']) }}

WITH JnHlFee AS (
	SELECT
		{{ ref('SrcPlFeeDS') }}.DELETED_TABLE_NAME,
		{{ ref('SrcPlFeeDS') }}.DELETED_KEY_1,
		{{ ref('SrcPlFeeDS') }}.DELETED_KEY_1_VALUE,
		{{ ref('SrcPlFeeDS') }}.DELETED_KEY_2,
		{{ ref('SrcPlFeeDS') }}.DELETED_KEY_2_VALUE,
		{{ ref('SrcPlFeeDS') }}.DELETED_KEY_3,
		{{ ref('SrcPlFeeDS') }}.DELETED_KEY_3_VALUE,
		{{ ref('SrcPlFeeDS') }}.DELETED_KEY_4,
		{{ ref('SrcPlFeeDS') }}.DELETED_KEY_4_VALUE,
		{{ ref('SrcPlFeeDS') }}.DELETED_KEY_5,
		{{ ref('SrcPlFeeDS') }}.DELETED_KEY_5_VALUE,
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
	FROM {{ ref('SrcPlFeeDS') }}
	OUTER JOIN {{ ref('CpyRename') }} ON {{ ref('SrcPlFeeDS') }}.DELETED_KEY_1_VALUE = {{ ref('CpyRename') }}.DELETED_KEY_1_VALUE
)

SELECT * FROM JnHlFee