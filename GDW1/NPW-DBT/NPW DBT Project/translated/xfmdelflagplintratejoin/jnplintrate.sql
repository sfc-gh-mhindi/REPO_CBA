{{ config(materialized='view', tags=['XfmDelFlagPlIntRateJoin']) }}

WITH JnPlIntRate AS (
	SELECT
		{{ ref('SrcPlIntRateDS') }}.DELETED_TABLE_NAME,
		{{ ref('SrcPlIntRateDS') }}.DELETED_KEY_1,
		{{ ref('SrcPlIntRateDS') }}.DELETED_KEY_1_VALUE,
		{{ ref('SrcPlIntRateDS') }}.DELETED_KEY_2,
		{{ ref('SrcPlIntRateDS') }}.DELETED_KEY_2_VALUE,
		{{ ref('SrcPlIntRateDS') }}.DELETED_KEY_3,
		{{ ref('SrcPlIntRateDS') }}.DELETED_KEY_3_VALUE,
		{{ ref('SrcPlIntRateDS') }}.DELETED_KEY_4,
		{{ ref('SrcPlIntRateDS') }}.DELETED_KEY_4_VALUE,
		{{ ref('SrcPlIntRateDS') }}.DELETED_KEY_5,
		{{ ref('SrcPlIntRateDS') }}.DELETED_KEY_5_VALUE,
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
	FROM {{ ref('SrcPlIntRateDS') }}
	OUTER JOIN {{ ref('CpyRename') }} ON {{ ref('SrcPlIntRateDS') }}.DELETED_KEY_1_VALUE = {{ ref('CpyRename') }}.DELETED_KEY_1_VALUE
)

SELECT * FROM JnPlIntRate