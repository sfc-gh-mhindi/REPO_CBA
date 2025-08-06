{{ config(materialized='view', tags=['XfmDelFlagHlIntRateJoin']) }}

WITH JnHlIntRate AS (
	SELECT
		{{ ref('SrcHlIntRateDS') }}.DELETED_TABLE_NAME,
		{{ ref('SrcHlIntRateDS') }}.DELETED_KEY_1,
		{{ ref('SrcHlIntRateDS') }}.DELETED_KEY_1_VALUE,
		{{ ref('SrcHlIntRateDS') }}.DELETED_KEY_2,
		{{ ref('SrcHlIntRateDS') }}.DELETED_KEY_2_VALUE,
		{{ ref('SrcHlIntRateDS') }}.DELETED_KEY_3,
		{{ ref('SrcHlIntRateDS') }}.DELETED_KEY_3_VALUE,
		{{ ref('SrcHlIntRateDS') }}.DELETED_KEY_4,
		{{ ref('SrcHlIntRateDS') }}.DELETED_KEY_4_VALUE,
		{{ ref('SrcHlIntRateDS') }}.DELETED_KEY_5,
		{{ ref('SrcHlIntRateDS') }}.DELETED_KEY_5_VALUE,
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
	FROM {{ ref('SrcHlIntRateDS') }}
	OUTER JOIN {{ ref('CpyRename') }} ON {{ ref('SrcHlIntRateDS') }}.DELETED_KEY_1_VALUE = {{ ref('CpyRename') }}.DELETED_KEY_1_VALUE
)

SELECT * FROM JnHlIntRate