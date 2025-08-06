{{ config(materialized='view', tags=['XfmDelFlagHlIntRateJoin']) }}

WITH CpyRename AS (
	SELECT
		{{ ref('SrcHlProdIntMarginDS') }}.DELETED_TABLE_NAME AS DELETED_TABLE_NAME_R,
		{{ ref('SrcHlProdIntMarginDS') }}.DELETED_KEY_1 AS DELETED_KEY_1_R,
		DELETED_KEY_1_VALUE,
		{{ ref('SrcHlProdIntMarginDS') }}.DELETED_KEY_2 AS DELETED_KEY_2_R,
		{{ ref('SrcHlProdIntMarginDS') }}.DELETED_KEY_2_VALUE AS DELETED_KEY_2_VALUE_R,
		{{ ref('SrcHlProdIntMarginDS') }}.DELETED_KEY_3 AS DELETED_KEY_3_R,
		{{ ref('SrcHlProdIntMarginDS') }}.DELETED_KEY_3_VALUE AS DELETED_KEY_3_VALUE_R,
		{{ ref('SrcHlProdIntMarginDS') }}.DELETED_KEY_4 AS DELETED_KEY_4_R,
		{{ ref('SrcHlProdIntMarginDS') }}.DELETED_KEY_4_VALUE AS DELETED_KEY_4_VALUE_R,
		{{ ref('SrcHlProdIntMarginDS') }}.DELETED_KEY_5 AS DELETED_KEY_5_R,
		{{ ref('SrcHlProdIntMarginDS') }}.DELETED_KEY_5_VALUE AS DELETED_KEY_5_VALUE_R
	FROM {{ ref('SrcHlProdIntMarginDS') }}
)

SELECT * FROM CpyRename