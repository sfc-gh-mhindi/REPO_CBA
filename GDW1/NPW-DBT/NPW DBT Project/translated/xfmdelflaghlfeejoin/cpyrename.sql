{{ config(materialized='view', tags=['XfmDelFlagHlFeeJoin']) }}

WITH CpyRename AS (
	SELECT
		{{ ref('SrcHlFeeDiscountDS') }}.DELETED_TABLE_NAME AS DELETED_TABLE_NAME_R,
		{{ ref('SrcHlFeeDiscountDS') }}.DELETED_KEY_1 AS DELETED_KEY_1_R,
		DELETED_KEY_1_VALUE,
		{{ ref('SrcHlFeeDiscountDS') }}.DELETED_KEY_2 AS DELETED_KEY_2_R,
		{{ ref('SrcHlFeeDiscountDS') }}.DELETED_KEY_2_VALUE AS DELETED_KEY_2_VALUE_R,
		{{ ref('SrcHlFeeDiscountDS') }}.DELETED_KEY_3 AS DELETED_KEY_3_R,
		{{ ref('SrcHlFeeDiscountDS') }}.DELETED_KEY_3_VALUE AS DELETED_KEY_3_VALUE_R,
		{{ ref('SrcHlFeeDiscountDS') }}.DELETED_KEY_4 AS DELETED_KEY_4_R,
		{{ ref('SrcHlFeeDiscountDS') }}.DELETED_KEY_4_VALUE AS DELETED_KEY_4_VALUE_R,
		{{ ref('SrcHlFeeDiscountDS') }}.DELETED_KEY_5 AS DELETED_KEY_5_R,
		{{ ref('SrcHlFeeDiscountDS') }}.DELETED_KEY_5_VALUE AS DELETED_KEY_5_VALUE_R
	FROM {{ ref('SrcHlFeeDiscountDS') }}
)

SELECT * FROM CpyRename