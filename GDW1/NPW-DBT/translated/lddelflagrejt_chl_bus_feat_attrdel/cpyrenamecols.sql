{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_FEAT_ATTRDel']) }}

WITH CpyRenameCols AS (
	SELECT
		{{ ref('HL_FEATURE_ATTR') }}.DELETED_KEY_1_VALUE AS HL_FEATURE_ATTR_ID
	FROM {{ ref('HL_FEATURE_ATTR') }}
)

SELECT * FROM CpyRenameCols