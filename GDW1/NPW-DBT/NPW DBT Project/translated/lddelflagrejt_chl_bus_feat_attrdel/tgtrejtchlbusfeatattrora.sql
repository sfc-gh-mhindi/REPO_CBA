{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_FEAT_ATTRDel']) }}

SELECT
	HL_FEATURE_ATTR_ID 
FROM {{ ref('CpyRenameCols') }}