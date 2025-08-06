{{ config(materialized='view', tags=['LdDelFlagREJT_RT_PERC_PROD_INT_MARGDel']) }}

WITH RenameCol AS (
	SELECT
		{{ ref('SrcHlIntRateDS') }}.DELETED_KEY_1_VALUE AS HL_INT_RATE_ID
	FROM {{ ref('SrcHlIntRateDS') }}
)

SELECT * FROM RenameCol