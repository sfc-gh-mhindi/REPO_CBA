{{ config(materialized='view', tags=['LdDelFlagREJT_CHL_BUS_FEE_DISC_FEEDel']) }}

WITH RenameCol AS (
	SELECT
		{{ ref('SrcHlFeeDS') }}.DELETED_KEY_1_VALUE AS HL_FEE_ID
	FROM {{ ref('SrcHlFeeDS') }}
)

SELECT * FROM RenameCol