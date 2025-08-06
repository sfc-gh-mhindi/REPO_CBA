{{ config(materialized='view', tags=['LdDelFlagREJT_CPL_BUS_INT_RATE_AMT_MARGDel']) }}

WITH RenameCol AS (
	SELECT
		{{ ref('SrcPlIntRateDS') }}.DELETED_KEY_1_VALUE AS PL_INT_RATE_ID
	FROM {{ ref('SrcPlIntRateDS') }}
)

SELECT * FROM RenameCol