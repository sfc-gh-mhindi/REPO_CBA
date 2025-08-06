{{ config(materialized='view', tags=['LdDelFlagREJT_PL_FEE_PL_MARGINDel']) }}

WITH RenameCol AS (
	SELECT
		{{ ref('SrcPlFeeDS') }}.DELETED_KEY_1_VALUE AS PL_FEE_ID
	FROM {{ ref('SrcPlFeeDS') }}
)

SELECT * FROM RenameCol