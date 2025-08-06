{{ config(materialized='view', tags=['LdDelFlagREJT_CSE_COI_BUS_CLNT_UNDTAKDel']) }}

WITH CpyRenameCols AS (
	SELECT
		{{ ref('FA_CLIENT_UNDERTAKING') }}.DELETED_KEY_1_VALUE AS FA_CLIENT_UNDERTAKING_ID
	FROM {{ ref('FA_CLIENT_UNDERTAKING') }}
)

SELECT * FROM CpyRenameCols