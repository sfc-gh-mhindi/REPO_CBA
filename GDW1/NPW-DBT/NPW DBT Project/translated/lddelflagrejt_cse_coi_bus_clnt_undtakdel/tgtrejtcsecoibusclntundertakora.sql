{{ config(materialized='view', tags=['LdDelFlagREJT_CSE_COI_BUS_CLNT_UNDTAKDel']) }}

SELECT
	FA_CLIENT_UNDERTAKING_ID 
FROM {{ ref('CpyRenameCols') }}