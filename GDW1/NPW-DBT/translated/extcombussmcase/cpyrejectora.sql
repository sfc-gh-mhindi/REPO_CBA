{{ config(materialized='view', tags=['ExtComBusSmCase']) }}

WITH CpyRejectOra AS (
	SELECT
		SM_CASE_ID,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.CREATED_TIMESTAMP AS CREATED_TIMESTAMP_R,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.WIM_PROCESS_ID AS WIM_PROCESS_ID_R,
		{{ ref('SrcRejtCCAppProdRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtCCAppProdRejectOra') }}
)

SELECT * FROM CpyRejectOra