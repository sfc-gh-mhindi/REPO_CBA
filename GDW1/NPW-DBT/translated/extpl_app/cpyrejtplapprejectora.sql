{{ config(materialized='view', tags=['ExtPL_APP']) }}

WITH CpyRejtPlAppRejectOra AS (
	SELECT
		PL_APP_ID,
		{{ ref('SrcRejtPlAppRejectOra') }}.NOMINATED_BRANCH_ID AS NOMINATED_BRANCH_ID_R,
		{{ ref('SrcRejtPlAppRejectOra') }}.PL_PACKAGE_CAT_ID AS PL_PACKAGE_CAT_ID_R,
		{{ ref('SrcRejtPlAppRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtPlAppRejectOra') }}
)

SELECT * FROM CpyRejtPlAppRejectOra