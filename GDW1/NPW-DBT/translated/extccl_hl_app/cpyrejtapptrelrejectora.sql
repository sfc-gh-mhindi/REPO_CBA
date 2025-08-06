{{ config(materialized='view', tags=['ExtCCL_HL_APP']) }}

WITH CpyRejtApptRelRejectOra AS (
	SELECT
		CCL_HL_APP_ID,
		{{ ref('SrcRejtApptRelRejectOra') }}.CCL_APP_ID AS CCL_APP_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.HL_APP_ID AS HL_APP_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.LMI_AMT AS LMI_AMT_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.HL_PACKAGE_CAT_ID AS HL_PACKAGE_CAT_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtApptRelRejectOra') }}
)

SELECT * FROM CpyRejtApptRelRejectOra