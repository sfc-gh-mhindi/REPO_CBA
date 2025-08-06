{{ config(materialized='view', tags=['ExtAppCon_TuApp']) }}

WITH CpyRejtApptRelRejectOra AS (
	SELECT
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_APP_CONDITION_ID AS TU_APP_CONDITION_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TU_APP_CONDITION_CAT_ID AS TU_APP_CONDITION_CAT_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.CONDITION_MET_DATE AS CONDITION_MET_DATE_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.SUBTYPE_CODE AS SUBTYPE_CODE_R,
		HL_APP_PROD_ID,
		{{ ref('SrcRejtApptRelRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtApptRelRejectOra') }}
)

SELECT * FROM CpyRejtApptRelRejectOra