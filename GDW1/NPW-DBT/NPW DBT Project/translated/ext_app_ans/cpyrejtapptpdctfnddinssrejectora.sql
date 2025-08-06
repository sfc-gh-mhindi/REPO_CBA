{{ config(materialized='view', tags=['Ext_APP_ANS']) }}

WITH CpyRejtApptPdctFnddInssRejectOra AS (
	SELECT
		{{ ref('SrcRejtApptRelRejectOra') }}.APP_ID AS HL_APP_ID,
		{{ ref('SrcRejtApptRelRejectOra') }}.QA_QUESTION_ID AS QA_QUESTION_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.QA_ANSWER_ID AS QA_ANSWER_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.TEXT_ANSWER AS TEXT_ANSWER_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.CIF_CODE AS CIF_CODE_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.CBA_STAFF_NUMBER AS CBA_STAFF_NUMBER_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.LODGEMENT_BRANCH_ID AS LODGEMENT_BRANCH_ID_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.SBTY_CODE AS SUBTYPE_CODE_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.MOD_TIMESTAMP AS MOD_TIMESTAMP_R,
		{{ ref('SrcRejtApptRelRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtApptRelRejectOra') }}
)

SELECT * FROM CpyRejtApptPdctFnddInssRejectOra