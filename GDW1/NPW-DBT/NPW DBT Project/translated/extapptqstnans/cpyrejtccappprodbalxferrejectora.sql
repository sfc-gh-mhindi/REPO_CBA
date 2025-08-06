{{ config(materialized='view', tags=['ExtApptQstnAns']) }}

WITH CpyRejtCCAppProdBalXferRejectOra AS (
	SELECT
		APP_ID,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.SUBTYPE_CODE AS SUBTYPE_CODE_R,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.QA_QUESTION_ID AS QA_QUESTION_ID_R,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.QA_ANSWER_ID AS QA_ANSWER_ID_R,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.TEXT_ANSWER AS TEXT_ANSWER_R,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.CIF_CODE AS CIF_CODE_R,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtCCAppProdBalXferRejectOra') }}
)

SELECT * FROM CpyRejtCCAppProdBalXferRejectOra