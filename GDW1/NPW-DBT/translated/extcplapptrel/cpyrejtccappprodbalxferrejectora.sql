{{ config(materialized='view', tags=['ExtCplApptRel']) }}

WITH CpyRejtCCAppProdBalXferRejectOra AS (
	SELECT
		APPT_I,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.RELD_APPT_I AS RELD_APPT_I_R,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.REL_TYPE_C AS REL_TYPE_C_R,
		{{ ref('SrcRejtCCAppProdBalXferRejectOra') }}.LOAN_SUBTYPE_CODE AS LOAN_SUBTYPE_CODE_R,
		ORIG_ETL_D
	FROM {{ ref('SrcRejtCCAppProdBalXferRejectOra') }}
)

SELECT * FROM CpyRejtCCAppProdBalXferRejectOra