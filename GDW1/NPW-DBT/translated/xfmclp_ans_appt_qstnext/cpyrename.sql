{{ config(materialized='view', tags=['XfmCLP_ANS_Appt_QstnExt']) }}

WITH CpyRename AS (
	SELECT
		APP_ID,
		{{ ref('SrcCCAppProdBalXferPremapDS') }}.SUBTYPE_CODE AS SBTY_CODE,
		{{ ref('SrcCCAppProdBalXferPremapDS') }}.QA_QUESTION_ID AS QSTN_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE
	FROM {{ ref('SrcCCAppProdBalXferPremapDS') }}
)

SELECT * FROM CpyRename