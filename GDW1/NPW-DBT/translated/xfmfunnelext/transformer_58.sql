{{ config(materialized='view', tags=['XfmFunnelExt']) }}

WITH Transformer_58 AS (
	SELECT
		-- *SRC*: \(20)If (IsNotNull(DSLink57.CIF_CODE)) then  If Len(Trim(DSLink57.CIF_CODE)) < 10 and Len(Trim(DSLink57.CIF_CODE)) > 0 THEN Str('0', 10 - Len(DSLink57.CIF_CODE)) : DSLink57.CIF_CODE ELSE DSLink57.CIF_CODE else '',
		IFF({{ ref('Funnel_52') }}.CIF_CODE IS NOT NULL, IFF(LEN(TRIM({{ ref('Funnel_52') }}.CIF_CODE)) < 10 AND LEN(TRIM({{ ref('Funnel_52') }}.CIF_CODE)) > 0, CONCAT(STR('0', 10 - LEN({{ ref('Funnel_52') }}.CIF_CODE)), {{ ref('Funnel_52') }}.CIF_CODE), {{ ref('Funnel_52') }}.CIF_CODE), '') AS FrmCifCode,
		-- *SRC*: \(20)If (IsNotNull(DSLink57.QA_QUESTION_ID)) then  If Len(Trim(DSLink57.QA_QUESTION_ID)) < 4 and Len(Trim(DSLink57.QA_QUESTION_ID)) > 0 THEN Str('0', 4 - Len(DSLink57.QA_QUESTION_ID)) : DSLink57.QA_QUESTION_ID ELSE DSLink57.QA_QUESTION_ID else '',
		IFF({{ ref('Funnel_52') }}.QA_QUESTION_ID IS NOT NULL, IFF(LEN(TRIM({{ ref('Funnel_52') }}.QA_QUESTION_ID)) < 4 AND LEN(TRIM({{ ref('Funnel_52') }}.QA_QUESTION_ID)) > 0, CONCAT(STR('0', 4 - LEN({{ ref('Funnel_52') }}.QA_QUESTION_ID)), {{ ref('Funnel_52') }}.QA_QUESTION_ID), {{ ref('Funnel_52') }}.QA_QUESTION_ID), '') AS FrmQstnId,
		-- *SRC*: \(20)If (IsNotNull(DSLink57.QA_ANSWER_ID)) then  If Len(Trim(DSLink57.QA_ANSWER_ID)) < 4 and Len(Trim(DSLink57.QA_ANSWER_ID)) > 0 THEN Str('0', 4 - Len(DSLink57.QA_ANSWER_ID)) : DSLink57.QA_ANSWER_ID ELSE DSLink57.QA_ANSWER_ID else '',
		IFF({{ ref('Funnel_52') }}.QA_ANSWER_ID IS NOT NULL, IFF(LEN(TRIM({{ ref('Funnel_52') }}.QA_ANSWER_ID)) < 4 AND LEN(TRIM({{ ref('Funnel_52') }}.QA_ANSWER_ID)) > 0, CONCAT(STR('0', 4 - LEN({{ ref('Funnel_52') }}.QA_ANSWER_ID)), {{ ref('Funnel_52') }}.QA_ANSWER_ID), {{ ref('Funnel_52') }}.QA_ANSWER_ID), '') AS FrmAnsid,
		APP_ID,
		SUBTYPE_CODE,
		-- *SRC*: \(20)if FrmQstnId = '' then DSLink57.QA_QUESTION_ID else FrmQstnId,
		IFF(FrmQstnId = '', {{ ref('Funnel_52') }}.QA_QUESTION_ID, FrmQstnId) AS QA_QUESTION_ID,
		-- *SRC*: \(20)if FrmAnsid = '' then DSLink57.QA_ANSWER_ID else FrmAnsid,
		IFF(FrmAnsid = '', {{ ref('Funnel_52') }}.QA_ANSWER_ID, FrmAnsid) AS QA_ANSWER_ID,
		TEXT_ANSWER,
		-- *SRC*: \(20)if FrmCifCode = '' then DSLink57.CIF_CODE else "CIFPT+" : FrmCifCode,
		IFF(FrmCifCode = '', {{ ref('Funnel_52') }}.CIF_CODE, CONCAT('CIFPT+', FrmCifCode)) AS CIF_CODE
	FROM {{ ref('Funnel_52') }}
	WHERE 
)

SELECT * FROM Transformer_58