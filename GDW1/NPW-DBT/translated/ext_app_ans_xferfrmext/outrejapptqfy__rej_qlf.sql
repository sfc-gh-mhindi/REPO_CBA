{{ config(materialized='view', tags=['Ext_APP_ANS_XFERFrmExt']) }}

WITH OutRejApptQfy__rej_qlf AS (
	SELECT
		{{ ref('LkpReferences') }}.HL_APP_ID AS APP_ID,
		QA_QUESTION_ID,
		QA_ANSWER_ID,
		TEXT_ANSWER,
		CIF_CODE,
		CBA_STAFF_NUMBER,
		LODGEMENT_BRANCH_ID,
		SBTY_CODE,
		MOD_TIMESTAMP,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		'RPR786' AS EROR_C
	FROM {{ ref('LkpReferences') }}
	WHERE 
)

SELECT * FROM OutRejApptQfy__rej_qlf