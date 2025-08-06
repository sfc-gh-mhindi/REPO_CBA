{{ config(materialized='view', tags=['XfmTu_App_Cnd_XFERFrmExt']) }}

WITH Trm_O__valid_Appt_Pdct_cond AS (
	SELECT
		-- *SRC*: \(20)If IsNull(valid.COND_C) Then '9999' Else valid.COND_C,
		IFF({{ ref('LkpReferences') }}.COND_C IS NULL, '9999', {{ ref('LkpReferences') }}.COND_C) AS COND,
		-- *SRC*: "CSE" : valid.APPT_QLFY_C : valid.HL_APP_PROD_ID,
		CONCAT(CONCAT('CSE', {{ ref('LkpReferences') }}.APPT_QLFY_C), {{ ref('LkpReferences') }}.HL_APP_PROD_ID) AS APPT_PDCT_I,
		COND AS COND_C,
		-- *SRC*: \(20)if isnull(valid.APPT_PDCT_COND_MEET_D) then setnull() else StringToDate(valid.APPT_PDCT_COND_MEET_D, "%yyyy%mm%dd"),
		IFF({{ ref('LkpReferences') }}.APPT_PDCT_COND_MEET_D IS NULL, SETNULL(), STRINGTODATE({{ ref('LkpReferences') }}.APPT_PDCT_COND_MEET_D, '%yyyy%mm%dd')) AS APPT_PDCT_COND_MEET_D
	FROM {{ ref('LkpReferences') }}
	WHERE 
)

SELECT * FROM Trm_O__valid_Appt_Pdct_cond