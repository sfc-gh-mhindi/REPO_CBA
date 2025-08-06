{{ config(materialized='view', tags=['LdTmp_Rm_Rate_Paty']) }}

WITH XfmTrans AS (
	SELECT
		-- *SRC*: \(20)If IsNull(FrmSrc.CIF_CODE) Then 'N' Else  If ( IF IsNotNull((FrmSrc.CIF_CODE)) THEN (FrmSrc.CIF_CODE) ELSE "") = '' Then 'N' Else 'Y',
		IFF({{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.CIF_CODE IS NULL, 'N', IFF(IFF({{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.CIF_CODE IS NOT NULL, {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.CIF_CODE, '') = '', 'N', 'Y')) AS svNull,
		-- *SRC*: 'CIFPT+' : Right('0000000000' : FrmSrc.CIF_CODE, 10),
		CONCAT('CIFPT+', RIGHT(CONCAT('0000000000', {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.CIF_CODE), 10)) AS PATY_I
	FROM {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}
	WHERE svNull = 'Y'
)

SELECT * FROM XfmTrans