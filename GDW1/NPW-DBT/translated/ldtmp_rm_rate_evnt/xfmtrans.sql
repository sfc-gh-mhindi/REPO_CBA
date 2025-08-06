{{ config(materialized='view', tags=['LdTmp_Rm_Rate_Evnt']) }}

WITH XfmTrans AS (
	SELECT
		-- *SRC*: \(20)If IsNull(FrmSrc.OL_CLIENT_RM_RATING_ID) Then 'N' Else  If ( IF IsNotNull((FrmSrc.OL_CLIENT_RM_RATING_ID)) THEN (FrmSrc.OL_CLIENT_RM_RATING_ID) ELSE "") = '' Then 'N' Else 'Y',
		IFF({{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.OL_CLIENT_RM_RATING_ID IS NULL, 'N', IFF(IFF({{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.OL_CLIENT_RM_RATING_ID IS NOT NULL, {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.OL_CLIENT_RM_RATING_ID, '') = '', 'N', 'Y')) AS svNull,
		-- *SRC*: 'CSE' : 'A7' : FrmSrc.OL_CLIENT_RM_RATING_ID,
		CONCAT(CONCAT('CSE', 'A7'), {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.OL_CLIENT_RM_RATING_ID) AS EVNT_I,
		WIM_PROCESS_ID
	FROM {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}
	WHERE svNull = 'Y'
)

SELECT * FROM XfmTrans