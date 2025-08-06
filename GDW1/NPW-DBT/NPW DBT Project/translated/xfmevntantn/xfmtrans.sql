{{ config(materialized='view', tags=['XfmEvntAntn']) }}

WITH XfmTrans AS (
	SELECT
		-- *SRC*: 'CSE' : 'A5' : FrmSrc.OL_CLIENT_RM_RATING_ID,
		CONCAT(CONCAT('CSE', 'A5'), {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE_CMMT') }}.OL_CLIENT_RM_RATING_ID) AS svAntnI,
		-- *SRC*: 'CSE' : 'A7' : FrmSrc.OL_CLIENT_RM_RATING_ID,
		CONCAT(CONCAT('CSE', 'A7'), {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE_CMMT') }}.OL_CLIENT_RM_RATING_ID) AS svEvntI,
		svEvntI AS EVNT_I,
		svAntnI AS ANTN_I,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'0' AS ROW_SECU_ACCS_C
	FROM {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE_CMMT') }}
	WHERE 
)

SELECT * FROM XfmTrans