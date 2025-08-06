{{ config(materialized='view', tags=['ExtCclappServCpty']) }}

WITH Trm AS (
	SELECT
		-- *SRC*: 'CSECL' : OutCclAppPremapDS.CCL_APP_ID,
		CONCAT('CSECL', {{ ref('CpyCclAppSeq') }}.CCL_APP_ID) AS APPT_I,
		-- *SRC*: 'CSECL' : OutCclAppPremapDS.CCL_APP_SERVICETST_ID,
		CONCAT('CSECL', {{ ref('CpyCclAppSeq') }}.CCL_APP_SERVICETST_ID) AS APPT_SERV_CPTY_I,
		{{ ref('CpyCclAppSeq') }}.CCL_APP_SERVICETST_ID AS SRCE_SYST_APPT_SERV_CPTY_I,
		{{ ref('CpyCclAppSeq') }}.NET_SURPLUS_AMT AS NET_SRPL_A,
		{{ ref('CpyCclAppSeq') }}.TOTAL_HOUSEHOLD_EXP_AMT AS TOTL_HSHD_EXPD_A,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('CpyCclAppSeq') }}
	WHERE 
)

SELECT * FROM Trm