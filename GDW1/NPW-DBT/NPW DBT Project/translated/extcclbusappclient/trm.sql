{{ config(materialized='view', tags=['ExtCclBusAppClient']) }}

WITH Trm AS (
	SELECT
		-- *SRC*: 'CSECL' : OutCclAppPremapDS.APPT_I,
		CONCAT('CSECL', {{ ref('CpyCclAppSeq') }}.APPT_I) AS APPT_I,
		-- *SRC*: 'CIFPT+' : Str("0", 10 - Len(( IF IsNotNull((OutCclAppPremapDS.PATY_I)) THEN (OutCclAppPremapDS.PATY_I) ELSE ""))) : ( IF IsNotNull((OutCclAppPremapDS.PATY_I)) THEN (OutCclAppPremapDS.PATY_I) ELSE ""),
		CONCAT(CONCAT('CIFPT+', STR('0', 10 - LEN(IFF({{ ref('CpyCclAppSeq') }}.PATY_I IS NOT NULL, {{ ref('CpyCclAppSeq') }}.PATY_I, '')))), IFF({{ ref('CpyCclAppSeq') }}.PATY_I IS NOT NULL, {{ ref('CpyCclAppSeq') }}.PATY_I, '')) AS PATY_I,
		-- *SRC*: \(20)If OutCclAppPremapDS.APPLICANT_FLAG = 'Y' then 'APPT' else 'INTDP',
		IFF({{ ref('CpyCclAppSeq') }}.APPLICANT_FLAG = 'Y', 'APPT', 'INTDP') AS REL_C,
		'UNKN' AS REL_REAS_C,
		'U' AS REL_STUS_C,
		'N/A' AS REL_LEVL_C,
		'CSE' AS SRCE_SYST_C,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('CpyCclAppSeq') }}
	WHERE 
)

SELECT * FROM Trm