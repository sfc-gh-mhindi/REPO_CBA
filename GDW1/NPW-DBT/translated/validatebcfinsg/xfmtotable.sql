{{ config(materialized='view', tags=['ValidateBcFinsg']) }}

WITH XfmToTable AS (
	SELECT
		-- *SRC*: \(20)IF FrmSrc.BCF_DT_CURR_PROC[3, 8] = pRUN_STRM_PROS_D THEN 'Y' ELSE 'N',
		IFF(SUBSTRING({{ ref('BCFINSG') }}.BCF_DT_CURR_PROC, 3, 8) = pRUN_STRM_PROS_D, 'Y', 'N') AS svDateValidation,
		'Process Date in File doesnt match with the ETL Processing date' AS MessageToWrite,
		'Warning' AS MessageSeverity
	FROM {{ ref('BCFINSG') }}
	WHERE svDateValidation = 'N'
)

SELECT * FROM XfmToTable