{{ config(materialized='view', tags=['ExtComBusSmCase']) }}

WITH XfmCheckIdNull__OutIdNullDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((OutCpyComBusSmCase.SM_CASE_ID)) THEN (OutCpyComBusSmCase.SM_CASE_ID) ELSE ""))) = 0) Then 'REJ5006' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpyComBusSmCase') }}.SM_CASE_ID IS NOT NULL, {{ ref('CpyComBusSmCase') }}.SM_CASE_ID, ''))) = 0, 'REJ5006', '') AS ErrorCode,
		SM_CASE_ID,
		CREATED_TIMESTAMP,
		WIM_PROCESS_ID,
		ETL_PROCESS_DT AS ETL_D,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		ErrorCode AS EROR_C
	FROM {{ ref('CpyComBusSmCase') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) = 'REJ'
)

SELECT * FROM XfmCheckIdNull__OutIdNullDS