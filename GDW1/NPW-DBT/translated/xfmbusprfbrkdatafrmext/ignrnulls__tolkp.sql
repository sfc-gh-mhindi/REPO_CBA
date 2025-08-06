{{ config(materialized='view', tags=['XfmBusPrfBrkDataFrmExt']) }}

WITH IgnrNulls__ToLkp AS (
	SELECT
		-- *SRC*: \(20)IF IsNull(ToIgnrNulls.ALIAS_ID) THEN 'N' ELSE  IF Trim(ToIgnrNulls.ALIAS_ID) = '' THEN 'N' ELSE 'Y',
		IFF({{ ref('SrcCseProdBusPrfBrkData') }}.ALIAS_ID IS NULL, 'N', IFF(TRIM({{ ref('SrcCseProdBusPrfBrkData') }}.ALIAS_ID) = '', 'N', 'Y')) AS svConstraint,
		-- *SRC*: "CSE" : "C5" : ( IF IsNotNull((ToIgnrNulls.ALIAS_ID)) THEN (ToIgnrNulls.ALIAS_ID) ELSE ('555555')),
		CONCAT(CONCAT('CSE', 'C5'), IFF({{ ref('SrcCseProdBusPrfBrkData') }}.ALIAS_ID IS NOT NULL, {{ ref('SrcCseProdBusPrfBrkData') }}.ALIAS_ID, '555555')) AS svUnidPatyI,
		svUnidPatyI AS UNID_PATY_I,
		ALIAS_ID,
		GRADE,
		SUBGRADE,
		PRINT_ANYWHERE_FLAG
	FROM {{ ref('SrcCseProdBusPrfBrkData') }}
	WHERE svConstraint = 'Y'
)

SELECT * FROM IgnrNulls__ToLkp