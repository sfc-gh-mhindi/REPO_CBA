{{ config(materialized='view', tags=['LdTMP_APPT_PRMO_TRAKFrmXfm']) }}

WITH xf_TransInsRecds AS (
	SELECT
		-- *SRC*: \(20)If IsNull(Ln_Read_Srce_Recds.CCL_APP_PROD_ID) Then 'N' Else 'Y',
		IFF({{ ref('sq_ValidatedFile') }}.CCL_APP_PROD_ID IS NULL, 'N', 'Y') AS svApptPdctId,
		-- *SRC*: \(20)If IsNull(Ln_Read_Srce_Recds.TRACKING_ID) Then 'N' Else 'Y',
		IFF({{ ref('sq_ValidatedFile') }}.TRACKING_ID IS NULL, 'N', 'Y') AS svTrakId,
		-- *SRC*: 'CSECL' : ( IF IsNotNull((Ln_Read_Srce_Recds.CCL_APP_PROD_ID)) THEN (Ln_Read_Srce_Recds.CCL_APP_PROD_ID) ELSE ""),
		CONCAT('CSECL', IFF({{ ref('sq_ValidatedFile') }}.CCL_APP_PROD_ID IS NOT NULL, {{ ref('sq_ValidatedFile') }}.CCL_APP_PROD_ID, '')) AS APPT_PDCT_I,
		-- *SRC*: ( IF IsNotNull((Ln_Read_Srce_Recds.TRACKING_ID)) THEN (Ln_Read_Srce_Recds.TRACKING_ID) ELSE ""),
		IFF({{ ref('sq_ValidatedFile') }}.TRACKING_ID IS NOT NULL, {{ ref('sq_ValidatedFile') }}.TRACKING_ID, '') AS TRAK_I,
		-- *SRC*: StringToDate(Ln_Read_Srce_Recds.MOD_TIMESTAMP, '%yyyy%mm%dd'),
		STRINGTODATE({{ ref('sq_ValidatedFile') }}.MOD_TIMESTAMP, '%yyyy%mm%dd') AS MOD_DATE,
		'I' AS RECD_IND
	FROM {{ ref('sq_ValidatedFile') }}
	WHERE svApptPdctId = 'Y' AND svTrakId = 'Y'
)

SELECT * FROM xf_TransInsRecds