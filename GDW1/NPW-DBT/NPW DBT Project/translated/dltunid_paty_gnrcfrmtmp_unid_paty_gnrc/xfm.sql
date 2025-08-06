{{ config(materialized='view', tags=['DltUNID_PATY_GNRCFrmTMP_UNID_PATY_GNRC']) }}

WITH Xfm AS (
	SELECT
		-- *SRC*: \(20)If IsNull(InTmpUnidPatyGnrcTera.OLD_UNID_PATY_I) Then 'Y' Else 'N',
		IFF({{ ref('SrcTmpUnidPatyGnrcTera') }}.OLD_UNID_PATY_I IS NULL, 'Y', 'N') AS svInserts,
		{{ ref('SrcTmpUnidPatyGnrcTera') }}.NEW_UNID_PATY_I AS UNID_PATY_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'U' AS PATY_TYPE_C,
		'TPBR' AS PATY_ROLE_C,
		REFR_PK AS PROS_KEY_EFFT_I,
		'CSE' AS SRCE_SYST_C,
		'C5' AS PATY_QLFY_C,
		{{ ref('SrcTmpUnidPatyGnrcTera') }}.NEW_SRCE_SYST_PATY_I AS SRCE_SYST_PATY_I
	FROM {{ ref('SrcTmpUnidPatyGnrcTera') }}
	WHERE svInserts = 'Y'
)

SELECT * FROM Xfm