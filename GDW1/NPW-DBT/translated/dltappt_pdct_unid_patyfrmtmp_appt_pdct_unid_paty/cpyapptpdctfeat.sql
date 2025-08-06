{{ config(materialized='view', tags=['DltAPPT_PDCT_UNID_PATYFrmTMP_APPT_PDCT_UNID_PATY']) }}

WITH CpyApptPdctFeat AS (
	SELECT
		NEW_APPT_PDCT_I,
		NEW_PATY_ROLE_C,
		NEW_UNID_PATY_CATG_C,
		NEW_SRCE_SYST_PATY_I,
		NEW_PATY_M,
		{{ ref('SrcTmpApptPdctUnidPatyTera') }}.OLD_APPT_PDCT_I AS NEW_APPT_PDCT_I,
		{{ ref('SrcTmpApptPdctUnidPatyTera') }}.OLD_PATY_ROLE_C AS NEW_PATY_ROLE_C,
		{{ ref('SrcTmpApptPdctUnidPatyTera') }}.OLD_UNID_PATY_CATG_C AS NEW_UNID_PATY_CATG_C,
		{{ ref('SrcTmpApptPdctUnidPatyTera') }}.OLD_SRCE_SYST_PATY_I AS NEW_SRCE_SYST_PATY_I,
		{{ ref('SrcTmpApptPdctUnidPatyTera') }}.OLD_PATY_M AS NEW_PATY_M,
		NEW_SRCE_SYST_C,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptPdctUnidPatyTera') }}
)

SELECT * FROM CpyApptPdctFeat