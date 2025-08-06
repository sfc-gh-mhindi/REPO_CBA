{{ config(materialized='view', tags=['DltAPPT_FEAT_SHLFrmTMP_APPT_FEAT']) }}

WITH CpyApptFeat AS (
	SELECT
		NEW_APPT_I,
		NEW_FEAT_I,
		NEW_SHL_F,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_FEAT_I AS NEW_FEAT_I,
		{{ ref('SrcTmpApptFeatTera') }}.OLD_SHL_F AS NEW_SHL_F,
		NEW_SRCE_SYST_C,
		OLD_EFFT_D,
		NEW_CASS_WITHHOLD_RISKBANK_FLAG
	FROM {{ ref('SrcTmpApptFeatTera') }}
)

SELECT * FROM CpyApptFeat