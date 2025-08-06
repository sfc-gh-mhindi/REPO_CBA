{{ config(materialized='view', tags=['DltAPPT_FEAT_SHLFrmTMP_APPT_FEAT']) }}

WITH Funnel_72 AS (
	SELECT
		APPT_I as APPT_I,
		EFFT_D as EFFT_D,
		FEAT_I as FEAT_I,
		SRCE_SYST_APPT_FEAT_I as SRCE_SYST_APPT_FEAT_I,
		EXPY_D as EXPY_D,
		PROS_KEY_EXPY_I as PROS_KEY_EXPY_I
	FROM {{ ref('XfmCheckDeltaAction__OutTgtApptFeatUpdateDS1') }}
	UNION ALL
	SELECT
		APPT_I,
		EFFT_D,
		FEAT_I,
		SRCE_SYST_APPT_FEAT_I,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM {{ ref('XfmCheckDeltaAction__OutTgtApptFeatUpdateDS2') }}
)

SELECT * FROM Funnel_72