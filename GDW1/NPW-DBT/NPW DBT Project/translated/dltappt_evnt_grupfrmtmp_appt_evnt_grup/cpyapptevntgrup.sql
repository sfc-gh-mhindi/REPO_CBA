{{ config(materialized='view', tags=['DltAPPT_EVNT_GRUPFrmTMP_APPT_EVNT_GRUP']) }}

WITH CpyApptEvntGrup AS (
	SELECT
		NEW_APPT_I,
		NEW_EVNT_GRUP_I,
		{{ ref('SrcTmpApptEvntGrupTera') }}.OLD_APPT_I AS NEW_APPT_I,
		{{ ref('SrcTmpApptEvntGrupTera') }}.OLD_EVNT_GRUP_I AS NEW_EVNT_GRUP_I,
		OLD_EFFT_D
	FROM {{ ref('SrcTmpApptEvntGrupTera') }}
)

SELECT * FROM CpyApptEvntGrup